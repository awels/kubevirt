package container_disk

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"kubevirt.io/client-go/log"
	containerdisk "kubevirt.io/kubevirt/pkg/container-disk"
	diskutils "kubevirt.io/kubevirt/pkg/ephemeral-disk-utils"
	"kubevirt.io/kubevirt/pkg/virt-handler/isolation"

	"k8s.io/apimachinery/pkg/types"

	v1 "kubevirt.io/client-go/api/v1"
)

//go:generate mockgen -source $GOFILE -package=$GOPACKAGE -destination=generated_mock_$GOFILE

type mounter struct {
	podIsolationDetector   isolation.PodIsolationDetector
	mountStateDir          string
	mountRecords           map[types.UID]*vmiMountTargetRecord
	mountRecordsLock       sync.Mutex
	suppressWarningTimeout time.Duration
	pathGetter             containerdisk.SocketPathGetter
}

type Mounter interface {
	ContainerDisksReady(vmi *v1.VirtualMachineInstance, notInitializedSince time.Time) (bool, error)
	Mount(vmi *v1.VirtualMachineInstance, verify bool) error
	MountHotplugVolumes(vmi *v1.VirtualMachineInstance) error
	Unmount(vmi *v1.VirtualMachineInstance) error
	UnmountHotplugVolumes(vmi *v1.VirtualMachineInstance) error
}

type vmiMountTargetEntry struct {
	TargetFile string `json:"targetFile"`
	SocketFile string `json:"socketFile"`
}

type vmiMountTargetRecord struct {
	MountTargetEntries []vmiMountTargetEntry `json:"mountTargetEntries"`
}

func NewMounter(isoDetector isolation.PodIsolationDetector, mountStateDir string) Mounter {
	return &mounter{
		mountRecords:           make(map[types.UID]*vmiMountTargetRecord),
		podIsolationDetector:   isoDetector,
		mountStateDir:          mountStateDir,
		suppressWarningTimeout: 1 * time.Minute,
		pathGetter:             containerdisk.NewSocketPathGetter(""),
	}
}

func (m *mounter) deleteMountTargetRecord(vmi *v1.VirtualMachineInstance) error {
	if string(vmi.UID) == "" {
		return fmt.Errorf("unable to find container disk mounted directories for vmi without uid")
	}

	recordFile := filepath.Join(m.mountStateDir, string(vmi.UID))

	exists, err := diskutils.FileExists(recordFile)
	if err != nil {
		return err
	}

	if exists {
		record, err := m.getMountTargetRecord(vmi)
		if err != nil {
			return err
		}

		for _, target := range record.MountTargetEntries {
			os.Remove(target.TargetFile)
			os.Remove(target.SocketFile)
		}

		os.Remove(recordFile)
	}

	m.mountRecordsLock.Lock()
	defer m.mountRecordsLock.Unlock()
	delete(m.mountRecords, vmi.UID)

	return nil
}

func (m *mounter) getMountTargetRecord(vmi *v1.VirtualMachineInstance) (*vmiMountTargetRecord, error) {
	var ok bool
	var existingRecord *vmiMountTargetRecord

	if string(vmi.UID) == "" {
		return nil, fmt.Errorf("unable to find container disk mounted directories for vmi without uid")
	}

	m.mountRecordsLock.Lock()
	defer m.mountRecordsLock.Unlock()
	existingRecord, ok = m.mountRecords[vmi.UID]

	// first check memory cache
	if ok {
		return existingRecord, nil
	}

	// if not there, see if record is on disk, this can happen if virt-handler restarts
	recordFile := filepath.Join(m.mountStateDir, string(vmi.UID))

	exists, err := diskutils.FileExists(recordFile)
	if err != nil {
		return nil, err
	}

	if exists {
		record := vmiMountTargetRecord{}
		bytes, err := ioutil.ReadFile(recordFile)
		if err != nil {
			return nil, err
		}
		err = json.Unmarshal(bytes, &record)
		if err != nil {
			return nil, err
		}

		m.mountRecords[vmi.UID] = &record
		return &record, nil
	}

	// not found
	return nil, nil
}

func (m *mounter) setMountTargetRecord(vmi *v1.VirtualMachineInstance, record *vmiMountTargetRecord) error {
	if string(vmi.UID) == "" {
		return fmt.Errorf("unable to set container disk mounted directories for vmi without uid")
	}

	recordFile := filepath.Join(m.mountStateDir, string(vmi.UID))
	fileExists, err := diskutils.FileExists(recordFile)
	if err != nil {
		return err
	}

	m.mountRecordsLock.Lock()
	defer m.mountRecordsLock.Unlock()

	existingRecord, ok := m.mountRecords[vmi.UID]
	if ok && fileExists && reflect.DeepEqual(existingRecord, record) {
		// already done
		return nil
	}

	bytes, err := json.Marshal(record)
	if err != nil {
		return err
	}

	err = os.MkdirAll(filepath.Dir(recordFile), 0755)
	if err != nil {
		return err
	}

	err = ioutil.WriteFile(recordFile, bytes, 0644)
	if err != nil {
		return err
	}

	m.mountRecords[vmi.UID] = record

	return nil
}

func (m *mounter) specContainsVolume(volumes []v1.Volume, volumeName string) bool {
	for _, volume := range volumes {
		if volume.Name == volumeName {
			return true
		}
	}
	return false
}

// TODO: Put this somewhere saner than here.
func (m *mounter) MountHotplugVolumes(vmi *v1.VirtualMachineInstance) error {
	record := vmiMountTargetRecord{}
	var targetPath *string
	var err error
	for volume, sourceUID := range vmi.Status.HotpluggedVolumes {
		if !m.specContainsVolume(vmi.Spec.Volumes, volume) {
			continue
		}
		log.DefaultLogger().Infof("Hotplug check volume name: %s", volume)
		targetPath = nil
		if m.IsBlockVolume(sourceUID) {
			targetPath, err = m.mountBlockHotplugVolume(vmi, volume, sourceUID)
			if err != nil {
				return err
			}
		} else {
			targetPath, err = m.mountFileSystemHotplugVolume(vmi, volume, sourceUID)
			if err != nil {
				return err
			}
		}
		if targetPath != nil {
			record.MountTargetEntries = append(record.MountTargetEntries, vmiMountTargetEntry{
				TargetFile: *targetPath,
			})
		}
	}

	if len(record.MountTargetEntries) > 0 {
		err := m.setMountTargetRecord(vmi, &record)
		if err != nil {
			return err
		}
	}

	return nil
}

func (m *mounter) mountFileSystemHotplugVolume(vmi *v1.VirtualMachineInstance, volume string, sourceUID types.UID) (*string, error) {
	sourceFiles, err := m.getSourcePodFiles(vmi, sourceUID)
	if err != nil {
		return nil, err
	}

	targetPath, _, err := m.getTargetPath(vmi, volume)
	if err != nil {
		return nil, err
	}
	log.DefaultLogger().Infof("Targetpath: %s", targetPath)

	nodeRes := isolation.NodeIsolationResult()

	if isMounted, err := nodeRes.IsMounted(targetPath); err != nil {
		return nil, fmt.Errorf("failed to determine if %s is already mounted: %v", targetPath, err)
	} else if !isMounted {
		log.DefaultLogger().Info("Not mounted")
		log.DefaultLogger().Infof("Sources: %v", sourceFiles)
		if len(sourceFiles) == 1 {
			for _, sourceFile := range sourceFiles {
				log.DefaultLogger().Infof("/usr/bin/virt-chroot --mount /proc/1/ns/mnt mount -o bind %s %s", strings.TrimPrefix(sourceFile, nodeRes.MountRoot()), targetPath)
				out, err := exec.Command("/usr/bin/virt-chroot", "--mount", "/proc/1/ns/mnt", "mount", "-o", "bind", strings.TrimPrefix(sourceFile, nodeRes.MountRoot()), targetPath).CombinedOutput()
				if err != nil {
					return nil, fmt.Errorf("failed to bindmount hotplug-disk %v: %v : %v", volume, string(out), err)
				}
			}
		}
	} else {
		log.DefaultLogger().Info("Skipping already mounted")
		return nil, nil
	}
	return &targetPath, nil
}

// mountBlockHotplugVolume will give every container in the pod access to the block device. Its because I can't tell which container is the
// container running the VM.  We need to determine if this is something we want or not. If it is possible to inject malicious side cars
// they can get access to the block devices. On the other hand they already have access to the volumes that were not hotplugged.
func (m *mounter) mountBlockHotplugVolume(vmi *v1.VirtualMachineInstance, volume string, sourceUID types.UID) (*string, error) {

	sourceMajor, sourceMinor, permissions, err := m.getSourceMajorMinor(vmi, sourceUID)
	if err != nil {
		return nil, err
	}
	targetPath, targetPodUID, err := m.getTargetPodPath(vmi)
	if err != nil {
		return nil, err
	}
	deviceName := filepath.Join(targetPath, volume)

	nodeRes := isolation.NodeIsolationResult()

	if isMounted, err := nodeRes.IsMounted(deviceName); err != nil {
		return nil, fmt.Errorf("failed to determine if %s is already mounted: %v", targetPath, err)
	} else if !isMounted {
		containerCGroupPaths, err := m.getTargetCgroupPaths(vmi, targetPodUID)
		if err != nil {
			return nil, err
		}

		for _, containerPath := range containerCGroupPaths {
			log.DefaultLogger().Infof("Allowing block device on %s", containerPath)
			// allow block devices
			if err := m.allowBlockMajorMinor(sourceMajor, sourceMinor, containerPath); err != nil {
				return nil, err
			}
		}
		device, err := m.createBlockDeviceFile(deviceName, sourceMajor, sourceMinor, permissions)
		if err != nil {
			return nil, err
		}
		return &device, nil
	} else {
		log.DefaultLogger().Info("Skipping already mounted")
		return nil, nil
	}
}

func (m *mounter) createBlockDeviceFile(deviceName string, major, minor int, blockDevicePermissions string) (string, error) {
	exists, err := diskutils.FileExists(deviceName)
	if err != nil {
		return "", err
	}
	if !exists {
		log.DefaultLogger().Infof("/usr/bin/mknod --mode 0%s %s b %d %d", blockDevicePermissions, deviceName, major, minor)
		out, err := exec.Command("/usr/bin/mknod", "--mode", fmt.Sprintf("0%s", blockDevicePermissions), deviceName, "b", strconv.Itoa(major), strconv.Itoa(minor)).CombinedOutput()
		if err != nil {
			log.DefaultLogger().Infof("failed to run mknod %s, err %s", string(out), err.Error())
			return "", err
		}
	}
	return deviceName, nil
}

func (m *mounter) removeBlockMajorMinor(major, minor int, path string) error {
	denyPath := filepath.Join(path, "devices.deny")
	return m.updateBlockMajorMinor(major, minor, denyPath)
}

func (m *mounter) allowBlockMajorMinor(major, minor int, path string) error {
	// example: echo 'b 252:16 rwm' > /sys/fs/cgroup/devices/kubepods.slice/kubepods-besteffort.slice/kubepods-besteffort-pod1f94197e_915d_43cf_ba30_6d6beaac7a61.slice/docker-a4ae23f2aa0794cf9c59e2f579fd809c44fda85d1ae305a9cda066f129a6267f.scope/devices.allow
	allowPath := filepath.Join(path, "devices.allow")
	return m.updateBlockMajorMinor(major, minor, allowPath)
}

func (m *mounter) updateBlockMajorMinor(major, minor int, fileName string) error {
	permission := fmt.Sprintf("b %d:%d rwm", major, minor)
	file, err := os.OpenFile(fileName, os.O_WRONLY, os.ModeAppend)
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = file.WriteString(permission)
	if err != nil {
		return err
	}
	return nil
}

// getTargetCgroupPaths returns all the container cgroup paths that are contained in the pod.
func (m *mounter) getTargetCgroupPaths(vmi *v1.VirtualMachineInstance, targetUID types.UID) ([]string, error) {
	basePath := "/proc/1/root/sys/fs/cgroup/devices/kubepods.slice/"
	matchString := strings.ReplaceAll(string(targetUID), "-", "_")
	var podCGroupPath string
	paths := make([]string, 0)
	log.DefaultLogger().Infof("Trying to find pod sys/fs/cgroup path, for UID: [%s]", targetUID)
	err := filepath.Walk(basePath, func(filePath string, info os.FileInfo, err error) error {
		if strings.Contains(filePath, matchString) {
			// found the pod path.
			podCGroupPath = filePath
			log.DefaultLogger().Infof("Found path: %s", filePath)
			return io.EOF
		}
		return nil
	})
	if err != nil && err != io.EOF {
		return paths, err
	}
	log.DefaultLogger().Infof("Finding containers starting at: %s", podCGroupPath)
	if podCGroupPath != "" {
		// found the pod path, now identify the container paths. The container paths end with ".scope"
		err := filepath.Walk(podCGroupPath, func(filePath string, info os.FileInfo, err error) error {
			if info.IsDir() && strings.HasSuffix(filePath, ".scope") {
				// found the container path.
				paths = append(paths, filePath)
			}
			return nil
		})
		if err != nil {
			return paths, err
		}
	} else {
		return paths, fmt.Errorf("Unable to find cgroup path for: %s", matchString)
	}

	log.DefaultLogger().Infof("Container paths: %v", paths)
	return paths, nil
}

func (m *mounter) getTargetPath(vmi *v1.VirtualMachineInstance, volumeName string) (string, types.UID, error) {
	targetPath, targetPodUID, err := m.getTargetPodPath(vmi)
	if err != nil {
		return targetPath, targetPodUID, err
	}
	diskPath := filepath.Join(targetPath, volumeName)
	exists, _ := diskutils.FileExists(diskPath)
	if !exists {
		err = os.Mkdir(diskPath, 0755)
		if err != nil {
			return diskPath, targetPodUID, err
		}
	}
	diskFile := filepath.Join(targetPath, volumeName)
	return diskFile, targetPodUID, err
}

func (m *mounter) getTargetPodPath(vmi *v1.VirtualMachineInstance) (string, types.UID, error) {
	var basepath string
	for podUID := range vmi.Status.ActivePods {
		basepath = fmt.Sprintf("/pods/%s/volumes/kubernetes.io~empty-dir/hotplug-disks", string(podUID))
		log.DefaultLogger().Infof("Checking path: %s", basepath)
		exists, _ := diskutils.FileExists(basepath)
		if exists {
			log.DefaultLogger().Infof("Found path: %s", basepath)
			return fmt.Sprintf("/var/lib/kubelet%s", basepath), podUID, nil
		}
	}
	return "", "", fmt.Errorf("Unable to locate target path")
}

func (m *mounter) getSourcePodFiles(vmi *v1.VirtualMachineInstance, sourceUID types.UID) ([]string, error) {
	// TODO: this doesn't work for multiple source pods, need to use some kind of disk identifier to make sure we find only one.
	paths := make([]string, 0)
	if sourceUID != types.UID("") {
		basepath := fmt.Sprintf("/proc/1/root/var/lib/kubelet/pods/%s/volumes", string(sourceUID))
		log.DefaultLogger().Infof("Source checking path: %s", basepath)
		err := filepath.Walk(basepath, func(filePath string, info os.FileInfo, err error) error {
			if path.Base(filePath) == "disk.img" {
				// Found disk image
				log.DefaultLogger().Infof("found source path: %s", filePath)
				paths = append(paths, path.Dir(filePath))
				return io.EOF
			}
			return nil
		})
		if err != nil && err != io.EOF {
			return paths, err
		}
	}
	return paths, nil
}

func (m *mounter) IsBlockFile(fileName string) bool {
	// Stat the file and see if there is no error
	out, err := exec.Command("/usr/bin/stat", fileName, "-L", "-c%t,%T,%a,%F").CombinedOutput()
	if err != nil {
		// Not a block device skip to next file
		return false
	}
	split := strings.Split(string(out), ",")
	// Verify I got 4 strings
	if len(split) != 4 {
		return false
	}
	return strings.TrimSpace(split[3]) == "block special file"
}
func (m *mounter) getBlockFileMajorMinor(fileName string) (int, int, string, error) {
	result := make([]int, 2)
	// Stat the file and see if there is no error
	out, err := exec.Command("/usr/bin/stat", fileName, "-L", "-c%t,%T,%a,%F").CombinedOutput()
	if err != nil {
		log.DefaultLogger().Errorf("error statting file %s, err: %s", fileName, err.Error())
		// Not a block device skip to next file
		return -1, -1, "", err
	}
	split := strings.Split(string(out), ",")
	// Verify I got 4 strings
	if len(split) != 4 {
		log.DefaultLogger().Infof("Split didn't produce expected result, len(%d) %s", len(split), string(out))
		return -1, -1, "", fmt.Errorf("Output invalid")
	}
	if strings.TrimSpace(split[3]) != "block special file" {
		log.DefaultLogger().Infof("Not a block device: %s", string(out))
		return -1, -1, "", fmt.Errorf("Not a block device")
	}
	// Verify that both values are ints.
	for i := 0; i < 2; i++ {
		result[i], err = strconv.Atoi(split[i])
		if err != nil {
			log.DefaultLogger().Errorf("Cannot convert %s to int, err: %s", split[i], err.Error())
			return -1, -1, "", err
		}
	}
	return result[0], result[1], split[2], nil
}

func (m *mounter) getSourceMajorMinor(vmi *v1.VirtualMachineInstance, sourceUID types.UID) (int, int, string, error) {
	///usr/bin/stat file -L -c%t,%T,%a,%F
	result := make([]int, 2)
	perms := ""
	if sourceUID != types.UID("") {
		basepath := fmt.Sprintf("/proc/1/root/var/lib/kubelet/pods/%s/volumeDevices", string(sourceUID))
		log.DefaultLogger().Infof("Source checking path for block devices: %s", basepath)
		//TODO: skip rest of walk once found.
		err := filepath.Walk(basepath, func(filePath string, info os.FileInfo, err error) error {
			if info != nil && !info.IsDir() && m.IsBlockFile(filePath) {
				result[0], result[1], perms, err = m.getBlockFileMajorMinor(filePath)
				// Err != nil means not a block device or unable to determine major/minor, try next file
				if err == nil {
					// Successfully located
					log.DefaultLogger().Infof("Found block device at %s resulting in Maj:%d, Min: %d perms: %s", filePath, result[0], result[1], perms)
					return io.EOF
				}
				return nil
			}
			return nil
		})
		if err != nil && err != io.EOF {
			return -1, -1, "", err
		}
	}
	if perms == "" {
		log.DefaultLogger().Info("Unable to find block device")
		return -1, -1, "", fmt.Errorf("Unable to find block device")
	}
	return result[0], result[1], perms, nil
}

func (m *mounter) IsBlockVolume(sourceUID types.UID) bool {
	// Check if the volumeDevices directory exists in the attachment pod, if so, its a block device, otherwise its file system.
	if sourceUID != types.UID("") {
		devicePath := fmt.Sprintf("/proc/1/root/var/lib/kubelet/pods/%s/volumeDevices", string(sourceUID))
		fileInfo, err := os.Stat(devicePath)
		if err != nil {
			// Can't stat the file, it doesn't exist, so not a blockVolume
			return false
		}
		return fileInfo.IsDir()
	}
	return false
}

// UnmountHotplugDisks unmounts all hotplug disks of a given VMI.
func (m *mounter) UnmountHotplugVolumes(vmi *v1.VirtualMachineInstance) error {
	log.DefaultLogger().Info("Unmounting start")
	if vmi.UID != "" {
		record, err := m.getMountTargetRecord(vmi)
		if err != nil {
			return err
		} else if record == nil {
			log.DefaultLogger().Info("No record found")
			// no entries to unmount
			return nil
		}

		log.DefaultLogger().Infof("Record found: %v", *record)

		currentHotplugPaths := make(map[string]types.UID, 0)

		_, targetUID, err := m.getTargetPodPath(vmi)
		if err != nil {
			return err
		}
		for volume, sourceUID := range vmi.Status.HotpluggedVolumes {
			if m.IsBlockVolume(sourceUID) {
				basePath, uid, err := m.getTargetPodPath(vmi)
				if err != nil {
					return err
				}
				path := filepath.Join(basePath, volume)
				log.DefaultLogger().Infof("Block volume path found: %s", path)
				currentHotplugPaths[path] = uid
			} else {
				path, uid, err := m.getTargetPath(vmi, volume)
				if err != nil {
					return err
				}
				log.DefaultLogger().Infof("Volume path found: %s", path)
				currentHotplugPaths[path] = uid
			}
		}
		for _, entry := range record.MountTargetEntries {
			diskPath := entry.TargetFile
			log.DefaultLogger().Infof("Checking if path mounted: %s", diskPath)
			if _, ok := currentHotplugPaths[diskPath]; !ok {
				if m.IsBlockFile(diskPath) {
					if err := m.unmountBlockHotplugVolumes(diskPath, vmi, targetUID); err != nil {
						return err
					}
				} else {
					if err := m.unmountFileSystemHotplugVolumes(diskPath); err != nil {
						return err
					}
				}
			}
		}
		err = m.deleteMountTargetRecord(vmi)
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *mounter) unmountFileSystemHotplugVolumes(diskPath string) error {
	log.DefaultLogger().Infof("Unmounting path: %s", diskPath)
	if mounted, err := isolation.NodeIsolationResult().IsMounted(diskPath); err != nil {
		return fmt.Errorf("failed to check mount point for hotplug disk %v: %v", diskPath, err)
	} else if mounted {
		out, err := exec.Command("/usr/bin/virt-chroot", "--mount", "/proc/1/ns/mnt", "umount", diskPath).CombinedOutput()
		if err != nil {
			return fmt.Errorf("failed to unmount hotplug disk %v: %v : %v", diskPath, string(out), err)
		}
		err = os.Remove(diskPath)
		if err != nil {
			return fmt.Errorf("failed to remove hotplug disk directory %v: %v : %v", diskPath, string(out), err)
		}

	} else {
		log.DefaultLogger().Info("Not mounted skipping")
	}
	return nil
}

func (m *mounter) unmountBlockHotplugVolumes(diskPath string, vmi *v1.VirtualMachineInstance, targetUID types.UID) error {
	// Get major and minor so we can deny the container.
	major, minor, _, err := m.getBlockFileMajorMinor(diskPath)
	if err != nil {
		return err
	}
	// Delete block device file
	err = os.Remove(diskPath)
	if err != nil {
		return err
	}
	paths, err := m.getTargetCgroupPaths(vmi, targetUID)
	for _, path := range paths {
		if err := m.removeBlockMajorMinor(major, minor, path); err != nil {
			return err
		}
	}
	return nil
}

// Mount takes a vmi and mounts all container disks of the VMI, so that they are visible for the qemu process.
// Additionally qcow2 images are validated if "verify" is true. The validation happens with rlimits set, to avoid DOS.
func (m *mounter) Mount(vmi *v1.VirtualMachineInstance, verify bool) error {
	record := vmiMountTargetRecord{}

	for i, volume := range vmi.Spec.Volumes {
		if volume.ContainerDisk != nil {
			targetFile, err := containerdisk.GetDiskTargetPathFromHostView(vmi, i)
			if err != nil {
				return err
			}

			sock, err := m.pathGetter(vmi, i)
			if err != nil {
				return err
			}

			record.MountTargetEntries = append(record.MountTargetEntries, vmiMountTargetEntry{
				TargetFile: targetFile,
				SocketFile: sock,
			})
		}
	}

	if len(record.MountTargetEntries) > 0 {
		err := m.setMountTargetRecord(vmi, &record)
		if err != nil {
			return err
		}
	}

	for i, volume := range vmi.Spec.Volumes {
		if volume.ContainerDisk != nil {
			targetFile, err := containerdisk.GetDiskTargetPathFromHostView(vmi, i)
			if err != nil {
				return err
			}

			nodeRes := isolation.NodeIsolationResult()

			if isMounted, err := nodeRes.IsMounted(targetFile); err != nil {
				return fmt.Errorf("failed to determine if %s is already mounted: %v", targetFile, err)
			} else if !isMounted {
				sock, err := m.pathGetter(vmi, i)
				if err != nil {
					return err
				}

				res, err := m.podIsolationDetector.DetectForSocket(vmi, sock)
				if err != nil {
					return fmt.Errorf("failed to detect socket for containerDisk %v: %v", volume.Name, err)
				}
				mountInfo, err := res.MountInfoRoot()
				if err != nil {
					return fmt.Errorf("failed to detect root mount info of containerDisk  %v: %v", volume.Name, err)
				}
				nodeMountInfo, err := nodeRes.ParentMountInfoFor(mountInfo)
				if err != nil {
					return fmt.Errorf("failed to detect root mount point of containerDisk %v on the node: %v", volume.Name, err)
				}
				sourceFile, err := containerdisk.GetImage(filepath.Join(nodeRes.MountRoot(), nodeMountInfo.Root, nodeMountInfo.MountPoint), volume.ContainerDisk.Path)
				if err != nil {
					return fmt.Errorf("failed to find a sourceFile in containerDisk %v: %v", volume.Name, err)
				}
				f, err := os.Create(targetFile)
				if err != nil {
					return fmt.Errorf("failed to create mount point target %v: %v", targetFile, err)
				}
				f.Close()

				if err = os.Chmod(sourceFile, 0444); err != nil {
					return fmt.Errorf("failed to change permisions on %s", sourceFile)
				}

				log.DefaultLogger().Object(vmi).Infof("Bind mounting container disk at %s to %s", strings.TrimPrefix(sourceFile, nodeRes.MountRoot()), targetFile)
				out, err := exec.Command("/usr/bin/virt-chroot", "--mount", "/proc/1/ns/mnt", "mount", "-o", "ro,bind", strings.TrimPrefix(sourceFile, nodeRes.MountRoot()), targetFile).CombinedOutput()
				if err != nil {
					return fmt.Errorf("failed to bindmount containerDisk %v: %v : %v", volume.Name, string(out), err)
				}
			}
			if verify {
				res, err := m.podIsolationDetector.Detect(vmi)
				if err != nil {
					return fmt.Errorf("failed to detect VMI pod: %v", err)
				}
				imageInfo, err := isolation.GetImageInfo(containerdisk.GetDiskTargetPathFromLauncherView(i), res)
				if err != nil {
					return fmt.Errorf("failed to get image info: %v", err)
				}

				if err := containerdisk.VerifyImage(imageInfo); err != nil {
					return fmt.Errorf("invalid image in containerDisk %v: %v", volume.Name, err)
				}
			}
		}
	}
	return nil
}

// Legacy Unmount unmounts all container disks of a given VMI when the hold HostPath method was in use.
// This exists for backwards compatibility for VMIs running before a KubeVirt update occurs.
func (m *mounter) legacyUnmount(vmi *v1.VirtualMachineInstance) error {
	mountDir := containerdisk.GetLegacyVolumeMountDirOnHost(vmi)

	files, err := ioutil.ReadDir(mountDir)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to list container disk mounts: %v", err)
	}

	if vmi.UID != "" {
		for _, file := range files {
			path := filepath.Join(mountDir, file.Name())
			if strings.HasSuffix(path, ".sock") {
				continue
			}
			if mounted, err := isolation.NodeIsolationResult().IsMounted(path); err != nil {
				return fmt.Errorf("failed to check mount point for containerDisk %v: %v", path, err)
			} else if mounted {
				out, err := exec.Command("/usr/bin/virt-chroot", "--mount", "/proc/1/ns/mnt", "umount", path).CombinedOutput()
				if err != nil {
					return fmt.Errorf("failed to unmount containerDisk %v: %v : %v", path, string(out), err)
				}
			}
		}

		if err := os.RemoveAll(mountDir); err != nil {
			return fmt.Errorf("failed to remove containerDisk files: %v", err)
		}
	}
	return nil
}

// Unmount unmounts all container disks of a given VMI.
func (m *mounter) Unmount(vmi *v1.VirtualMachineInstance) error {
	if vmi.UID != "" {

		// this will catch unmounting a vmi's container disk when
		// an old VMI is left over after a KubeVirt update
		err := m.legacyUnmount(vmi)
		if err != nil {
			return err
		}

		record, err := m.getMountTargetRecord(vmi)
		if err != nil {
			return err
		} else if record == nil {
			// no entries to unmount

			log.DefaultLogger().Object(vmi).Infof("No container disk mount entries found to unmount")
			return nil
		}

		log.DefaultLogger().Object(vmi).Infof("Found container disk mount entries")
		for _, entry := range record.MountTargetEntries {
			path := entry.TargetFile
			log.DefaultLogger().Object(vmi).Infof("Looking to see if containerdisk is mounted at path %s", path)
			if mounted, err := isolation.NodeIsolationResult().IsMounted(path); err != nil {
				return fmt.Errorf("failed to check mount point for containerDisk %v: %v", path, err)
			} else if mounted {
				log.DefaultLogger().Object(vmi).Infof("unmounting container disk at path %s", path)
				out, err := exec.Command("/usr/bin/virt-chroot", "--mount", "/proc/1/ns/mnt", "umount", path).CombinedOutput()
				if err != nil {
					return fmt.Errorf("failed to unmount containerDisk %v: %v : %v", path, string(out), err)
				}
			}

		}
		err = m.deleteMountTargetRecord(vmi)
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *mounter) ContainerDisksReady(vmi *v1.VirtualMachineInstance, notInitializedSince time.Time) (bool, error) {
	for i, volume := range vmi.Spec.Volumes {
		if volume.ContainerDisk != nil {
			_, err := m.pathGetter(vmi, i)
			if err != nil {
				log.DefaultLogger().Object(vmi).Infof("containerdisk %s not yet ready", volume.Name)
				if time.Now().After(notInitializedSince.Add(m.suppressWarningTimeout)) {
					return false, fmt.Errorf("containerdisk %s still not ready after one minute", volume.Name)
				}
				return false, nil
			}
		}
	}
	log.DefaultLogger().Object(vmi).V(4).Info("all containerdisks are ready")
	return true, nil
}
