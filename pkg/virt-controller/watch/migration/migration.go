/*
 * This file is part of the KubeVirt project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Copyright 2017, 2018 Red Hat, Inc.
 *
 */

package migration

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"kubevirt.io/kubevirt/pkg/pointer"
	backendstorage "kubevirt.io/kubevirt/pkg/storage/backend-storage"
	migrationproxy "kubevirt.io/kubevirt/pkg/virt-handler/migration-proxy"

	"github.com/opencontainers/selinux/go-selinux"

	"kubevirt.io/api/migrations/v1alpha1"

	k8sv1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"

	"k8s.io/apimachinery/pkg/util/intstr"

	"kubevirt.io/kubevirt/pkg/apimachinery/patch"
	"kubevirt.io/kubevirt/pkg/util"
	"kubevirt.io/kubevirt/pkg/util/pdbs"

	virtconfig "kubevirt.io/kubevirt/pkg/virt-config"

	"kubevirt.io/kubevirt/pkg/util/migrations"

	virtv1 "kubevirt.io/api/core/v1"
	"kubevirt.io/client-go/kubecli"
	"kubevirt.io/client-go/log"

	"kubevirt.io/kubevirt/pkg/controller"
	storagetypes "kubevirt.io/kubevirt/pkg/storage/types"
	"kubevirt.io/kubevirt/pkg/virt-controller/services"
	"kubevirt.io/kubevirt/pkg/virt-controller/watch/descheduler"
)

const (
	failedToProcessDeleteNotificationErrMsg   = "Failed to process delete notification"
	successfulUpdatePodDisruptionBudgetReason = "SuccessfulUpdate"
	failedUpdatePodDisruptionBudgetReason     = "FailedUpdate"
	failedGetAttractionPodsFmt                = "failed to get attachment pods: %v"
	migrationServiceLabelPrefix               = "kubevirt.io/migration"
	syncServiceLabelPrefix                    = "kubevirt.io/sync"
)

// This is the timeout used when a target pod is stuck in
// a pending unschedulable state.
const defaultUnschedulablePendingTimeoutSeconds = int64(60 * 5)

// This is how many finalized migration objects left in
// the system before we begin garbage collecting the oldest
// migration objects
const defaultFinalizedMigrationGarbageCollectionBuffer = 5

// This is catch all timeout used when a target pod is stuck in
// a in the pending phase for any reason. The theory behind this timeout
// being longer than the unschedulable timeout is that we don't necessarily
// know all the reasons a pod will be stuck in pending for an extended
// period of time, so we want to make this timeout long enough that it doesn't
// cause the migration to fail when it could have reasonably succeeded.
const defaultCatchAllPendingTimeoutSeconds = int64(60 * 15)

var migrationBackoffError = errors.New(controller.MigrationBackoffReason)

type Controller struct {
	templateService      services.TemplateService
	clientset            kubecli.KubevirtClient
	Queue                workqueue.TypedRateLimitingInterface[string]
	vmiStore             cache.Store
	podIndexer           cache.Indexer
	migrationIndexer     cache.Indexer
	vmIndexer            cache.Indexer
	nodeStore            cache.Store
	pvcStore             cache.Store
	storageClassStore    cache.Store
	storageProfileStore  cache.Store
	pdbIndexer           cache.Indexer
	migrationPolicyStore cache.Store
	resourceQuotaIndexer cache.Indexer
	recorder             record.EventRecorder
	podExpectations      *controller.UIDTrackingControllerExpectations
	pvcExpectations      *controller.UIDTrackingControllerExpectations
	migrationStartLock   *sync.Mutex
	clusterConfig        *virtconfig.ClusterConfig
	hasSynced            func() bool

	// the set of cancelled migrations before being handed off to virt-handler.
	// the map keys are migration keys
	handOffLock sync.Mutex
	handOffMap  map[string]struct{}

	unschedulablePendingTimeoutSeconds int64
	catchAllPendingTimeoutSeconds      int64
}

func NewController(templateService services.TemplateService,
	vmiInformer cache.SharedIndexInformer,
	vmInformer cache.SharedIndexInformer,
	podInformer cache.SharedIndexInformer,
	migrationInformer cache.SharedIndexInformer,
	nodeInformer cache.SharedIndexInformer,
	pvcInformer cache.SharedIndexInformer,
	storageClassInformer cache.SharedIndexInformer,
	storageProfileInformer cache.SharedIndexInformer,
	pdbInformer cache.SharedIndexInformer,
	migrationPolicyInformer cache.SharedIndexInformer,
	resourceQuotaInformer cache.SharedIndexInformer,
	recorder record.EventRecorder,
	clientset kubecli.KubevirtClient,
	clusterConfig *virtconfig.ClusterConfig,
) (*Controller, error) {

	c := &Controller{
		templateService: templateService,
		Queue: workqueue.NewTypedRateLimitingQueueWithConfig[string](
			workqueue.DefaultTypedControllerRateLimiter[string](),
			workqueue.TypedRateLimitingQueueConfig[string]{Name: "virt-controller-migration"},
		),
		vmiStore:             vmiInformer.GetStore(),
		podIndexer:           podInformer.GetIndexer(),
		migrationIndexer:     migrationInformer.GetIndexer(),
		vmIndexer:            vmInformer.GetIndexer(),
		nodeStore:            nodeInformer.GetStore(),
		pvcStore:             pvcInformer.GetStore(),
		storageClassStore:    storageClassInformer.GetStore(),
		storageProfileStore:  storageProfileInformer.GetStore(),
		pdbIndexer:           pdbInformer.GetIndexer(),
		resourceQuotaIndexer: resourceQuotaInformer.GetIndexer(),
		migrationPolicyStore: migrationPolicyInformer.GetStore(),
		recorder:             recorder,
		clientset:            clientset,
		podExpectations:      controller.NewUIDTrackingControllerExpectations(controller.NewControllerExpectations()),
		pvcExpectations:      controller.NewUIDTrackingControllerExpectations(controller.NewControllerExpectations()),
		migrationStartLock:   &sync.Mutex{},
		clusterConfig:        clusterConfig,
		handOffMap:           make(map[string]struct{}),

		unschedulablePendingTimeoutSeconds: defaultUnschedulablePendingTimeoutSeconds,
		catchAllPendingTimeoutSeconds:      defaultCatchAllPendingTimeoutSeconds,
	}

	c.hasSynced = func() bool {
		return vmiInformer.HasSynced() && podInformer.HasSynced() && migrationInformer.HasSynced() && pdbInformer.HasSynced() && resourceQuotaInformer.HasSynced()
	}

	_, err := vmiInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    c.addVMI,
		DeleteFunc: c.deleteVMI,
		UpdateFunc: c.updateVMI,
	})
	if err != nil {
		return nil, err
	}

	_, err = podInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    c.addPod,
		DeleteFunc: c.deletePod,
		UpdateFunc: c.updatePod,
	})
	if err != nil {
		return nil, err
	}

	_, err = migrationInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    c.addMigration,
		DeleteFunc: c.deleteMigration,
		UpdateFunc: c.updateMigration,
	})
	if err != nil {
		return nil, err
	}

	_, err = pdbInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		UpdateFunc: c.updatePDB,
	})
	if err != nil {
		return nil, err
	}

	_, err = pvcInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: c.addPVC,
	})

	_, err = resourceQuotaInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		UpdateFunc: c.updateResourceQuota,
		DeleteFunc: c.deleteResourceQuota,
	})
	if err != nil {
		return nil, err
	}

	return c, nil
}

func (c *Controller) Run(threadiness int, stopCh <-chan struct{}) {
	defer controller.HandlePanic()
	defer c.Queue.ShutDown()
	log.Log.Info("Starting migration controller.")

	// Wait for cache sync before we start the pod controller
	cache.WaitForCacheSync(stopCh, c.hasSynced)
	// Start the actual work
	for i := 0; i < threadiness; i++ {
		go wait.Until(c.runWorker, time.Second, stopCh)
	}

	<-stopCh
	log.Log.Info("Stopping migration controller.")
}

func (c *Controller) runWorker() {
	for c.Execute() {
	}
}

func (c *Controller) Execute() bool {
	key, quit := c.Queue.Get()
	if quit {
		return false
	}
	defer c.Queue.Done(key)
	err := c.execute(key)

	if err != nil {
		log.Log.Reason(err).Infof("reenqueuing Migration %v", key)
		c.Queue.AddRateLimited(key)
	} else {
		log.Log.V(4).Infof("processed Migration %v", key)
		c.Queue.Forget(key)
	}
	return true
}

func ensureSelectorLabelPresent(migration *virtv1.VirtualMachineInstanceMigration) {
	if migration.Labels == nil {
		migration.Labels = map[string]string{virtv1.MigrationSelectorLabel: migration.Spec.VMIName}
	} else if _, exist := migration.Labels[virtv1.MigrationSelectorLabel]; !exist {
		migration.Labels[virtv1.MigrationSelectorLabel] = migration.Spec.VMIName
	}
}

func (c *Controller) patchVMI(origVMI, newVMI *virtv1.VirtualMachineInstance) error {
	patchSet := patch.New()
	if !equality.Semantic.DeepEqual(origVMI.Status.MigrationState, newVMI.Status.MigrationState) {
		if origVMI.Status.MigrationState == nil {
			patchSet.AddOption(patch.WithAdd("/status/migrationState", newVMI.Status.MigrationState))
		} else {
			patchSet.AddOption(
				patch.WithTest("/status/migrationState", origVMI.Status.MigrationState),
				patch.WithReplace("/status/migrationState", newVMI.Status.MigrationState),
			)
		}
	}

	if !equality.Semantic.DeepEqual(origVMI.Labels, newVMI.Labels) {
		patchSet.AddOption(
			patch.WithTest("/metadata/labels", origVMI.Labels),
			patch.WithReplace("/metadata/labels", newVMI.Labels),
		)
	}

	if !equality.Semantic.DeepEqual(origVMI.Annotations, newVMI.Annotations) {
		patchSet.AddOption(
			patch.WithTest("/metadata/annotations", origVMI.Annotations),
			patch.WithReplace("/metadata/annotations", newVMI.Annotations),
		)
	}

	if !patchSet.IsEmpty() {
		patchBytes, err := patchSet.GeneratePayload()
		if err != nil {
			return err
		}
		if _, err = c.clientset.VirtualMachineInstance(origVMI.Namespace).Patch(context.Background(), origVMI.Name, types.JSONPatchType, patchBytes, v1.PatchOptions{}); err != nil {
			return err
		}
	}
	return nil
}

func (c *Controller) execute(key string) error {
	var vmi *virtv1.VirtualMachineInstance
	var targetPods []*k8sv1.Pod

	// Fetch the latest state from cache
	obj, exists, err := c.migrationIndexer.GetByKey(key)
	if err != nil {
		return err
	}

	if !exists {
		c.podExpectations.DeleteExpectations(key)
		c.removeHandOffKey(key)
		return nil
	}
	migration := obj.(*virtv1.VirtualMachineInstanceMigration)
	logger := log.Log.Object(migration)
	logger.Infof("processing migration: %s", key)

	// this must be first step in execution. Writing the object
	// when api version changes ensures our api stored version is updated.
	if !controller.ObservedLatestApiVersionAnnotation(migration) {
		migration := migration.DeepCopy()
		controller.SetLatestApiVersionAnnotation(migration)
		// Ensure the migration contains our selector label
		ensureSelectorLabelPresent(migration)
		_, err = c.clientset.VirtualMachineInstanceMigration(migration.Namespace).Update(context.Background(), migration, metav1.UpdateOptions{})
		return err
	}

	vmiObj, vmiExists, err := c.vmiStore.GetByKey(fmt.Sprintf("%s/%s", migration.Namespace, migration.Spec.VMIName))
	if err != nil {
		return err
	}

	if !vmiExists {
		var err error

		// Wait for the VMI to exist in case of a target VMIMigration
		if migration.Spec.Operation == nil || *migration.Spec.Operation != virtv1.MigrationTarget {
			if migration.DeletionTimestamp == nil {
				logger.V(3).Infof("Deleting migration for deleted vmi %s/%s", migration.Namespace, migration.Spec.VMIName)
				err = c.clientset.VirtualMachineInstanceMigration(migration.Namespace).Delete(context.Background(), migration.Name, v1.DeleteOptions{})
			}
			// nothing to process for a migration that's being deleted
			return err
		} else {
			vmiKey := controller.NamespacedKey(migration.Namespace, migration.Spec.VMIName)
			logger.Infof("Getting VM for key %s", vmiKey)
			obj, exists, err := c.vmIndexer.GetByKey(vmiKey)
			if err != nil {
				logger.Reason(err).Error("Failed to get VM")
				return err
			}
			if !exists {
				return nil
			} else {
				vm := obj.(*virtv1.VirtualMachine)
				if vm.Spec.RunStrategy != nil {
					// Save the run strategy for the target
					vm.Annotations[virtv1.RestoreRunStrategy] = string(*vm.Spec.RunStrategy)
				}
				vm.Spec.RunStrategy = pointer.P(virtv1.RunStrategyWaitAsReceiver)
				_, err = c.clientset.VirtualMachine(migration.Namespace).Update(context.Background(), vm, v1.UpdateOptions{})
				if err != nil {
					return err
				}
				return nil
			}
		}
	} else {
		vmi = vmiObj.(*virtv1.VirtualMachineInstance)
	}

	targetPods, err = c.listMatchingTargetPods(migration, vmi)
	if err != nil {
		return err
	}

	needsSync := c.podExpectations.SatisfiedExpectations(key) && c.pvcExpectations.SatisfiedExpectations(key)

	logger.V(4).Infof("processing migration: needsSync %t, hasVMI %t, targetPod len %d", needsSync, vmiExists, len(targetPods))

	var syncErr error

	if needsSync {
		syncErr = c.sync(key, migration, vmi, targetPods)
	}

	if vmi != nil {
		err = c.updateStatus(migration, vmi, targetPods, syncErr)
		if err != nil {
			return err
		}

		if syncErr != nil {
			return syncErr
		}

		if migration.IsFinal() {
			if err := c.deleteSourceVMI(migration, vmi); err != nil {
				return err
			}
			err = c.garbageCollectFinalizedMigrations(vmi)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (c *Controller) deleteSourceVMI(migration *virtv1.VirtualMachineInstanceMigration, vmi *virtv1.VirtualMachineInstance) error {
	if migration.IsSource() && !migration.IsTarget() && vmi.Status.MigrationState != nil {
		vmiKey := controller.NamespacedKey(migration.Namespace, migration.Spec.VMIName)
		log.Log.Infof("Shutting down VMI, Getting VM for key %s", vmiKey)
		obj, exists, err := c.vmIndexer.GetByKey(vmiKey)
		if err != nil {
			log.Log.Reason(err).Error("Shutting down VMI, Failed to get VM")
			return err
		}
		if exists {
			vm := obj.(*virtv1.VirtualMachine)
			vm.Spec.RunStrategy = pointer.P(virtv1.RunStrategyHalted)
			_, err = c.clientset.VirtualMachine(migration.Namespace).Update(context.Background(), vm, v1.UpdateOptions{})
			if err != nil {
				return err
			}
		}
		if err := c.clientset.VirtualMachineInstance(vmi.Namespace).Delete(context.Background(), vmi.Name, v1.DeleteOptions{}); err != nil {
			return err
		}
	}
	return nil
}

func (c *Controller) canMigrateVMI(migration *virtv1.VirtualMachineInstanceMigration, vmi *virtv1.VirtualMachineInstance) (bool, error) {

	if vmi.Status.MigrationState == nil {
		return true, nil
	} else if vmi.Status.MigrationState.SourceMigrationUID == migration.UID || vmi.Status.MigrationState.TargetMigrationUID == migration.UID {
		return true, nil
	} else if vmi.Status.MigrationState.SourceMigrationUID == "" && vmi.Status.MigrationState.TargetMigrationUID == "" {
		return true, nil
	}

	sourceCurMigrationUID := vmi.Status.MigrationState.SourceMigrationUID
	targetCurMigrationUID := vmi.Status.MigrationState.TargetMigrationUID

	// check to see if the curMigrationUID still exists or is finalized
	objs, err := c.migrationIndexer.ByIndex(cache.NamespaceIndex, migration.Namespace)

	if err != nil {
		return false, err
	}
	for _, obj := range objs {
		curMigration := obj.(*virtv1.VirtualMachineInstanceMigration)
		if curMigration.UID != sourceCurMigrationUID && curMigration.UID != targetCurMigrationUID {
			continue
		}
		if migration.Spec.Operation != nil {
			log.DefaultLogger().Infof("Migration operation: %s", *migration.Spec.Operation)
		}
		if curMigration.Spec.Operation != nil {
			log.DefaultLogger().Infof("Current migration operation: %s", *curMigration.Spec.Operation)
		}
		log.DefaultLogger().Info("Checking")
		// Allow migration source and target to be in flight at the same time
		if migration.Spec.Operation != nil && *migration.Spec.Operation == virtv1.MigrationSource &&
			curMigration.Spec.Operation != nil && *curMigration.Spec.Operation == virtv1.MigrationTarget {
			log.DefaultLogger().Info("Skipping because source and target migration are in flight")
			continue
		}

		if curMigration.IsFinal() {
			// If the other job already completed, it's okay to take over the migration.
			return true, nil
		}
		log.DefaultLogger().Info("Returning false on canMigrateVMI")
		return false, nil
	}

	return true, nil
}

func (c *Controller) failMigration(migration *virtv1.VirtualMachineInstanceMigration) error {
	err := backendstorage.MigrationAbort(c.clientset, migration)
	if err != nil {
		return err
	}
	migration.Status.Phase = virtv1.MigrationFailed
	return nil
}

func (c *Controller) updateStatus(migration *virtv1.VirtualMachineInstanceMigration, vmi *virtv1.VirtualMachineInstance, pods []*k8sv1.Pod, syncError error) error {

	var pod *k8sv1.Pod = nil
	var attachmentPod *k8sv1.Pod = nil
	conditionManager := controller.NewVirtualMachineInstanceMigrationConditionManager()
	migrationCopy := migration.DeepCopy()
	logger := log.Log.With("migration name", migration.Name)

	podExists, attachmentPodExists := len(pods) > 0, false
	if podExists {
		pod = pods[0]

		if attachmentPods, err := controller.AttachmentPods(pod, c.podIndexer); err != nil {
			return fmt.Errorf(failedGetAttractionPodsFmt, err)
		} else {
			attachmentPodExists = len(attachmentPods) > 0
			if attachmentPodExists {
				attachmentPod = attachmentPods[0]
			}
		}
	}

	// Remove the finalizer and conditions if the migration has already completed
	if migration.IsFinal() {
		if controller.HasFinalizer(migration, virtv1.VirtualMachineInstanceMigrationFinalizer) {
			log.Log.Object(migration).Info("Migration is final")
			if vmi.Status.MigrationState != nil && (migration.UID == vmi.Status.MigrationState.SourceMigrationUID || migration.UID == vmi.Status.MigrationState.TargetMigrationUID) {
				// Store the finalized migration state data from the VMI status in the migration object
				migrationCopy.Status.MigrationState = vmi.Status.MigrationState
			}

			syncNode, err := c.getSyncNodeForVMI(vmi)
			if err != nil {
				return err
			}
			if syncNode != nil {
				migrationTargetNode, err := c.getMigrationTargetNodeForVMI(vmi)
				if err != nil {
					return err
				}
				migrationVirthandler, err := c.getVirtHandlerForNode(migrationTargetNode)
				if err != nil {
					return err
				}
				syncVirthandler, err := c.getVirtHandlerForNode(syncNode)
				if err != nil {
					return err
				}
				if err := c.removeMigrationLabelFromVirtHandler(migration, migrationVirthandler); err != nil {
					return err
				}
				if err := c.removeSyncLabelFromVirtHandler(migration, syncVirthandler); err != nil {
					return err
				}
				if err := c.removeMigrationService(migration, vmi, migrationVirthandler.Namespace); err != nil {
					return err
				}
				if err := c.removeSyncService(migration, vmi, syncVirthandler.Namespace); err != nil {
					return err
				}
			}
			log.Log.Object(migration).Info("Migration is final, removing finalizer")
			// remove the migration finalizer
			controller.RemoveFinalizer(migrationCopy, virtv1.VirtualMachineInstanceMigrationFinalizer)

			// Clean up after the migration completes.
			if migration.IsSource() {
				if err := c.completeVMISourceMigration(migrationCopy, vmi); err != nil {
					return err
				}
			} else if migration.IsTarget() {
				if err := c.completeVMITargetMigration(migrationCopy, vmi); err != nil {
					return err
				}
			}
		}
		// Status checking of active Migration job.
		//
		// 1. Fail if VMI isn't in running state.
		// 2. Fail if target pod exists and has gone down for any reason.
		// 3. Begin progressing migration state based on VMI's MigrationState status.
	} else if vmi == nil && migration.IsTarget() {
		logger.Info("Failed migration because VMI is nil")
		err := c.failMigration(migrationCopy)
		if err != nil {
			return err
		}
		c.recorder.Eventf(migration, k8sv1.EventTypeWarning, controller.FailedMigrationReason, "Migration failed because vmi does not exist.")
		logger.Object(migration).Error("vmi does not exist")
	} else if vmi.IsFinal() {
		logger.Info("Failed migration because VMI is final")
		err := c.failMigration(migrationCopy)
		if err != nil {
			return err
		}
		c.recorder.Eventf(migration, k8sv1.EventTypeWarning, controller.FailedMigrationReason, "Migration failed vmi shutdown during migration.")
		logger.Object(migration).Error("Unable to migrate vmi because vmi is shutdown.")
	} else if migration.DeletionTimestamp != nil && !c.isMigrationHandedOff(migration, vmi) {
		c.recorder.Eventf(migration, k8sv1.EventTypeWarning, controller.FailedMigrationReason, "Migration failed due to being canceled")
		if !conditionManager.HasCondition(migration, virtv1.VirtualMachineInstanceMigrationAbortRequested) {
			condition := virtv1.VirtualMachineInstanceMigrationCondition{
				Type:          virtv1.VirtualMachineInstanceMigrationAbortRequested,
				Status:        k8sv1.ConditionTrue,
				LastProbeTime: v1.Now(),
			}
			migrationCopy.Status.Conditions = append(migrationCopy.Status.Conditions, condition)
		}
		logger.Info("Failed migration because migration was canceled")
		err := c.failMigration(migrationCopy)
		if err != nil {
			return err
		}
	} else if podExists && controller.PodIsDown(pod) {
		logger.Info("Failed migration because pod is down")
		err := c.failMigration(migrationCopy)
		if err != nil {
			return err
		}
		c.recorder.Eventf(migration, k8sv1.EventTypeWarning, controller.FailedMigrationReason, "Migration failed because target pod shutdown during migration")
		logger.Object(migration).Errorf("target pod %s/%s shutdown during migration", pod.Namespace, pod.Name)
	} else if migration.TargetIsCreated() && !podExists && !migration.IsSource() {
		log.Log.Info("Failed migration because target is created but pod does not exist")
		err := c.failMigration(migrationCopy)
		if err != nil {
			return err
		}
		c.recorder.Eventf(migration, k8sv1.EventTypeWarning, controller.FailedMigrationReason, "Migration target pod was removed during active migration.")
		log.Log.Object(migration).Error("target pod disappeared during migration")
	} else if migration.TargetIsHandedOff() && vmi.Status.MigrationState == nil {
		logger.Infof("Failed migration because target is handed off and migration state is nil, vmi: %s/%s, phase: %s", migration.Namespace, migration.Name, migration.Status.Phase)
		err := c.failMigration(migrationCopy)
		if err != nil {
			return err
		}
		c.recorder.Eventf(migration, k8sv1.EventTypeWarning, controller.FailedMigrationReason, "VMI's migration state was cleared during the active migration.")
		log.Log.Object(migration).Error("vmi migration state cleared during migration")
	} else if migration.TargetIsHandedOff() &&
		vmi.Status.MigrationState != nil &&
		((migration.IsTarget() && vmi.Status.MigrationState.TargetMigrationUID != migration.UID) || migration.IsSource() && vmi.Status.MigrationState.SourceMigrationUID != migration.UID) {
		logger.Info("Failed migration because target is handed off and migration state target migration uid does not match")
		logger.Infof("soure: %s, target: %s, migration: %s", vmi.Status.MigrationState.SourceMigrationUID, vmi.Status.MigrationState.TargetMigrationUID, migration.UID)
		err := c.failMigration(migrationCopy)
		if err != nil {
			return err
		}
		c.recorder.Eventf(migration, k8sv1.EventTypeWarning, controller.FailedMigrationReason, "VMI's migration state was taken over by another migration job during active migration.")
		log.Log.Object(migration).Error("vmi's migration state was taken over by another migration object")
	} else if vmi.Status.MigrationState != nil &&
		((migration.IsTarget() && vmi.Status.MigrationState.TargetMigrationUID == migration.UID) || (migration.IsSource() && vmi.Status.MigrationState.SourceMigrationUID == migration.UID)) &&
		vmi.Status.MigrationState.Failed {
		logger.Info("Failed migration source node reported migration failed")
		err := c.failMigration(migrationCopy)
		if err != nil {
			return err
		}
		c.recorder.Eventf(migration, k8sv1.EventTypeWarning, controller.FailedMigrationReason, "source node reported migration failed")
		logger.Object(migration).Errorf("VMI %s/%s reported migration failed", vmi.Namespace, vmi.Name)

	} else if migration.DeletionTimestamp != nil && !migration.IsFinal() &&
		!conditionManager.HasCondition(migration, virtv1.VirtualMachineInstanceMigrationAbortRequested) {
		condition := virtv1.VirtualMachineInstanceMigrationCondition{
			Type:          virtv1.VirtualMachineInstanceMigrationAbortRequested,
			Status:        k8sv1.ConditionTrue,
			LastProbeTime: v1.Now(),
		}
		migrationCopy.Status.Conditions = append(migrationCopy.Status.Conditions, condition)
	} else if attachmentPodExists && controller.PodIsDown(attachmentPod) {
		err := c.failMigration(migrationCopy)
		if err != nil {
			return err
		}
		c.recorder.Eventf(migration, k8sv1.EventTypeWarning, controller.FailedMigrationReason, "Migration failed because target attachment pod shutdown during migration")
		logger.Object(migration).Errorf("target attachment pod %s/%s shutdown during migration", attachmentPod.Namespace, attachmentPod.Name)
	} else {
		logger.Info("Processing migration phase")
		err := c.processMigrationPhase(migration, migrationCopy, pod, attachmentPod, vmi, syncError)
		if err != nil {
			return err
		}
	}

	if migrationCopy.Status.Phase == virtv1.MigrationFailed {
		if err := descheduler.MarkSourcePodEvictionCompleted(c.clientset, migrationCopy, c.podIndexer); err != nil {
			return err
		}
	}

	controller.SetVMIMigrationPhaseTransitionTimestamp(migration, migrationCopy)
	controller.SetSourcePod(migrationCopy, vmi, c.podIndexer)

	if !equality.Semantic.DeepEqual(migration.Finalizers, migrationCopy.Finalizers) {
		log.Log.Object(migration).Infof("Updating migration")
		_, err := c.clientset.VirtualMachineInstanceMigration(migrationCopy.Namespace).Update(context.Background(), migrationCopy, metav1.UpdateOptions{})
		if err != nil {
			return err
		}
	} else if !equality.Semantic.DeepEqual(migration.Status, migrationCopy.Status) {
		log.Log.Object(migration).Infof("Updating migration status, phase: %s", migrationCopy.Status.Phase)
		_, err := c.clientset.VirtualMachineInstanceMigration(migrationCopy.Namespace).UpdateStatus(context.Background(), migrationCopy, v1.UpdateOptions{})
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *Controller) processMigrationPhase(
	migration, migrationCopy *virtv1.VirtualMachineInstanceMigration,
	pod, attachmentPod *k8sv1.Pod,
	vmi *virtv1.VirtualMachineInstance,
	syncError error,
) error {
	logger := log.Log.Object(migration)
	conditionManager := controller.NewVirtualMachineInstanceMigrationConditionManager()
	vmiConditionManager := controller.NewVirtualMachineInstanceConditionManager()
	switch migration.Status.Phase {
	case virtv1.MigrationPhaseUnset:
		canMigrate, err := c.canMigrateVMI(migration, vmi)
		if err != nil {
			return err
		}

		if canMigrate {
			if migration.IsTarget() {
				migrationCopy.Status.Phase = virtv1.MigrationWaitingForVMISyncEndpoint
			} else {
				migrationCopy.Status.Phase = virtv1.MigrationPending
			}
		} else {
			// can not migrate because there is an active migration already
			// in progress for this VMI.
			err := c.failMigration(migrationCopy)
			if err != nil {
				return err
			}
			c.recorder.Eventf(migration, k8sv1.EventTypeWarning, controller.FailedMigrationReason, "VMI is not eligible for migration because another migration job is in progress.")
			log.Log.Object(migration).Error("Migration object not eligible for migration because another job is in progress")
		}
	case virtv1.MigrationWaitingForVMISyncEndpoint:
		if vmi.Status.MigrationState != nil && len(vmi.Status.MigrationState.TargetDirectMigrationNodePorts) > 0 {
			// Build sync endpoint, and service
			for realPort, port := range vmi.Status.MigrationState.TargetDirectMigrationNodePorts {
				logger.Infof("Target Ready Checking if port %d matches %d, and real port %s", port, migrationproxy.VMISyncPort, realPort)
				if port == migrationproxy.VMISyncPort {
					logger.Infof("MigrationState: %+v", vmi.Status.MigrationState)
					if vmi.Status.MigrationState.MigrationNetworkType == virtv1.Pod {
						if err := c.createSyncService(migration, vmi); err != nil {
							return err
						}

						serviceIP, err := c.getSyncServiceNameForVMI(migration, vmi)
						if err != nil {
							return err
						}
						if serviceIP != "" {
							migrationCopy.Status.SyncEndpoint = pointer.P(net.JoinHostPort(serviceIP, realPort))
						}
					} else if vmi.Status.MigrationState.TargetSyncAddress != "" {
						migrationCopy.Status.SyncEndpoint = pointer.P(net.JoinHostPort(vmi.Status.MigrationState.TargetSyncAddress, realPort))
					}
				}
			}
			if migrationCopy.Status.SyncEndpoint != nil {
				migrationCopy.Status.Phase = virtv1.MigrationWaitingForVMISync
			}
		}
	case virtv1.MigrationWaitingForVMISync:
		if vmi.IsSyncedWithRemote() {
			// Sync happened, switch to MigrationPendingTargetVMI
			migrationCopy.Status.Phase = virtv1.MigrationPendingTargetVMI
		}
	case virtv1.MigrationPending, virtv1.MigrationPendingTargetVMI:
		if migration.IsTarget() {
			if pod != nil {
				if controller.VMIHasHotplugVolumes(vmi) {
					if attachmentPod != nil {
						migrationCopy.Status.Phase = virtv1.MigrationScheduling
					}
				} else {
					migrationCopy.Status.Phase = virtv1.MigrationScheduling
				}
			} else if syncError != nil && strings.Contains(syncError.Error(), "exceeded quota") && !conditionManager.HasCondition(migration, virtv1.VirtualMachineInstanceMigrationRejectedByResourceQuota) {
				condition := virtv1.VirtualMachineInstanceMigrationCondition{
					Type:          virtv1.VirtualMachineInstanceMigrationRejectedByResourceQuota,
					Status:        k8sv1.ConditionTrue,
					LastProbeTime: v1.Now(),
				}
				migrationCopy.Status.Conditions = append(migrationCopy.Status.Conditions, condition)
			}
		} else if migration.IsSource() {
			migrationCopy.Status.Phase = virtv1.MigrationScheduling
		}
	case virtv1.MigrationScheduling:
		if conditionManager.HasCondition(migrationCopy, virtv1.VirtualMachineInstanceMigrationRejectedByResourceQuota) {
			conditionManager.RemoveCondition(migrationCopy, virtv1.VirtualMachineInstanceMigrationRejectedByResourceQuota)
		}
		if migration.IsSource() || controller.IsPodReady(pod) {
			if controller.VMIHasHotplugVolumes(vmi) {
				if attachmentPod != nil && controller.IsPodReady(attachmentPod) {
					log.Log.Object(migration).Infof("Attachment pod %s for vmi %s/%s is ready", attachmentPod.Name, vmi.Namespace, vmi.Name)
					migrationCopy.Status.Phase = virtv1.MigrationScheduled
				}
			} else {
				migrationCopy.Status.Phase = virtv1.MigrationScheduled
			}
		}
	case virtv1.MigrationScheduled:
		if vmi.Status.MigrationState != nil {
			logger.Infof("Migration scheduled, isSource %t, UID matches: %t, isTarget %t, target UID matches: %t", vmi.IsMigrationSource(), vmi.Status.MigrationState.SourceMigrationUID == migration.UID, vmi.IsMigrationTarget(), vmi.Status.MigrationState.TargetMigrationUID == migration.UID)
			logger.Infof("Does the migration match source or target: %t", (vmi.IsMigrationSource() && vmi.Status.MigrationState.SourceMigrationUID == migration.UID) || (vmi.IsMigrationTarget() && vmi.Status.MigrationState.TargetMigrationUID == migration.UID))
			logger.Infof("TargetNode: %s", vmi.Status.MigrationState.TargetNode)
		}
		if vmi.Status.MigrationState != nil &&
			((vmi.IsMigrationSource() && vmi.Status.MigrationState.SourceMigrationUID == migration.UID) ||
				(vmi.IsMigrationTarget() && vmi.Status.MigrationState.TargetMigrationUID == migration.UID)) &&
			vmi.Status.MigrationState.TargetNode != "" {
			migrationCopy.Status.Phase = virtv1.MigrationPreparingTarget
		} else {
			if vmi.Status.MigrationState != nil {
				logger.Infof("didn't match migration state, is target node set? %t", vmi.Status.MigrationState.TargetNode != "")
			} else {
				logger.Infof("didn't match migration state, migration state is nil")
			}
		}
	case virtv1.MigrationPreparingTarget:
		if migration.IsSource() {
			migrationPortAvailable := false
			for _, targetPort := range vmi.Status.MigrationState.TargetDirectMigrationNodePorts {
				log.Log.Infof("Target port %d", targetPort)
				if targetPort != 0 && targetPort != migrationproxy.VMISyncPort {
					migrationPortAvailable = true
					break
				}
			}
			log.Log.Object(migration).Info("Checking if we should move to target ready")
			if migrationPortAvailable && migration.IsSource() && !migration.IsTarget() {
				log.Log.Info("Setting migration target ready")
				migrationCopy.Status.Phase = virtv1.MigrationTargetReady
			} else {
				log.Log.Object(migration).Infof("Is port available? %t, is source %t, is target %t", migrationPortAvailable, migration.IsSource(), migration.IsTarget())
			}
		}
		if migration.IsTarget() && vmi.Status.MigrationState.TargetNode != "" && vmi.Status.MigrationState.TargetNodeAddress != "" {
			migrationCopy.Status.Phase = virtv1.MigrationTargetReady
		}
	case virtv1.MigrationTargetReady:
		if vmi.Status.MigrationState.StartTimestamp != nil {
			logger.Info("Setting migration to running")
			migrationCopy.Status.Phase = virtv1.MigrationRunning
		}
	case virtv1.MigrationRunning:
		if pod != nil {
			_, exists := pod.Annotations[virtv1.MigrationTargetReadyTimestamp]
			if !exists && vmi.Status.MigrationState.TargetNodeDomainReadyTimestamp != nil {
				if backendstorage.IsBackendStorageNeededForVMI(&vmi.Spec) {
					err := backendstorage.MigrationHandoff(c.clientset, c.pvcStore, migration)
					if err != nil {
						return err
					}
				}

				patchBytes, err := patch.New(
					patch.WithAdd(fmt.Sprintf("/metadata/annotations/%s", patch.EscapeJSONPointer(virtv1.MigrationTargetReadyTimestamp)), vmi.Status.MigrationState.TargetNodeDomainReadyTimestamp.String()),
				).GeneratePayload()
				if err != nil {
					return err
				}

				if _, err = c.clientset.CoreV1().Pods(pod.Namespace).Patch(context.Background(), pod.Name, types.JSONPatchType, patchBytes, v1.PatchOptions{}); err != nil {
					return err
				}
			}
		} else {
			log.Log.Object(migration).Error("Target pod is nil")
		}
		if vmi.Status.MigrationState.Completed &&
			!vmiConditionManager.HasCondition(vmi, virtv1.VirtualMachineInstanceVCPUChange) &&
			!vmiConditionManager.HasConditionWithStatus(vmi, virtv1.VirtualMachineInstanceMemoryChange, k8sv1.ConditionTrue) {
			migrationCopy.Status.Phase = virtv1.MigrationSucceeded
			c.recorder.Eventf(migration, k8sv1.EventTypeNormal, controller.SuccessfulMigrationReason, "Source node reported migration succeeded")
			log.Log.Object(migration).Infof("VMI reported migration succeeded.")
		}
	}
	return nil
}

func (c *Controller) removeSettingUpSyncLabelFromVMI(vmi *virtv1.VirtualMachineInstance) error {
	vmiCopy := vmi.DeepCopy()
	delete(vmiCopy.Labels, virtv1.MigrationSyncLabel)
	log.Log.Info("Removeing setting up sync label from VMI")
	_, err := c.clientset.VirtualMachineInstance(vmi.Namespace).Update(context.Background(), vmiCopy, v1.UpdateOptions{})
	return err
}

func (c *Controller) getMigrationServiceNameForVMI(migration *virtv1.VirtualMachineInstanceMigration, vmi *virtv1.VirtualMachineInstance) (string, error) {
	targetNode, err := c.getMigrationTargetNodeForVMI(vmi)
	if err != nil {
		return "", err
	}
	virthandler, err := c.getVirtHandlerForNode(targetNode)
	if err != nil {
		return "", err
	}
	service, err := c.getMigrationService(migration, virthandler.Namespace)
	if err != nil {
		return "", err
	}
	// return service.Spec.ClusterIP, nil
	return fmt.Sprintf("%s.%s.svc", service.Name, service.Namespace), nil
}

func (c *Controller) getSyncServiceNameForVMI(migration *virtv1.VirtualMachineInstanceMigration, vmi *virtv1.VirtualMachineInstance) (string, error) {
	syncNode, err := c.getSyncNodeForVMI(vmi)
	if err != nil {
		return "", err
	}
	if syncNode == nil {
		return "", nil
	}
	virthandler, err := c.getVirtHandlerForNode(syncNode)
	if err != nil {
		return "", err
	}
	service, err := c.getSyncService(migration, virthandler.Namespace)
	if err != nil {
		return "", err
	}
	// return service.Spec.ClusterIP, nil
	return fmt.Sprintf("%s.%s.svc", service.Name, service.Namespace), nil
}

func setTargetPodSELinuxLevel(pod *k8sv1.Pod, vmiSeContext string) error {
	// The target pod may share resources with the sources pod (RWX disks for example)
	// Therefore, it needs to share the same SELinux categories to inherit the same permissions
	// Note: there is a small probablility that the target pod will share the same categories as another pod on its node.
	//   It is a slight security concern, but not as bad as removing categories on all shared objects for the duration of the migration.
	if vmiSeContext == "none" {
		// The SelinuxContext is explicitly set to "none" when SELinux is not present
		return nil
	}
	if vmiSeContext == "" {
		return fmt.Errorf("SELinux context not set on VMI status")
	} else {
		seContext, err := selinux.NewContext(vmiSeContext)
		if err != nil {
			return err
		}
		level, exists := seContext["level"]
		if exists && level != "" {
			// The SELinux context looks like "system_u:object_r:container_file_t:s0:c1,c2", we care about "s0:c1,c2"
			if pod.Spec.SecurityContext == nil {
				pod.Spec.SecurityContext = &k8sv1.PodSecurityContext{}
			}
			pod.Spec.SecurityContext.SELinuxOptions = &k8sv1.SELinuxOptions{
				Level: level,
			}
		}
	}

	return nil
}

func (c *Controller) createTargetPod(migration *virtv1.VirtualMachineInstanceMigration, vmi *virtv1.VirtualMachineInstance, sourcePod *k8sv1.Pod) error {
	if !c.pvcExpectations.SatisfiedExpectations(controller.MigrationKey(migration)) {
		// Give time to the PVC informer to update itself
		return nil
	}
	templatePod, err := c.templateService.RenderMigrationManifest(vmi, migration, sourcePod)
	if err != nil {
		return fmt.Errorf("failed to render launch manifest: %v", err)
	}

	if vmi.Status.MigrationState != nil && vmi.Status.MigrationState.SourceNode != "" {
		log.Log.Object(migration).Infof("Node anti affinity set to %s", vmi.Status.MigrationState.SourceNode)
		// Node anti affinity set create anti affinity rules
		// for the migration target pod
		if templatePod.Spec.Affinity == nil {
			templatePod.Spec.Affinity = &k8sv1.Affinity{}
		}
		if templatePod.Spec.Affinity.NodeAffinity == nil {
			templatePod.Spec.Affinity.NodeAffinity = &k8sv1.NodeAffinity{}
		}
		if templatePod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution == nil {
			templatePod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution = &k8sv1.NodeSelector{}
		}
		if templatePod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms == nil {
			templatePod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms = []k8sv1.NodeSelectorTerm{}
		}
		templatePod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms[0].MatchExpressions = append(templatePod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms[0].MatchExpressions,
			k8sv1.NodeSelectorRequirement{
				Key:      k8sv1.LabelHostname,
				Operator: k8sv1.NodeSelectorOpNotIn,
				Values:   []string{vmi.Status.MigrationState.SourceNode},
			},
		)
		log.Log.Object(migration).Infof("pod node affinity %#v", templatePod.Spec.Affinity.NodeAffinity)
	} else {
		antiAffinityTerm := k8sv1.PodAffinityTerm{
			LabelSelector: &v1.LabelSelector{
				MatchLabels: map[string]string{
					virtv1.CreatedByLabel: string(vmi.UID),
				},
			},
			TopologyKey: k8sv1.LabelHostname,
		}
		antiAffinityRule := &k8sv1.PodAntiAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: []k8sv1.PodAffinityTerm{antiAffinityTerm},
		}

		if templatePod.Spec.Affinity == nil {
			templatePod.Spec.Affinity = &k8sv1.Affinity{
				PodAntiAffinity: antiAffinityRule,
			}
		} else if templatePod.Spec.Affinity.PodAntiAffinity == nil {
			templatePod.Spec.Affinity.PodAntiAffinity = antiAffinityRule
		} else {
			templatePod.Spec.Affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution = append(templatePod.Spec.Affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution, antiAffinityTerm)
		}
	}
	templatePod.ObjectMeta.Labels[virtv1.MigrationJobLabel] = string(migration.UID)
	templatePod.ObjectMeta.Annotations[virtv1.MigrationJobNameAnnotation] = migration.Name

	// If cpu model is "host model" allow migration only to nodes that supports this cpu model
	if cpu := vmi.Spec.Domain.CPU; cpu != nil && cpu.Model == virtv1.CPUModeHostModel {
		var nodeSelectors map[string]string

		if migration.Spec.Operation == nil || *migration.Spec.Operation != virtv1.MigrationTarget {
			node, err := c.getNodeForVMI(vmi)
			if err != nil {
				return err
			}
			nodeSelectors, err = prepareNodeSelectorForHostCpuModel(node, templatePod, sourcePod)
		} else {
			nodeSelectors, err = getNodeSelectorsFromVMIMigrationState(vmi.Status.MigrationState)
		}
		if err != nil {
			return err
		}
		for k, v := range nodeSelectors {
			templatePod.Spec.NodeSelector[k] = v
		}
	}

	matchLevelOnTarget := c.clusterConfig.GetMigrationConfiguration().MatchSELinuxLevelOnMigration
	if (matchLevelOnTarget == nil || *matchLevelOnTarget) && !(migration.Spec.Operation != nil && *migration.Spec.Operation == virtv1.MigrationTarget) {
		err = setTargetPodSELinuxLevel(templatePod, vmi.Status.SelinuxContext)
		if err != nil {
			return err
		}
	}

	// This is used by the functional test to simulate failures
	computeImageOverride, ok := migration.Annotations[virtv1.FuncTestMigrationTargetImageOverrideAnnotation]
	if ok && computeImageOverride != "" {
		for i, container := range templatePod.Spec.Containers {
			if container.Name == "compute" {
				container.Image = computeImageOverride
				templatePod.Spec.Containers[i] = container
				break
			}
		}
	}

	key := controller.MigrationKey(migration)
	c.podExpectations.ExpectCreations(key, 1)
	pod, err := c.clientset.CoreV1().Pods(vmi.GetNamespace()).Create(context.Background(), templatePod, v1.CreateOptions{})
	if err != nil {
		if k8serrors.IsForbidden(err) && strings.Contains(err.Error(), "violates PodSecurity") {
			err = fmt.Errorf("failed to create target pod for vmi %s/%s, it needs a privileged namespace to run: %w", vmi.GetNamespace(), vmi.GetName(), err)
			c.recorder.Eventf(vmi, k8sv1.EventTypeWarning, controller.FailedCreatePodReason, services.FailedToRenderLaunchManifestErrFormat, err)

		} else {
			c.recorder.Eventf(vmi, k8sv1.EventTypeWarning, controller.FailedCreatePodReason, "Error creating pod: %v", err)
			err = fmt.Errorf("failed to create vmi migration target pod: %v", err)
		}
		c.podExpectations.CreationObserved(key)
		return err
	}
	log.Log.Object(vmi).Infof("Created migration target pod %s/%s with uuid %s for migration %s with uuid %s", pod.Namespace, pod.Name, string(pod.UID), migration.Name, string(migration.UID))
	c.recorder.Eventf(migration, k8sv1.EventTypeNormal, controller.SuccessfulCreatePodReason, "Created migration target pod %s", pod.Name)
	return nil
}

func (c *Controller) expandPDB(pdb *policyv1.PodDisruptionBudget, vmi *virtv1.VirtualMachineInstance, vmim *virtv1.VirtualMachineInstanceMigration) error {
	minAvailable := 2

	if pdb.Spec.MinAvailable != nil && pdb.Spec.MinAvailable.IntValue() == minAvailable && pdb.Labels[virtv1.MigrationNameLabel] == vmim.Name {
		log.Log.V(4).Object(vmi).Infof("PDB has been already expanded")
		return nil
	}

	patchBytes := []byte(fmt.Sprintf(`{"spec":{"minAvailable": %d},"metadata":{"labels":{"%s": "%s"}}}`, minAvailable, virtv1.MigrationNameLabel, vmim.Name))

	_, err := c.clientset.PolicyV1().PodDisruptionBudgets(pdb.Namespace).Patch(context.Background(), pdb.Name, types.StrategicMergePatchType, patchBytes, v1.PatchOptions{})
	if err != nil {
		c.recorder.Eventf(vmi, k8sv1.EventTypeWarning, failedUpdatePodDisruptionBudgetReason, "Error expanding the PodDisruptionBudget %s: %v", pdb.Name, err)
		return err
	}
	log.Log.Object(vmi).Infof("expanding pdb for VMI %s/%s to protect migration %s", vmi.Namespace, vmi.Name, vmim.Name)
	c.recorder.Eventf(vmi, k8sv1.EventTypeNormal, successfulUpdatePodDisruptionBudgetReason, "Expanded PodDisruptionBudget %s", pdb.Name)
	return nil
}

// handleMigrationBackoff introduce a backoff (when needed) only for migrations
// created by the evacuation controller.
func (c *Controller) handleMigrationBackoff(key string, vmi *virtv1.VirtualMachineInstance, migration *virtv1.VirtualMachineInstanceMigration) error {
	if _, exists := migration.Annotations[virtv1.FuncTestForceIgnoreMigrationBackoffAnnotation]; exists {
		return nil
	}
	_, existsEvacMig := migration.Annotations[virtv1.EvacuationMigrationAnnotation]
	_, existsWorkUpdMig := migration.Annotations[virtv1.WorkloadUpdateMigrationAnnotation]
	if !existsEvacMig && !existsWorkUpdMig {
		return nil
	}

	migrations, err := c.listBackoffEligibleMigrations(vmi.Namespace, vmi.Name)
	if err != nil {
		return err
	}
	if len(migrations) < 2 {
		return nil
	}

	// Newest first
	sort.Sort(sort.Reverse(vmimCollection(migrations)))
	if migrations[0].UID != migration.UID {
		return nil
	}

	backoff := time.Second * 0
	for _, m := range migrations[1:] {
		if m.Status.Phase == virtv1.MigrationSucceeded {
			break
		}
		if m.DeletionTimestamp != nil {
			continue
		}

		if m.Status.Phase == virtv1.MigrationFailed {
			if backoff == 0 {
				backoff = time.Second * 20
			} else {
				backoff = backoff * 2
			}
		}
	}
	if backoff == 0 {
		return nil
	}

	getFailedTS := func(migration *virtv1.VirtualMachineInstanceMigration) metav1.Time {
		for _, ts := range migration.Status.PhaseTransitionTimestamps {
			if ts.Phase == virtv1.MigrationFailed {
				return ts.PhaseTransitionTimestamp
			}
		}
		return metav1.Time{}
	}

	outOffBackoffTS := getFailedTS(migrations[1]).Add(backoff)
	backoff = outOffBackoffTS.Sub(time.Now())

	if backoff > 0 {
		log.Log.Object(vmi).Errorf("vmi in migration backoff, re-enqueueing after %v", backoff)
		c.Queue.AddAfter(key, backoff)
		return migrationBackoffError
	}
	return nil
}

func (c *Controller) handleMarkMigrationFailedOnVMI(migration *virtv1.VirtualMachineInstanceMigration, vmi *virtv1.VirtualMachineInstance) error {

	// Mark Migration Done on VMI if virt handler never started it.
	// Once virt-handler starts the migration, it's up to handler
	// to finalize it.

	vmiCopy := vmi.DeepCopy()

	now := v1.NewTime(time.Now())
	vmiCopy.Status.MigrationState.StartTimestamp = &now
	vmiCopy.Status.MigrationState.EndTimestamp = &now
	vmiCopy.Status.MigrationState.Failed = true
	vmiCopy.Status.MigrationState.Completed = true

	err := c.patchVMI(vmi, vmiCopy)
	if err != nil {
		log.Log.Reason(err).Object(vmi).Errorf("Failed to patch VMI status to indicate migration %s/%s failed.", migration.Namespace, migration.Name)
		return err
	}
	log.Log.Object(vmi).Infof("Marked Migration %s/%s failed on vmi due to target pod disappearing before migration kicked off.", migration.Namespace, migration.Name)
	failureReason := "Target pod is down"
	c.recorder.Event(vmi, k8sv1.EventTypeWarning, controller.FailedMigrationReason, fmt.Sprintf("VirtualMachineInstance migration uid %s failed. reason: %s", string(migration.UID), failureReason))
	if vmiCopy.Status.MigrationState.FailureReason == "" {
		// Only set the failure reason if empty, as virt-handler may already have provided a better one
		vmiCopy.Status.MigrationState.FailureReason = failureReason
	}

	return nil
}

func (c *Controller) handlePreHandoffMigrationCancel(migration *virtv1.VirtualMachineInstanceMigration, vmi *virtv1.VirtualMachineInstance, pod *k8sv1.Pod) error {
	if pod == nil {
		return nil
	}

	c.podExpectations.ExpectDeletions(controller.MigrationKey(migration), []string{controller.PodKey(pod)})
	err := c.clientset.CoreV1().Pods(pod.Namespace).Delete(context.Background(), pod.Name, v1.DeleteOptions{})
	if err != nil {
		c.podExpectations.DeletionObserved(controller.MigrationKey(migration), controller.PodKey(pod))
		c.recorder.Eventf(migration, k8sv1.EventTypeWarning, controller.FailedDeletePodReason, "Error deleting canceled migration target pod: %v", err)
		return fmt.Errorf("cannot delete pending target pod %s/%s for migration although migration is aborted", pod.Name, pod.Namespace)
	}

	reason := fmt.Sprintf("migration canceled and pod %s/%s is deleted", pod.Namespace, pod.Name)
	log.Log.Object(vmi).Infof("Deleted pending migration target pod with uuid %s for migration %s with uuid %s with reason [%s]", string(pod.UID), migration.Name, string(migration.UID), reason)
	c.recorder.Event(migration, k8sv1.EventTypeNormal, controller.SuccessfulDeletePodReason, reason)
	return nil
}

func (c *Controller) markVMISourceMigration(migration *virtv1.VirtualMachineInstanceMigration, vmi *virtv1.VirtualMachineInstance) error {
	vmiCopy := vmi.DeepCopy()
	if len(vmiCopy.Annotations) == 0 {
		vmiCopy.Annotations = make(map[string]string)
	}
	if vmiCopy.Status.MigrationState == nil {
		vmiCopy.Status.MigrationState = &virtv1.VirtualMachineInstanceMigrationState{}
	}

	vmiCopy.Status.MigrationState.SourceMigrationUID = migration.UID
	if migration.Spec.ConnectURL != nil {
		vmiCopy.Status.MigrationState.TargetSyncAddress = *migration.Spec.ConnectURL
	} else {
		return fmt.Errorf("connectURL is nil")
	}
	vmiCopy.Annotations[virtv1.CreateMigrationSource] = "true"
	if err := c.patchVMI(vmi, vmiCopy); err != nil {
		c.recorder.Eventf(migration, k8sv1.EventTypeWarning, controller.FailedHandOverPodReason, fmt.Sprintf("Failed to set MigrationState in VMI status. :%v", err))
		return err
	}
	return nil
}

func (c *Controller) completeVMISourceMigration(migration *virtv1.VirtualMachineInstanceMigration, vmi *virtv1.VirtualMachineInstance) error {
	vmiCopy := vmi.DeepCopy()
	delete(vmiCopy.Annotations, virtv1.CreateMigrationSource)
	if err := c.patchVMI(vmi, vmiCopy); err != nil {
		c.recorder.Eventf(migration, k8sv1.EventTypeWarning, controller.FailedMigrationReason, fmt.Sprintf("Failed to set MigrationState in VMI status. :%v", err))
		return err
	}
	return nil
}

func (c *Controller) completeVMITargetMigration(migration *virtv1.VirtualMachineInstanceMigration, vmi *virtv1.VirtualMachineInstance) error {
	vmiCopy := vmi.DeepCopy()
	delete(vmiCopy.Annotations, virtv1.CreateMigrationTarget)
	delete(vmiCopy.Annotations, virtv1.WaitForSyncTarget)
	delete(vmiCopy.Annotations, virtv1.SyncTargetNodeName)
	delete(vmiCopy.Labels, virtv1.MigrationSyncLabel)
	vmiCopy.Status.Phase = virtv1.Running
	if err := c.patchVMI(vmi, vmiCopy); err != nil {
		c.recorder.Eventf(migration, k8sv1.EventTypeWarning, controller.FailedMigrationReason, fmt.Sprintf("Failed to set MigrationState in VMI status. :%v", err))
		return err
	}
	return nil
}

func (c *Controller) updateVMITargetRemoteNodeAddress(migration *virtv1.VirtualMachineInstanceMigration, vmi *virtv1.VirtualMachineInstance) error {
	vmiCopy := vmi.DeepCopy()

	clusterIP, err := c.getMigrationServiceNameForVMI(migration, vmi)
	if err != nil {
		return err
	}
	vmiCopy.Status.MigrationState.RemoteTargetNodeAddress = clusterIP
	if err := c.patchVMI(vmi, vmiCopy); err != nil {
		c.recorder.Eventf(migration, k8sv1.EventTypeWarning, controller.FailedHandOverPodReason, fmt.Sprintf("Failed to set MigrationState in VMI status. :%v", err))
		return err
	}
	return nil
}

func (c *Controller) setSourceNodeAndVMIUID(migration *virtv1.VirtualMachineInstanceMigration, vmi *virtv1.VirtualMachineInstance) error {
	vmiCopy := vmi.DeepCopy()
	log.Log.Object(migration).Infof("Setting source node %s in VMI %s/%s", vmi.Status.NodeName, vmi.Namespace, vmi.Name)
	if len(vmiCopy.Annotations) == 0 {
		vmiCopy.Annotations = make(map[string]string)
	}
	sourceNode := vmi.Status.NodeName
	if len(vmiCopy.Labels) == 0 {
		vmiCopy.Labels = make(map[string]string)
	}
	vmiCopy.Labels[virtv1.MigrationSyncLabel] = "true"
	if migration.Spec.NodeAntiAffinity != nil {
		sourceNode = *migration.Spec.NodeAntiAffinity
	}
	if migration.Status.MigrationState == nil {
		vmiCopy.Status.MigrationState = &virtv1.VirtualMachineInstanceMigrationState{}
	}
	vmiCopy.Status.MigrationState.SourceNode = sourceNode
	vmiCopy.Status.MigrationState.SourceVirtualMachineInstanceUID = vmi.UID
	var err error
	vmiCopy.Status.MigrationState.SourceNodeSelectors, err = c.getNodeSelectorsFromNodeName(vmiCopy.Status.MigrationState.SourceNode)
	if err != nil {
		return err
	}
	return c.patchVMI(vmi, vmiCopy)
}

func (c *Controller) getNodeSelectorsFromNodeName(nodeName string) (map[string]string, error) {
	obj, exists, err := c.nodeStore.GetByKey(nodeName)
	if err != nil {
		return nil, err
	}
	res := make(map[string]string)
	if exists {
		node := obj.(*k8sv1.Node)
		for key, value := range node.Labels {
			if strings.HasPrefix(key, virtv1.HostModelCPULabel) || strings.HasPrefix(key, virtv1.HostModelRequiredFeaturesLabel) {
				res[key] = value
			}
		}
	}
	return res, nil
}

func (c *Controller) handleTargetPodHandoff(migration *virtv1.VirtualMachineInstanceMigration, vmi *virtv1.VirtualMachineInstance, pod *k8sv1.Pod) error {

	if vmi.Status.MigrationState != nil && vmi.Status.MigrationState.TargetMigrationUID == migration.UID {
		// already handed off
		return nil
	}

	vmiCopy := vmi.DeepCopy()
	if len(vmiCopy.Annotations) == 0 {
		vmiCopy.Annotations = make(map[string]string)
	}
	vmiCopy.Status.MigrationState = &virtv1.VirtualMachineInstanceMigrationState{
		TargetNode: pod.Spec.NodeName,
		SourceNode: vmi.Status.NodeName,
		TargetPod:  pod.Name,
	}
	if migration.IsTarget() {
		vmiCopy.Annotations[virtv1.CreateMigrationTarget] = "true"
		vmiCopy.Status.MigrationState.TargetMigrationUID = migration.UID
		vmiCopy.Status.MigrationState.TargetDomainName = vmi.Name
		vmiCopy.Status.MigrationState.TargetDomainNamespace = vmi.Namespace
		vmiCopy.Status.MigrationState.TargetVirtualMachineInstanceUID = vmi.UID
	}
	if migration.Status.MigrationState != nil {
		vmiCopy.Status.MigrationState.SourcePod = migration.Status.MigrationState.SourcePod
		vmiCopy.Status.MigrationState.SourcePersistentStatePVCName = migration.Status.MigrationState.SourcePersistentStatePVCName
		vmiCopy.Status.MigrationState.TargetPersistentStatePVCName = migration.Status.MigrationState.TargetPersistentStatePVCName
	}

	// By setting this label, virt-handler on the target node will receive
	// the vmi and prepare the local environment for the migration
	vmiCopy.ObjectMeta.Labels[virtv1.MigrationTargetNodeNameLabel] = pod.Spec.NodeName

	if controller.VMIHasHotplugVolumes(vmiCopy) {
		attachmentPods, err := controller.AttachmentPods(pod, c.podIndexer)
		if err != nil {
			return fmt.Errorf(failedGetAttractionPodsFmt, err)
		}
		if len(attachmentPods) > 0 {
			log.Log.Object(migration).Infof("Target attachment pod for vmi %s/%s: %s", vmiCopy.Namespace, vmiCopy.Name, string(attachmentPods[0].UID))
			vmiCopy.Status.MigrationState.TargetAttachmentPodUID = attachmentPods[0].UID
		} else {
			return fmt.Errorf("target attachment pod not found")
		}
	}

	clusterMigrationConfigs := c.clusterConfig.GetMigrationConfiguration().DeepCopy()
	err := c.matchMigrationPolicy(vmiCopy, clusterMigrationConfigs)
	if err != nil {
		return fmt.Errorf("failed to match migration policy: %v", err)
	}

	if !c.isMigrationPolicyMatched(vmiCopy) {
		vmiCopy.Status.MigrationState.MigrationConfiguration = clusterMigrationConfigs
	}

	if controller.VMIHasHotplugCPU(vmi) && vmi.IsCPUDedicated() {
		cpuLimitsCount, err := getTargetPodLimitsCount(pod)
		if err != nil {
			return err
		}
		vmiCopy.ObjectMeta.Labels[virtv1.VirtualMachinePodCPULimitsLabel] = strconv.Itoa(int(cpuLimitsCount))
	}

	if controller.VMIHasHotplugMemory(vmi) {
		memoryReq, err := getTargetPodMemoryRequests(pod)
		if err != nil {
			return err
		}
		vmiCopy.ObjectMeta.Labels[virtv1.VirtualMachinePodMemoryRequestsLabel] = memoryReq
	}

	if backendStoragePVC := backendstorage.PVCForMigrationTarget(c.pvcStore, migration); backendStoragePVC != nil {
		bs := backendstorage.NewBackendStorage(c.clientset, c.clusterConfig, c.storageClassStore, c.storageProfileStore, c.pvcStore)
		bs.UpdateVolumeStatus(vmiCopy, backendStoragePVC)
	}

	err = c.patchVMI(vmi, vmiCopy)
	if err != nil {
		c.recorder.Eventf(migration, k8sv1.EventTypeWarning, controller.FailedHandOverPodReason, fmt.Sprintf("Failed to set MigrationState in VMI status. :%v", err))
		return err
	}

	c.addHandOffKey(controller.MigrationKey(migration))
	log.Log.Object(vmi).Infof("Handed off migration %s/%s to target virt-handler.", migration.Namespace, migration.Name)
	c.recorder.Eventf(migration, k8sv1.EventTypeNormal, controller.SuccessfulHandOverPodReason, "Migration target pod is ready for preparation by virt-handler.")
	return nil
}

func (c *Controller) markMigrationAbortInVmiStatus(migration *virtv1.VirtualMachineInstanceMigration, vmi *virtv1.VirtualMachineInstance) error {

	if vmi.Status.MigrationState == nil {
		return fmt.Errorf("migration state is nil when trying to mark migratio abortion in vmi status")
	}

	vmiCopy := vmi.DeepCopy()

	vmiCopy.Status.MigrationState.AbortRequested = true
	if !equality.Semantic.DeepEqual(vmi.Status, vmiCopy.Status) {

		newStatus := vmiCopy.Status
		oldStatus := vmi.Status
		patchBytes, err := patch.New(
			patch.WithTest("/status", oldStatus),
			patch.WithReplace("/status", newStatus),
		).GeneratePayload()
		if err != nil {
			return err
		}

		_, err = c.clientset.VirtualMachineInstance(vmi.Namespace).Patch(context.Background(), vmi.Name, types.JSONPatchType, patchBytes, v1.PatchOptions{})
		if err != nil {
			msg := fmt.Sprintf("failed to set MigrationState in VMI status. :%v", err)
			c.recorder.Eventf(migration, k8sv1.EventTypeWarning, controller.FailedAbortMigrationReason, msg)
			return fmt.Errorf(msg)
		}
		log.Log.Object(vmi).Infof("Signaled migration %s/%s to be aborted.", migration.Namespace, migration.Name)
		c.recorder.Eventf(migration, k8sv1.EventTypeNormal, controller.SuccessfulAbortMigrationReason, "Migration is ready to be canceled by virt-handler.")
	}

	return nil
}

func isMigrationProtected(pdb *policyv1.PodDisruptionBudget) bool {
	return pdb.Status.DesiredHealthy == 2 && pdb.Generation == pdb.Status.ObservedGeneration
}

func filterOutOldPDBs(pdbList []*policyv1.PodDisruptionBudget) []*policyv1.PodDisruptionBudget {
	var filteredPdbs []*policyv1.PodDisruptionBudget

	for i := range pdbList {
		if !pdbs.IsPDBFromOldMigrationController(pdbList[i]) {
			filteredPdbs = append(filteredPdbs, pdbList[i])
		}
	}
	return filteredPdbs
}

func (c *Controller) handleTargetPodCreation(key string, migration *virtv1.VirtualMachineInstanceMigration, vmi *virtv1.VirtualMachineInstance, sourcePod *k8sv1.Pod) error {

	c.migrationStartLock.Lock()
	defer c.migrationStartLock.Unlock()

	// Don't start new migrations if we wait for cache updates on migration target pods
	if c.podExpectations.AllPendingCreations() > 0 {
		c.Queue.AddAfter(key, 1*time.Second)
		return nil
	} else if controller.VMIActivePodsCount(vmi, c.podIndexer) > 1 {
		log.Log.Object(migration).Infof("Waiting to schedule target pod for migration because there are already multiple pods running for vmi %s/%s", vmi.Namespace, vmi.Name)
		c.Queue.AddAfter(key, 1*time.Second)
		return nil

	}

	// Don't start new migrations if we wait for migration object updates because of new target pods
	runningMigrations, err := c.findRunningMigrations()
	if err != nil {
		return fmt.Errorf("failed to determin the number of running migrations: %v", err)
	}

	// XXX: Make this configurable, think about limit per node, bandwidth per migration, and so on.
	if len(runningMigrations) >= int(*c.clusterConfig.GetMigrationConfiguration().ParallelMigrationsPerCluster) {
		log.Log.Object(migration).Infof("Waiting to schedule target pod for vmi [%s/%s] migration because total running parallel migration count [%d] is currently at the global cluster limit.", vmi.Namespace, vmi.Name, len(runningMigrations))
		// Let's wait until some migrations are done
		c.Queue.AddAfter(key, time.Second*5)
		return nil
	}

	outboundMigrations, err := c.outboundMigrationsOnNode(vmi.Status.NodeName, runningMigrations)

	if err != nil {
		return err
	}

	if outboundMigrations >= int(*c.clusterConfig.GetMigrationConfiguration().ParallelOutboundMigrationsPerNode) {
		// Let's ensure that we only have two outbound migrations per node
		// XXX: Make this configurable, thinkg about inbound migration limit, bandwidh per migration, and so on.
		log.Log.Object(migration).Infof("Waiting to schedule target pod for vmi [%s/%s] migration because total running parallel outbound migrations on target node [%d] has hit outbound migrations per node limit.", vmi.Namespace, vmi.Name, outboundMigrations)
		c.Queue.AddAfter(key, time.Second*5)
		return nil
	}

	// migration was accepted into the system, now see if we
	// should create the target pod
	if vmi.IsRunning() || vmi.IsMigrationTarget() {
		if migrations.VMIMigratableOnEviction(c.clusterConfig, vmi) {
			pdbs, err := pdbs.PDBsForVMI(vmi, c.pdbIndexer)
			if err != nil {
				return err
			}
			// removes pdbs from old implementation from list.
			pdbs = filterOutOldPDBs(pdbs)

			if len(pdbs) < 1 {
				log.Log.Object(vmi).Errorf("Found no PDB protecting the vmi")
				return fmt.Errorf("Found no PDB protecting the vmi %s", vmi.Name)
			}
			pdb := pdbs[0]

			if err := c.expandPDB(pdb, vmi, migration); err != nil {
				return err
			}

			// before proceeding we have to check that the k8s pdb controller has processed
			// the pdb expansion and is actually protecting the VMI migration
			if !isMigrationProtected(pdb) {
				log.Log.V(4).Object(migration).Infof("Waiting for the pdb-controller to protect the migration pods, postponing migration start")
				return nil
			}
		}
		err = c.handleBackendStorage(migration, vmi)
		if err != nil {
			return err
		}
		if migration.IsTarget() && sourcePod == nil {
			// This should not happen since we generate a source pod in the caller if we
			// are in target mode
			log.Log.Info("Generating source pod while in target mode")
			// This is a migration to another namespace or cluster. Generate the source pod
			// template from the VMI so we can use it to create the target pod.
			sourcePod, err = c.templateService.RenderLaunchManifest(vmi)
			if err != nil {
				return fmt.Errorf("failed to render launch manifest: %v", err)
			}
			//TODO: Run the same validations here as happen in vmi.go?
		}
		log.Log.Object(migration).Info("Creating target pod")
		return c.createTargetPod(migration, vmi, sourcePod)
	}
	log.Log.Object(vmi).Info("Returning, VMI is not running or target")
	return nil
}

func (c *Controller) handleBackendStorage(migration *virtv1.VirtualMachineInstanceMigration, vmi *virtv1.VirtualMachineInstance) error {
	if !backendstorage.IsBackendStorageNeededForVMI(&vmi.Spec) {
		return nil
	}
	if migration.Status.MigrationState == nil {
		migration.Status.MigrationState = &virtv1.VirtualMachineInstanceMigrationState{}
	}
	migration.Status.MigrationState.SourcePersistentStatePVCName = backendstorage.CurrentPVCName(vmi)
	if migration.Status.MigrationState.SourcePersistentStatePVCName == "" {
		return fmt.Errorf("no backend-storage PVC found in VMI volume status")
	}

	pvc := backendstorage.PVCForMigrationTarget(c.pvcStore, migration)
	if pvc != nil {
		migration.Status.MigrationState.TargetPersistentStatePVCName = pvc.Name
	}
	if migration.Status.MigrationState.TargetPersistentStatePVCName != "" {
		// backend storage pvc has already been created or has ReadWriteMany access-mode
		return nil
	}
	bs := backendstorage.NewBackendStorage(c.clientset, c.clusterConfig, c.storageClassStore, c.storageProfileStore, c.pvcStore)
	vmiKey, err := controller.KeyFunc(vmi)
	if err != nil {
		return err
	}
	c.pvcExpectations.ExpectCreations(vmiKey, 1)
	backendStoragePVC, err := bs.CreatePVCForMigrationTarget(vmi, migration.Name)
	if err != nil {
		c.pvcExpectations.CreationObserved(vmiKey)
		return err
	}
	migration.Status.MigrationState.TargetPersistentStatePVCName = backendStoragePVC.Name
	if migration.Status.MigrationState.SourcePersistentStatePVCName == migration.Status.MigrationState.TargetPersistentStatePVCName {
		// The PVC is shared between source and target, satisfy the expectation since the creation will never happen
		c.pvcExpectations.CreationObserved(vmiKey)
	}

	return nil
}

func (c *Controller) createAttachmentPod(migration *virtv1.VirtualMachineInstanceMigration, vmi *virtv1.VirtualMachineInstance, virtLauncherPod *k8sv1.Pod) error {
	sourcePod, err := controller.CurrentVMIPod(vmi, c.podIndexer)
	if err != nil {
		return fmt.Errorf("failed to get current VMI pod: %v", err)
	}

	volumes := controller.GetHotplugVolumes(vmi, sourcePod)

	volumeNamesPVCMap, err := storagetypes.VirtVolumesToPVCMap(volumes, c.pvcStore, virtLauncherPod.Namespace)
	if err != nil {
		return fmt.Errorf("failed to get PVC map: %v", err)
	}

	// Reset the hotplug volume statuses to enforce mount
	vmiCopy := vmi.DeepCopy()
	vmiCopy.Status.VolumeStatus = []virtv1.VolumeStatus{}
	attachmentPodTemplate, err := c.templateService.RenderHotplugAttachmentPodTemplate(volumes, virtLauncherPod, vmiCopy, volumeNamesPVCMap)
	if err != nil {
		return fmt.Errorf("failed to render attachment pod template: %v", err)
	}

	if attachmentPodTemplate.ObjectMeta.Labels == nil {
		attachmentPodTemplate.ObjectMeta.Labels = make(map[string]string)
	}

	if attachmentPodTemplate.ObjectMeta.Annotations == nil {
		attachmentPodTemplate.ObjectMeta.Annotations = make(map[string]string)
	}

	attachmentPodTemplate.ObjectMeta.Labels[virtv1.MigrationJobLabel] = string(migration.UID)
	attachmentPodTemplate.ObjectMeta.Annotations[virtv1.MigrationJobNameAnnotation] = migration.Name

	key := controller.MigrationKey(migration)
	c.podExpectations.ExpectCreations(key, 1)

	attachmentPod, err := c.clientset.CoreV1().Pods(vmi.GetNamespace()).Create(context.Background(), attachmentPodTemplate, v1.CreateOptions{})
	if err != nil {
		c.podExpectations.CreationObserved(key)
		c.recorder.Eventf(vmi, k8sv1.EventTypeWarning, controller.FailedCreatePodReason, "Error creating attachment pod: %v", err)
		return fmt.Errorf("failed to create attachment pod: %v", err)
	}
	c.recorder.Eventf(migration, k8sv1.EventTypeNormal, controller.SuccessfulCreatePodReason, "Created attachment pod %s", attachmentPod.Name)
	return nil
}

func isPodPendingUnschedulable(pod *k8sv1.Pod) bool {

	if pod.Status.Phase != k8sv1.PodPending || pod.DeletionTimestamp != nil {
		return false
	}

	for _, condition := range pod.Status.Conditions {
		if condition.Type == k8sv1.PodScheduled &&
			condition.Status == k8sv1.ConditionFalse &&
			condition.Reason == k8sv1.PodReasonUnschedulable {

			return true
		}
	}
	return false
}

func timeSinceCreationSeconds(objectMeta *metav1.ObjectMeta) int64 {

	now := time.Now().UTC().Unix()
	creationTime := objectMeta.CreationTimestamp.Time.UTC().Unix()
	seconds := now - creationTime
	if seconds < 0 {
		seconds = 0
	}

	return seconds
}

func (c *Controller) deleteTimedOutTargetPod(migration *virtv1.VirtualMachineInstanceMigration, vmi *virtv1.VirtualMachineInstance, pod *k8sv1.Pod, message string) error {

	migrationKey, err := controller.KeyFunc(migration)
	if err != nil {
		return err
	}

	c.podExpectations.ExpectDeletions(migrationKey, []string{controller.PodKey(pod)})
	err = c.clientset.CoreV1().Pods(vmi.Namespace).Delete(context.Background(), pod.Name, v1.DeleteOptions{})
	if err != nil {
		c.podExpectations.DeletionObserved(migrationKey, controller.PodKey(pod))
		c.recorder.Eventf(migration, k8sv1.EventTypeWarning, controller.FailedDeletePodReason, "Error deleted migration target pod: %v", err)
		return fmt.Errorf("failed to delete vmi migration target pod that reached pending pod timeout period.: %v", err)
	}
	log.Log.Object(vmi).Infof("Deleted pending migration target pod with uuid %s for migration %s with uuid %s with reason [%s]", string(pod.UID), migration.Name, string(migration.UID), message)
	c.recorder.Event(migration, k8sv1.EventTypeNormal, controller.SuccessfulDeletePodReason, message)
	return nil
}

func (c *Controller) getUnschedulablePendingTimeoutSeconds(migration *virtv1.VirtualMachineInstanceMigration) int64 {
	timeout := c.unschedulablePendingTimeoutSeconds
	customTimeoutStr, ok := migration.Annotations[virtv1.MigrationUnschedulablePodTimeoutSecondsAnnotation]
	if !ok {
		return timeout
	}

	newTimeout, err := strconv.Atoi(customTimeoutStr)
	if err != nil {
		log.Log.Object(migration).Reason(err).Errorf("Unable to parse unschedulable pending timeout value for migration")
		return timeout
	}

	return int64(newTimeout)
}

func (c *Controller) getCatchAllPendingTimeoutSeconds(migration *virtv1.VirtualMachineInstanceMigration) int64 {
	timeout := c.catchAllPendingTimeoutSeconds
	customTimeoutStr, ok := migration.Annotations[virtv1.MigrationPendingPodTimeoutSecondsAnnotation]
	if !ok {
		return timeout
	}

	newTimeout, err := strconv.Atoi(customTimeoutStr)
	if err != nil {
		log.Log.Object(migration).Reason(err).Errorf("Unable to parse catch all pending timeout value for migration")
		return timeout
	}

	return int64(newTimeout)
}

func (c *Controller) handlePendingPodTimeout(migration *virtv1.VirtualMachineInstanceMigration, vmi *virtv1.VirtualMachineInstance, pod *k8sv1.Pod) error {

	if pod.Status.Phase != k8sv1.PodPending || pod.DeletionTimestamp != nil || pod.CreationTimestamp.IsZero() {
		// only check if timeout has occurred if pod is pending and not already marked for deletion
		return nil
	}

	migrationKey, err := controller.KeyFunc(migration)
	if err != nil {
		return err
	}

	unschedulableTimeout := c.getUnschedulablePendingTimeoutSeconds(migration)
	catchAllTimeout := c.getCatchAllPendingTimeoutSeconds(migration)
	secondsSpentPending := timeSinceCreationSeconds(&pod.ObjectMeta)

	if isPodPendingUnschedulable(pod) {
		c.alertIfHostModelIsUnschedulable(vmi, pod)
		c.recorder.Eventf(
			migration,
			k8sv1.EventTypeWarning,
			controller.MigrationTargetPodUnschedulable,
			"Migration target pod for VMI [%s/%s] is currently unschedulable.", vmi.Namespace, vmi.Name)
		log.Log.Object(migration).Warningf("Migration target pod for VMI [%s/%s] is currently unschedulable.", vmi.Namespace, vmi.Name)
		if secondsSpentPending >= unschedulableTimeout {
			return c.deleteTimedOutTargetPod(migration, vmi, pod, fmt.Sprintf("unschedulable pod %s/%s timeout period exceeded", pod.Namespace, pod.Name))
		} else {
			// Make sure we check this again after some time
			c.Queue.AddAfter(migrationKey, time.Second*time.Duration(unschedulableTimeout-secondsSpentPending))
		}
	}

	if secondsSpentPending >= catchAllTimeout {
		return c.deleteTimedOutTargetPod(migration, vmi, pod, fmt.Sprintf("pending pod %s/%s timeout period exceeded", pod.Namespace, pod.Name))
	} else {
		// Make sure we check this again after some time
		c.Queue.AddAfter(migrationKey, time.Second*time.Duration(catchAllTimeout-secondsSpentPending))
	}

	return nil
}

func (c *Controller) sync(key string, migration *virtv1.VirtualMachineInstanceMigration, vmi *virtv1.VirtualMachineInstance, pods []*k8sv1.Pod) error {

	var pod *k8sv1.Pod = nil
	targetPodExists := len(pods) > 0
	if targetPodExists {
		pod = pods[0]
	}

	if vmiDeleted := vmi == nil || vmi.DeletionTimestamp != nil; vmiDeleted {
		return nil
	}

	if migrationFinalizedOnVMI := vmi.Status.MigrationState != nil && (vmi.Status.MigrationState.TargetMigrationUID == migration.UID || vmi.Status.MigrationState.SourceMigrationUID == migration.UID) &&
		vmi.Status.MigrationState.EndTimestamp != nil; migrationFinalizedOnVMI {
		return nil
	}

	if canMigrate, err := c.canMigrateVMI(migration, vmi); err != nil {
		return err
	} else if !canMigrate {
		return fmt.Errorf("vmi is inelgible for migration because another migration job is running")
	}

	switch migration.Status.Phase {
	case virtv1.MigrationPending, virtv1.MigrationPendingTargetVMI:
		log.Log.Infof("Migration status phase: %s", migration.Status.Phase)
		if migration.DeletionTimestamp != nil {
			return c.handlePreHandoffMigrationCancel(migration, vmi, pod)
		}
		if err := c.handleMigrationBackoff(key, vmi, migration); errors.Is(err, migrationBackoffError) {
			warningMsg := fmt.Sprintf("backoff migrating vmi %s/%s", vmi.Namespace, vmi.Name)
			c.recorder.Eventf(vmi, k8sv1.EventTypeWarning, err.Error(), warningMsg)
			return nil
		} else if err != nil {
			log.Log.Reason(err).Errorf("Failed to handle migration backoff")
		}
		if migration.IsTarget() {
			if !targetPodExists {
				log.Log.Info("target pod doesn't exist")
				var sourcePod *k8sv1.Pod
				var err error
				if migration.Status.Phase == virtv1.MigrationPendingTargetVMI {
					// Migration to other namespace/cluster, generate the source pod spec from
					// the VMI. Not actually creating the source pod, just need the manifest to
					// properly generate the target pod.
					sourcePod, err = c.templateService.RenderLaunchManifest(vmi)
					if err != nil {
						return fmt.Errorf("failed to render launch manifest: %v", err)
					}
				} else {
					sourcePod, err = controller.CurrentVMIPod(vmi, c.podIndexer)
					if err != nil {
						log.Log.Reason(err).Error("Failed to fetch pods for namespace from cache.")
						return err
					}
					if !controller.PodExists(sourcePod) {
						// for instance sudden deletes can cause this. In this
						// case we don't have to do anything in the creation flow anymore.
						// Once the VMI is in a final state or deleted the migration
						// will be marked as failed too.
						return nil
					}
				}
				if _, exists := migration.GetAnnotations()[virtv1.EvacuationMigrationAnnotation]; exists {
					if err = descheduler.MarkEvictionInProgress(c.clientset, sourcePod); err != nil {
						return err
					}
				}

				// patch VMI annotations and set RuntimeUser in preparation for target pod creation
				patches := c.setupVMIRuntimeUser(vmi)
				if !patches.IsEmpty() {
					patchBytes, err := patches.GeneratePayload()
					if err != nil {
						return err
					}
					vmi, err = c.clientset.VirtualMachineInstance(vmi.Namespace).Patch(context.Background(), vmi.Name, types.JSONPatchType, patchBytes, v1.PatchOptions{})
					if err != nil {
						return fmt.Errorf("failed to set VMI RuntimeUser: %v", err)
					}
				}
				log.Log.Infof("Creating target pod, vmi %s/%s, status %+v", vmi.Namespace, vmi.Name, vmi.Status)
				return c.handleTargetPodCreation(key, migration, vmi, sourcePod)
			} else if controller.IsPodReady(pod) {
				if controller.VMIHasHotplugVolumes(vmi) {
					attachmentPods, err := controller.AttachmentPods(pod, c.podIndexer)
					if err != nil {
						return fmt.Errorf(failedGetAttractionPodsFmt, err)
					}
					if len(attachmentPods) == 0 {
						log.Log.Object(migration).Infof("Creating attachment pod for vmi %s/%s on node %s", vmi.Namespace, vmi.Name, pod.Spec.NodeName)
						return c.createAttachmentPod(migration, vmi, pod)
					}
				}
			} else {
				return c.handlePendingPodTimeout(migration, vmi, pod)
			}
		} else {
			c.markVMISourceMigration(migration, vmi)
		}
	case virtv1.MigrationScheduling:
		if migration.DeletionTimestamp != nil {
			return c.handlePreHandoffMigrationCancel(migration, vmi, pod)
		}

		if targetPodExists {
			return c.handlePendingPodTimeout(migration, vmi, pod)
		}

	case virtv1.MigrationScheduled:
		if migration.DeletionTimestamp != nil && !c.isMigrationHandedOff(migration, vmi) {
			return c.handlePreHandoffMigrationCancel(migration, vmi, pod)
		}

		if migration.IsSource() && !migration.IsTarget() && !vmi.IsMigrationTarget() {
			if err := c.patchVMIMigratedVolumes(vmi); err != nil {
				return err
			}
			log.Log.Object(migration).Info("Setting source node")
			return c.setSourceNodeAndVMIUID(migration, vmi)
		} else if targetPodExists && controller.IsPodReady(pod) && migration.IsTarget() {
			log.Log.Object(migration).Infof("Calling handleTargetPodHandoff for migration. isSource: %t, isTarget: %t", migration.IsSource(), migration.IsTarget())
			// once target pod is running, then alert the VMI of the migration by
			// setting the target and source nodes. This kicks off the preparation stage.
			return c.handleTargetPodHandoff(migration, vmi, pod)
		}
	case virtv1.MigrationPreparingTarget:
		if migration.IsTarget() && vmi.Status.MigrationState.MigrationNetworkType == virtv1.Pod {
			// We are migrating on the pod network, create the services for the migration
			if err := c.createMigrationService(migration, vmi); err != nil {
				return err
			}
			log.Log.Info("Seeing if we can change the remote address")
			if vmi.Status.MigrationState != nil && vmi.Status.MigrationState.MigrationNetworkType == virtv1.Pod {
				log.Log.Info("Setting remote node address")
				if err := c.updateVMITargetRemoteNodeAddress(migration, vmi); err != nil {
					return err
				}
			}
		}
	case virtv1.MigrationTargetReady, virtv1.MigrationFailed:
		if migration.IsTarget() &&
			(!targetPodExists || controller.PodIsDown(pod)) &&
			vmi.Status.MigrationState != nil &&
			len(vmi.Status.MigrationState.TargetDirectMigrationNodePorts) == 0 &&
			vmi.Status.MigrationState.StartTimestamp == nil &&
			!vmi.Status.MigrationState.Failed &&
			!vmi.Status.MigrationState.Completed {

			log.Log.Object(migration).Infof("Migration failed, target pod is down")
			if err := c.handleMarkMigrationFailedOnVMI(migration, vmi); err != nil {
				return err
			}
		}

		if migration.Status.Phase != virtv1.MigrationFailed {
			return nil
		}
		return descheduler.MarkSourcePodEvictionCompleted(c.clientset, migration, c.podIndexer)
	case virtv1.MigrationRunning:
		if migration.DeletionTimestamp != nil && vmi.Status.MigrationState != nil {
			if err := c.markMigrationAbortInVmiStatus(migration, vmi); err != nil {
				return err
			}
		}
	}

	return nil
}

func (c *Controller) patchVMIMigratedVolumes(vmi *virtv1.VirtualMachineInstance) error {
	vmiCopy := vmi.DeepCopy()
	vmiCopy.Status.MigratedVolumes = []virtv1.StorageMigratedVolumeInfo{}
	// Mark all DV/PVC volumes as migrateable in the VMI status.
	for _, volume := range vmiCopy.Spec.Volumes {
		if volume.PersistentVolumeClaim != nil {
			key := controller.NamespacedKey(vmi.Namespace, volume.PersistentVolumeClaim.ClaimName)
			obj, exists, err := c.pvcStore.GetByKey(key)
			if err != nil {
				return err
			}
			if exists {
				pvc := obj.(*k8sv1.PersistentVolumeClaim)
				vmiCopy.Status.MigratedVolumes = append(vmiCopy.Status.MigratedVolumes, virtv1.StorageMigratedVolumeInfo{
					VolumeName: volume.Name,
					SourcePVCInfo: &virtv1.PersistentVolumeClaimInfo{
						ClaimName:   volume.PersistentVolumeClaim.ClaimName,
						AccessModes: pvc.Spec.AccessModes,
						VolumeMode:  pvc.Spec.VolumeMode,
						Requests:    pvc.Spec.Resources.Requests,
						Capacity:    pvc.Status.Capacity,
					},
					DestinationPVCInfo: &virtv1.PersistentVolumeClaimInfo{
						ClaimName:   volume.PersistentVolumeClaim.ClaimName,
						AccessModes: pvc.Spec.AccessModes,
						VolumeMode:  pvc.Spec.VolumeMode,
						Requests:    pvc.Spec.Resources.Requests,
						Capacity:    pvc.Status.Capacity,
					},
				})
			}
		} else if volume.DataVolume != nil {
			key := controller.NamespacedKey(vmi.Namespace, volume.DataVolume.Name)
			obj, exists, err := c.pvcStore.GetByKey(key)
			if err != nil {
				return err
			}
			if exists {
				pvc := obj.(*k8sv1.PersistentVolumeClaim)
				vmiCopy.Status.MigratedVolumes = append(vmiCopy.Status.MigratedVolumes, virtv1.StorageMigratedVolumeInfo{
					VolumeName: volume.Name,
					SourcePVCInfo: &virtv1.PersistentVolumeClaimInfo{
						ClaimName:   volume.DataVolume.Name,
						AccessModes: pvc.Spec.AccessModes,
						VolumeMode:  pvc.Spec.VolumeMode,
						Requests:    pvc.Spec.Resources.Requests,
						Capacity:    pvc.Status.Capacity,
					},
					DestinationPVCInfo: &virtv1.PersistentVolumeClaimInfo{
						ClaimName:   volume.DataVolume.Name,
						AccessModes: pvc.Spec.AccessModes,
						VolumeMode:  pvc.Spec.VolumeMode,
						Requests:    pvc.Spec.Resources.Requests,
						Capacity:    pvc.Status.Capacity,
					},
				})
			}
		}
	}
	patch, err := patch.New(
		patch.WithTest("/status/migratedVolumes", vmi.Status.MigratedVolumes),
		patch.WithReplace("/status/migratedVolumes", vmiCopy.Status.MigratedVolumes),
	).GeneratePayload()
	if err != nil {
		return err
	}
	vmi, err = c.clientset.VirtualMachineInstance(vmi.Namespace).Patch(context.Background(), vmi.Name, types.JSONPatchType, patch, metav1.PatchOptions{})
	if err != nil {
		return err
	}
	return nil
}

func (c *Controller) getVirtHandlerForNode(node *k8sv1.Node) (*k8sv1.Pod, error) {
	virthandlerPods := c.podIndexer.List()
	for _, obj := range virthandlerPods {
		pod := obj.(*k8sv1.Pod)
		if pod.Labels["kubevirt.io"] == "virt-handler" && pod.Spec.NodeName == node.Name {
			return pod, nil
		}
	}
	return nil, fmt.Errorf("virt-handler pod not found for node %s", node.Name)
}

func (c *Controller) createMigrationService(migration *virtv1.VirtualMachineInstanceMigration, vmi *virtv1.VirtualMachineInstance) error {
	if vmi.Status.MigrationState == nil || len(vmi.Status.MigrationState.TargetDirectMigrationNodePorts) == 0 {
		return nil
	}
	targetNode, err := c.getMigrationTargetNodeForVMI(vmi)
	if err != nil {
		return err
	}
	virthandler, err := c.getVirtHandlerForNode(targetNode)
	if err != nil {
		return err
	}
	if err := c.addMigrationLabelToVirtHandler(migration, virthandler); err != nil {
		return err
	}
	service := c.newMigrationService(migration, vmi, virthandler.GetNamespace())

	if _, err := c.getMigrationService(migration, virthandler.GetNamespace()); err != nil && !k8serrors.IsNotFound(err) {
		return err
	} else if k8serrors.IsNotFound(err) {
		log.Log.With("migration name", migration.Name).Infof("Creating migration service %s/%s", service.Namespace, service.Name)
		_, err = c.clientset.CoreV1().Services(virthandler.GetNamespace()).Create(context.Background(), service, v1.CreateOptions{})
		if err != nil {
			log.Log.Object(migration).Reason(err).Errorf("Failed to create migration service")
			return err
		}
	} else if err == nil {
		log.Log.With("migration name", migration.Name).Infof("Updating migration service %s/%s", service.Namespace, service.Name)
		_, err = c.clientset.CoreV1().Services(virthandler.GetNamespace()).Update(context.Background(), service, v1.UpdateOptions{})
		if err != nil {
			log.Log.Object(migration).Reason(err).Errorf("Failed to update migration service")
			return err
		}
	}
	return nil
}

func (c *Controller) createSyncService(migration *virtv1.VirtualMachineInstanceMigration, vmi *virtv1.VirtualMachineInstance) error {
	if vmi.Status.MigrationState == nil || len(vmi.Status.MigrationState.TargetDirectMigrationNodePorts) == 0 {
		return nil
	}
	targetNode, err := c.getSyncNodeForVMI(vmi)
	if err != nil {
		return err
	}
	if targetNode == nil {
		return nil
	}
	virthandler, err := c.getVirtHandlerForNode(targetNode)
	if err != nil {
		return err
	}
	if err := c.addSyncLabelToVirtHandler(migration, virthandler); err != nil {
		return err
	}
	service := c.newSyncService(migration, vmi, virthandler.GetNamespace())

	if _, err := c.getSyncService(migration, virthandler.GetNamespace()); err != nil && !k8serrors.IsNotFound(err) {
		return err
	} else if k8serrors.IsNotFound(err) {
		log.Log.With("migration name", migration.Name).Infof("Creating sync service %s/%s", service.Namespace, service.Name)
		_, err = c.clientset.CoreV1().Services(virthandler.GetNamespace()).Create(context.Background(), service, v1.CreateOptions{})
		if err != nil {
			log.Log.Object(migration).Reason(err).Errorf("Failed to create sync service")
			return err
		}
	} else if err == nil {
		log.Log.With("migration name", migration.Name).Infof("Updating sync service %s/%s", service.Namespace, service.Name)
		_, err = c.clientset.CoreV1().Services(virthandler.GetNamespace()).Update(context.Background(), service, v1.UpdateOptions{})
		if err != nil {
			log.Log.Object(migration).Reason(err).Errorf("Failed to update sync service")
			return err
		}
	}
	return nil
}

func (c *Controller) getMigrationService(migration *virtv1.VirtualMachineInstanceMigration, namespace string) (*k8sv1.Service, error) {
	// TODO: use an informer/store for this
	return c.clientset.CoreV1().Services(namespace).Get(context.Background(), fmt.Sprintf("migration-%s-%s", migration.Spec.VMIName, migration.Namespace), v1.GetOptions{})
}

func (c *Controller) getSyncService(migration *virtv1.VirtualMachineInstanceMigration, namespace string) (*k8sv1.Service, error) {
	// TODO: use an informer/store for this
	return c.clientset.CoreV1().Services(namespace).Get(context.Background(), fmt.Sprintf("sync-%s-%s", migration.Spec.VMIName, migration.Namespace), v1.GetOptions{})
}

func (c *Controller) addMigrationLabelToVirtHandler(migration *virtv1.VirtualMachineInstanceMigration, virthandlerPod *k8sv1.Pod) error {
	podCopy := virthandlerPod.DeepCopy()
	if podCopy.Labels == nil {
		podCopy.Labels = make(map[string]string)
	}
	podCopy.Labels[getMigrationServiceLabel(migration)] = "migration"
	log.Log.With("migration name", migration.Name).Infof("Adding migration label to virt-handler %s for migration service for VMI %s, labels: %v", podCopy.Name, migration.Spec.VMIName, podCopy.Labels)
	_, err := c.clientset.CoreV1().Pods(podCopy.GetNamespace()).Update(context.Background(), podCopy, v1.UpdateOptions{})
	if err != nil {
		log.Log.Object(migration).Reason(err).Errorf("Failed to add migration label to virt handler")
		return err
	}
	return nil
}

func (c *Controller) addSyncLabelToVirtHandler(migration *virtv1.VirtualMachineInstanceMigration, virthandlerPod *k8sv1.Pod) error {
	podCopy := virthandlerPod.DeepCopy()
	if podCopy.Labels == nil {
		podCopy.Labels = make(map[string]string)
	}
	podCopy.Labels[getSyncServiceLabel(migration)] = "sync"
	log.Log.With("migration name", migration.Name).Infof("Adding sync label to virt-handler %s for migration service for VMI %s, labels: %v", podCopy.Name, migration.Spec.VMIName, podCopy.Labels)
	_, err := c.clientset.CoreV1().Pods(podCopy.GetNamespace()).Update(context.Background(), podCopy, v1.UpdateOptions{})
	if err != nil {
		log.Log.Object(migration).Reason(err).Errorf("Failed to add sync label to virt handler")
		return err
	}
	return nil
}

func (c *Controller) removeMigrationService(migration *virtv1.VirtualMachineInstanceMigration, vmi *virtv1.VirtualMachineInstance, namespace string) error {
	service, err := c.getMigrationService(migration, namespace)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			return nil
		}
		return err
	}
	log.Log.With("migration name", migration.Name).Infof("Deleting migration service %s", service.Name)
	err = c.clientset.CoreV1().Services(namespace).Delete(context.Background(), service.Name, v1.DeleteOptions{})
	if err != nil {
		log.Log.Object(migration).Reason(err).Errorf("Failed to delete migration service")
		return err
	}
	return nil
}

func (c *Controller) removeSyncService(migration *virtv1.VirtualMachineInstanceMigration, vmi *virtv1.VirtualMachineInstance, namespace string) error {
	service, err := c.getSyncService(migration, namespace)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			return nil
		}
		return err
	}
	log.Log.With("migration name", migration.Name).Infof("Deleting sync service %s", service.Name)
	err = c.clientset.CoreV1().Services(namespace).Delete(context.Background(), service.Name, v1.DeleteOptions{})
	if err != nil {
		log.Log.Object(migration).Reason(err).Errorf("Failed to delete sync service")
		return err
	}
	return nil
}

func (c *Controller) removeMigrationLabelFromVirtHandler(migration *virtv1.VirtualMachineInstanceMigration, virthandlerPod *k8sv1.Pod) error {
	podCopy := virthandlerPod.DeepCopy()
	if podCopy.Labels == nil {
		return nil
	}
	delete(podCopy.Labels, getMigrationServiceLabel(migration))
	log.Log.With("migration name", migration.Name).Infof("Removing migration label from virt-handler %s for migration service for VMI %s, labels: %v", podCopy.Name, migration.Spec.VMIName, podCopy.Labels)
	_, err := c.clientset.CoreV1().Pods(podCopy.GetNamespace()).Update(context.Background(), podCopy, v1.UpdateOptions{})
	if err != nil {
		log.Log.Object(migration).Reason(err).Errorf("Failed to remove migration label from virt handler")
		return err
	}
	return nil
}

func (c *Controller) removeSyncLabelFromVirtHandler(migration *virtv1.VirtualMachineInstanceMigration, virthandlerPod *k8sv1.Pod) error {
	podCopy := virthandlerPod.DeepCopy()
	if podCopy.Labels == nil {
		return nil
	}
	delete(podCopy.Labels, getSyncServiceLabel(migration))
	log.Log.With("migration name", migration.Name).Infof("Removing sync label from virt-handler %s for migration service for VMI %s, labels: %v", podCopy.Name, migration.Spec.VMIName, podCopy.Labels)
	_, err := c.clientset.CoreV1().Pods(podCopy.GetNamespace()).Update(context.Background(), podCopy, v1.UpdateOptions{})
	if err != nil {
		log.Log.Object(migration).Reason(err).Errorf("Failed to remove sync label from virt handler")
		return err
	}
	return nil
}

func getMigrationServiceLabel(migration *virtv1.VirtualMachineInstanceMigration) string {
	return fmt.Sprintf("%s-%s-%s", migrationServiceLabelPrefix, migration.Spec.VMIName, migration.Namespace)
}

func getSyncServiceLabel(migration *virtv1.VirtualMachineInstanceMigration) string {
	return fmt.Sprintf("%s-%s-%s", syncServiceLabelPrefix, migration.Spec.VMIName, migration.Namespace)
}

func getMigrationPortNameFromType(portType int) string {
	switch portType {
	case migrationproxy.LibvirtDirectMigrationPort:
		return "direct-migration"
	case migrationproxy.LibvirtBlockMigrationPort:
		return "block-migration"
	case migrationproxy.VMISyncPort:
		return "sync"
	case 0:
		return "proxy"
	}
	return "unknown"
}

func (c *Controller) newSyncService(migration *virtv1.VirtualMachineInstanceMigration, vmi *virtv1.VirtualMachineInstance, namespace string) *k8sv1.Service {
	service := &k8sv1.Service{
		ObjectMeta: metav1.ObjectMeta{
			// TODO ensure this is not longer than 63 characters
			Name:      fmt.Sprintf("sync-%s-%s", migration.Spec.VMIName, migration.Namespace),
			Namespace: namespace,
			Labels: map[string]string{
				"kubevirt.io": "migration",
			},
		},
		Spec: k8sv1.ServiceSpec{
			Type: k8sv1.ServiceTypeClusterIP,
			Selector: map[string]string{
				getSyncServiceLabel(migration): "sync",
			},
			Ports: []k8sv1.ServicePort{},
		},
	}
	for port, portType := range vmi.Status.MigrationState.TargetDirectMigrationNodePorts {
		if portType != migrationproxy.VMISyncPort {
			continue
		}
		portInt, err := strconv.Atoi(port)
		if err != nil {
			return nil
		}
		service.Spec.Ports = append(service.Spec.Ports, k8sv1.ServicePort{
			Name:       getMigrationPortNameFromType(portType),
			TargetPort: intstr.FromInt32(int32(portInt)),
			Protocol:   k8sv1.ProtocolTCP,
			Port:       int32(portInt),
		})
	}

	return service
}

func (c *Controller) newMigrationService(migration *virtv1.VirtualMachineInstanceMigration, vmi *virtv1.VirtualMachineInstance, namespace string) *k8sv1.Service {
	service := &k8sv1.Service{
		ObjectMeta: metav1.ObjectMeta{
			// TODO ensure this is not longer than 63 characters
			Name:      fmt.Sprintf("migration-%s-%s", migration.Spec.VMIName, migration.Namespace),
			Namespace: namespace,
			Labels: map[string]string{
				"kubevirt.io": "migration",
			},
		},
		Spec: k8sv1.ServiceSpec{
			Type: k8sv1.ServiceTypeClusterIP,
			Selector: map[string]string{
				getMigrationServiceLabel(migration): "migration",
			},
			Ports: []k8sv1.ServicePort{},
		},
	}
	for port, portType := range vmi.Status.MigrationState.TargetDirectMigrationNodePorts {
		if portType == migrationproxy.VMISyncPort {
			continue
		}
		portInt, err := strconv.Atoi(port)
		if err != nil {
			return nil
		}
		service.Spec.Ports = append(service.Spec.Ports, k8sv1.ServicePort{
			Name:       getMigrationPortNameFromType(portType),
			TargetPort: intstr.FromInt32(int32(portInt)),
			Protocol:   k8sv1.ProtocolTCP,
			Port:       int32(portInt),
		})
	}

	return service
}

func (c *Controller) setupVMIRuntimeUser(vmi *virtv1.VirtualMachineInstance) *patch.PatchSet {
	patchSet := patch.New()
	if !c.clusterConfig.RootEnabled() {
		// The cluster is configured for non-root VMs, ensure the VMI is non-root.
		// If the VMI is root, the migration will be a root -> non-root migration.
		if vmi.Status.RuntimeUser != util.NonRootUID {
			patchSet.AddOption(patch.WithReplace("/status/runtimeUser", util.NonRootUID))
		}

		// This is required in order to be able to update from v0.43-v0.51 to v0.52+
		if vmi.Annotations == nil {
			patchSet.AddOption(patch.WithAdd("/metadata/annotations", map[string]string{virtv1.DeprecatedNonRootVMIAnnotation: "true"}))
		} else if _, ok := vmi.Annotations[virtv1.DeprecatedNonRootVMIAnnotation]; !ok {
			patchSet.AddOption(patch.WithAdd(fmt.Sprintf("/metadata/annotations/%s", patch.EscapeJSONPointer(virtv1.DeprecatedNonRootVMIAnnotation)), "true"))
		}
	} else {
		// The cluster is configured for root VMs, ensure the VMI is root.
		// If the VMI is non-root, the migration will be a non-root -> root migration.
		if vmi.Status.RuntimeUser != util.RootUser {
			patchSet.AddOption(patch.WithReplace("/status/runtimeUser", util.RootUser))
		}

		if _, ok := vmi.Annotations[virtv1.DeprecatedNonRootVMIAnnotation]; ok {
			patchSet.AddOption(patch.WithRemove(fmt.Sprintf("/metadata/annotations/%s", patch.EscapeJSONPointer(virtv1.DeprecatedNonRootVMIAnnotation))))
		}
	}
	return patchSet
}

func (c *Controller) listMatchingTargetPods(migration *virtv1.VirtualMachineInstanceMigration, vmi *virtv1.VirtualMachineInstance) ([]*k8sv1.Pod, error) {

	selector, err := v1.LabelSelectorAsSelector(&v1.LabelSelector{
		MatchLabels: map[string]string{
			virtv1.CreatedByLabel:    string(vmi.UID),
			virtv1.AppLabel:          "virt-launcher",
			virtv1.MigrationJobLabel: string(migration.UID),
		},
	})
	if err != nil {
		return nil, err
	}

	objs, err := c.podIndexer.ByIndex(cache.NamespaceIndex, migration.Namespace)
	if err != nil {
		return nil, err
	}

	var pods []*k8sv1.Pod
	for _, obj := range objs {
		pod := obj.(*k8sv1.Pod)
		if selector.Matches(labels.Set(pod.ObjectMeta.Labels)) {
			pods = append(pods, pod)
		}
	}

	return pods, nil
}

func (c *Controller) addMigration(obj interface{}) {
	c.enqueueMigration(obj)
}

func (c *Controller) deleteMigration(obj interface{}) {
	c.enqueueMigration(obj)
}

func (c *Controller) updateMigration(_, curr interface{}) {
	c.enqueueMigration(curr)
}

func (c *Controller) enqueueMigration(obj interface{}) {
	logger := log.Log
	migration := obj.(*virtv1.VirtualMachineInstanceMigration)
	key, err := controller.KeyFunc(migration)
	if err != nil {
		logger.Object(migration).Reason(err).Error("Failed to extract key from migration.")
		return
	}
	c.Queue.Add(key)
}

func (c *Controller) getControllerOf(pod *k8sv1.Pod) *v1.OwnerReference {
	t := true
	return &v1.OwnerReference{
		Kind:               virtv1.VirtualMachineInstanceMigrationGroupVersionKind.Kind,
		Name:               pod.Annotations[virtv1.MigrationJobNameAnnotation],
		UID:                types.UID(pod.Labels[virtv1.MigrationJobLabel]),
		Controller:         &t,
		BlockOwnerDeletion: &t,
	}
}

// resolveControllerRef returns the controller referenced by a ControllerRef,
// or nil if the ControllerRef could not be resolved to a matching controller
// of the correct Kind.
func (c *Controller) resolveControllerRef(namespace string, controllerRef *v1.OwnerReference) *virtv1.VirtualMachineInstanceMigration {
	// We can't look up by UID, so look up by Name and then verify UID.
	// Don't even try to look up by Name if it's the wrong Kind.
	if controllerRef.Kind != virtv1.VirtualMachineInstanceMigrationGroupVersionKind.Kind {
		return nil
	}
	migration, exists, err := c.migrationIndexer.GetByKey(controller.NamespacedKey(namespace, controllerRef.Name))
	if err != nil {
		return nil
	}
	if !exists {
		return nil
	}

	if migration.(*virtv1.VirtualMachineInstanceMigration).UID != controllerRef.UID {
		// The controller we found with this Name is not the same one that the
		// ControllerRef points to.
		return nil
	}
	return migration.(*virtv1.VirtualMachineInstanceMigration)
}

// When a pod is created, enqueue the migration that manages it and update its podExpectations.
func (c *Controller) addPod(obj interface{}) {
	pod := obj.(*k8sv1.Pod)

	if pod.DeletionTimestamp != nil {
		// on a restart of the controller manager, it's possible a new pod shows up in a state that
		// is already pending deletion. Prevent the pod from being a creation observation.
		c.deletePod(pod)
		return
	}

	controllerRef := c.getControllerOf(pod)
	migration := c.resolveControllerRef(pod.Namespace, controllerRef)
	if migration == nil {
		return
	}
	migrationKey, err := controller.KeyFunc(migration)
	if err != nil {
		return
	}
	log.Log.V(4).Object(pod).Infof("Pod created")
	c.podExpectations.CreationObserved(migrationKey)
	c.enqueueMigration(migration)
}

// When a pod is updated, figure out what migration manages it and wake them
// up. If the labels of the pod have changed we need to awaken both the old
// and new migration. old and cur must be *v1.Pod types.
func (c *Controller) updatePod(old, cur interface{}) {
	curPod := cur.(*k8sv1.Pod)
	oldPod := old.(*k8sv1.Pod)
	if curPod.ResourceVersion == oldPod.ResourceVersion {
		// Periodic resync will send update events for all known pods.
		// Two different versions of the same pod will always have different RVs.
		return
	}

	labelChanged := !equality.Semantic.DeepEqual(curPod.Labels, oldPod.Labels)
	if curPod.DeletionTimestamp != nil {
		// having a pod marked for deletion is enough to count as a deletion expectation
		c.deletePod(curPod)
		if labelChanged {
			// we don't need to check the oldPod.DeletionTimestamp because DeletionTimestamp cannot be unset.
			c.deletePod(oldPod)
		}
		return
	}

	curControllerRef := c.getControllerOf(curPod)
	oldControllerRef := c.getControllerOf(oldPod)
	controllerRefChanged := !equality.Semantic.DeepEqual(curControllerRef, oldControllerRef)
	if controllerRefChanged && oldControllerRef != nil {
		// The ControllerRef was changed. Sync the old controller, if any.
		if migration := c.resolveControllerRef(oldPod.Namespace, oldControllerRef); migration != nil {
			c.enqueueMigration(migration)
		}
	}

	migration := c.resolveControllerRef(curPod.Namespace, curControllerRef)
	if migration == nil {
		return
	}
	log.Log.V(4).Object(curPod).Infof("Pod updated")
	c.enqueueMigration(migration)
	return
}

// When a resourceQuota is updated, figure out if there are pending migration in the namespace
// if there are we should push them into the queue to accelerate the target creation process
func (c *Controller) updateResourceQuota(_, cur interface{}) {
	curResourceQuota := cur.(*k8sv1.ResourceQuota)
	log.Log.V(4).Object(curResourceQuota).Infof("ResourceQuota updated")
	objs, _ := c.migrationIndexer.ByIndex(cache.NamespaceIndex, curResourceQuota.Namespace)
	for _, obj := range objs {
		migration := obj.(*virtv1.VirtualMachineInstanceMigration)
		if migration.Status.Conditions == nil {
			continue
		}
		for _, cond := range migration.Status.Conditions {
			if cond.Type == virtv1.VirtualMachineInstanceMigrationRejectedByResourceQuota {
				c.enqueueMigration(migration)
			}
		}
	}
	return
}

// When a resourceQuota is deleted, figure out if there are pending migration in the namespace
// if there are we should push them into the queue to accelerate the target creation process
func (c *Controller) deleteResourceQuota(obj interface{}) {
	resourceQuota := obj.(*k8sv1.ResourceQuota)
	log.Log.V(4).Object(resourceQuota).Infof("ResourceQuota deleted")
	objs, _ := c.migrationIndexer.ByIndex(cache.NamespaceIndex, resourceQuota.Namespace)
	for _, obj := range objs {
		migration := obj.(*virtv1.VirtualMachineInstanceMigration)
		if migration.Status.Conditions == nil {
			continue
		}
		for _, cond := range migration.Status.Conditions {
			if cond.Type == virtv1.VirtualMachineInstanceMigrationRejectedByResourceQuota {
				c.enqueueMigration(migration)
			}
		}
	}
	return
}

// When a pod is deleted, enqueue the migration that manages the pod and update its podExpectations.
// obj could be an *v1.Pod, or a DeletionFinalStateUnknown marker item.
func (c *Controller) deletePod(obj interface{}) {
	pod, ok := obj.(*k8sv1.Pod)

	// When a delete is dropped, the relist will notice a pod in the store not
	// in the list, leading to the insertion of a tombstone object which contains
	// the deleted key/value. Note that this value might be stale. If the pod
	// changed labels the new migration will not be woken up till the periodic resync.
	if !ok {
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			log.Log.Reason(fmt.Errorf("couldn't get object from tombstone %+v", obj)).Error(failedToProcessDeleteNotificationErrMsg)
			return
		}
		pod, ok = tombstone.Obj.(*k8sv1.Pod)
		if !ok {
			log.Log.Reason(fmt.Errorf("tombstone contained object that is not a pod %#v", obj)).Error(failedToProcessDeleteNotificationErrMsg)
			return
		}
	}

	controllerRef := c.getControllerOf(pod)
	migration := c.resolveControllerRef(pod.Namespace, controllerRef)
	if migration == nil {
		return
	}
	migrationKey, err := controller.KeyFunc(migration)
	if err != nil {
		return
	}
	c.podExpectations.DeletionObserved(migrationKey, controller.PodKey(pod))
	c.enqueueMigration(migration)
}

func (c *Controller) updatePDB(old, cur interface{}) {
	curPDB := cur.(*policyv1.PodDisruptionBudget)
	oldPDB := old.(*policyv1.PodDisruptionBudget)
	if curPDB.ResourceVersion == oldPDB.ResourceVersion {
		return
	}

	// Only process PDBs manipulated by this controller
	migrationName := curPDB.Labels[virtv1.MigrationNameLabel]
	if migrationName == "" {
		return
	}

	objs, err := c.migrationIndexer.ByIndex(cache.NamespaceIndex, curPDB.Namespace)
	if err != nil {
		return
	}

	for _, obj := range objs {
		vmim := obj.(*virtv1.VirtualMachineInstanceMigration)

		if vmim.Name == migrationName {
			log.Log.V(4).Object(curPDB).Infof("PDB updated")
			c.enqueueMigration(vmim)
		}
	}
}

func (c *Controller) addPVC(obj interface{}) {
	pvc := obj.(*k8sv1.PersistentVolumeClaim)
	if pvc.DeletionTimestamp != nil {
		return
	}

	if !strings.HasPrefix(pvc.Name, backendstorage.PVCPrefix) {
		return
	}
	migrationName, exists := pvc.Labels[virtv1.MigrationNameLabel]
	if !exists {
		return
	}
	migrationKey := controller.NamespacedKey(pvc.Namespace, migrationName)
	c.pvcExpectations.CreationObserved(migrationKey)
	c.Queue.Add(migrationKey)
}

type vmimCollection []*virtv1.VirtualMachineInstanceMigration

func (c vmimCollection) Len() int {
	return len(c)
}

func (c vmimCollection) Less(i, j int) bool {
	t1 := &c[i].CreationTimestamp
	t2 := &c[j].CreationTimestamp
	return t1.Before(t2)
}

func (c vmimCollection) Swap(i, j int) {
	c[i], c[j] = c[j], c[i]
}

func (c *Controller) garbageCollectFinalizedMigrations(vmi *virtv1.VirtualMachineInstance) error {

	log.Log.Object(vmi).Infof("Garbage collecting completed migration objects")
	var finalizedMigrations []string

	migrations, err := c.listMigrationsMatchingVMI(vmi.Namespace, vmi.Name)
	if err != nil {
		return err
	}
	log.Log.Object(vmi).Infof("Found %d migration objects", len(migrations))

	// Oldest first
	sort.Sort(vmimCollection(migrations))
	for _, migration := range migrations {
		if migration.IsFinal() && migration.DeletionTimestamp == nil {
			finalizedMigrations = append(finalizedMigrations, migration.Name)
		}
	}

	// only keep the oldest 5 finalized migration objects
	garbageCollectionCount := len(finalizedMigrations) - defaultFinalizedMigrationGarbageCollectionBuffer

	if garbageCollectionCount <= 0 {
		return nil
	}

	log.Log.Object(vmi).Infof("Garbage collecting %d migration objects", garbageCollectionCount)
	for i := 0; i < garbageCollectionCount; i++ {
		log.Log.Object(vmi).Infof("Garbage collecting migration object %s", finalizedMigrations[i])
		err = c.clientset.VirtualMachineInstanceMigration(vmi.Namespace).Delete(context.Background(), finalizedMigrations[i], v1.DeleteOptions{})
		if err != nil && k8serrors.IsNotFound(err) {
			// This is safe to ignore. It's possible in some
			// scenarios that the migration we're trying to garbage
			// collect has already disappeared. Let's log it as debug
			// and suppress the error in this situation.
			log.Log.Reason(err).Infof("error encountered when garbage collecting migration object %s/%s", vmi.Namespace, finalizedMigrations[i])
		} else if err != nil {
			return err
		}
	}

	return nil
}

func (c *Controller) filterMigrations(namespace string, filter func(*virtv1.VirtualMachineInstanceMigration) bool) ([]*virtv1.VirtualMachineInstanceMigration, error) {
	objs, err := c.migrationIndexer.ByIndex(cache.NamespaceIndex, namespace)
	if err != nil {
		return nil, err
	}

	var migrations []*virtv1.VirtualMachineInstanceMigration
	for _, obj := range objs {
		migration := obj.(*virtv1.VirtualMachineInstanceMigration)

		if filter(migration) {
			migrations = append(migrations, migration)
		}
	}
	return migrations, nil
}

// takes a namespace and returns all migrations listening for this vmi
func (c *Controller) listMigrationsMatchingVMI(namespace, name string) ([]*virtv1.VirtualMachineInstanceMigration, error) {
	return c.filterMigrations(namespace, func(migration *virtv1.VirtualMachineInstanceMigration) bool {
		return migration.Spec.VMIName == name
	})
}

func (c *Controller) listBackoffEligibleMigrations(namespace string, name string) ([]*virtv1.VirtualMachineInstanceMigration, error) {
	return c.filterMigrations(namespace, func(migration *virtv1.VirtualMachineInstanceMigration) bool {
		return evacuationMigrationsFilter(migration, name) || workloadUpdaterMigrationsFilter(migration, name)
	})
}

func evacuationMigrationsFilter(migration *virtv1.VirtualMachineInstanceMigration, name string) bool {
	_, isEvacuation := migration.Annotations[virtv1.EvacuationMigrationAnnotation]
	return migration.Spec.VMIName == name && isEvacuation
}

func workloadUpdaterMigrationsFilter(migration *virtv1.VirtualMachineInstanceMigration, name string) bool {
	_, isWorkloadUpdater := migration.Annotations[virtv1.WorkloadUpdateMigrationAnnotation]
	return migration.Spec.VMIName == name && isWorkloadUpdater
}

func (c *Controller) addVMI(obj interface{}) {
	vmi := obj.(*virtv1.VirtualMachineInstance)
	if vmi.DeletionTimestamp != nil {
		c.deleteVMI(vmi)
		return
	}

	migrations, err := c.listMigrationsMatchingVMI(vmi.Namespace, vmi.Name)
	if err != nil {
		return
	}
	for _, migration := range migrations {
		c.enqueueMigration(migration)
	}
}

func (c *Controller) updateVMI(old, cur interface{}) {
	curVMI := cur.(*virtv1.VirtualMachineInstance)
	oldVMI := old.(*virtv1.VirtualMachineInstance)
	if curVMI.ResourceVersion == oldVMI.ResourceVersion {
		// Periodic resync will send update events for all known VMIs.
		// Two different versions of the same vmi will always
		// have different RVs.
		return
	}
	labelChanged := !equality.Semantic.DeepEqual(curVMI.Labels, oldVMI.Labels)
	if curVMI.DeletionTimestamp != nil {
		// having a DataVOlume marked for deletion is enough
		// to count as a deletion expectation
		c.deleteVMI(curVMI)
		if labelChanged {
			// we don't need to check the oldVMI.DeletionTimestamp
			// because DeletionTimestamp cannot be unset.
			c.deleteVMI(oldVMI)
		}
		return
	}

	migrations, err := c.listMigrationsMatchingVMI(curVMI.Namespace, curVMI.Name)
	if err != nil {
		log.Log.Object(curVMI).Errorf("Error encountered during datavolume update: %v", err)
		return
	}
	for _, migration := range migrations {
		log.Log.V(4).Object(curVMI).Infof("vmi updated for migration %s", migration.Name)
		c.enqueueMigration(migration)
	}
}
func (c *Controller) deleteVMI(obj interface{}) {
	vmi, ok := obj.(*virtv1.VirtualMachineInstance)
	// When a delete is dropped, the relist will notice a vmi in the store not
	// in the list, leading to the insertion of a tombstone object which contains
	// the deleted key/value. Note that this value might be stale. If the vmi
	// changed labels the new vmi will not be woken up till the periodic resync.
	if !ok {
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			log.Log.Reason(fmt.Errorf("couldn't get object from tombstone %+v", obj)).Error(failedToProcessDeleteNotificationErrMsg)
			return
		}
		vmi, ok = tombstone.Obj.(*virtv1.VirtualMachineInstance)
		if !ok {
			log.Log.Reason(fmt.Errorf("tombstone contained object that is not a vmi %#v", obj)).Error(failedToProcessDeleteNotificationErrMsg)
			return
		}
	}
	migrations, err := c.listMigrationsMatchingVMI(vmi.Namespace, vmi.Name)
	if err != nil {
		return
	}
	for _, migration := range migrations {
		log.Log.V(4).Object(vmi).Infof("vmi deleted for migration %s", migration.Name)
		c.enqueueMigration(migration)
	}
}

func (c *Controller) outboundMigrationsOnNode(node string, runningMigrations []*virtv1.VirtualMachineInstanceMigration) (int, error) {
	sum := 0
	for _, migration := range runningMigrations {
		key := controller.NamespacedKey(migration.Namespace, migration.Spec.VMIName)
		if vmi, exists, _ := c.vmiStore.GetByKey(key); exists {
			if vmi.(*virtv1.VirtualMachineInstance).Status.NodeName == node {
				sum = sum + 1
			}
		}
	}
	return sum, nil
}

// findRunningMigrations calcules how many migrations are running or in flight to be triggered to running
// Migrations which are in running phase are added alongside with migrations which are still pending but
// where we already see a target pod.
func (c *Controller) findRunningMigrations() ([]*virtv1.VirtualMachineInstanceMigration, error) {

	// Don't start new migrations if we wait for migration object updates because of new target pods
	notFinishedMigrations := migrations.ListUnfinishedMigrations(c.migrationIndexer)
	var runningMigrations []*virtv1.VirtualMachineInstanceMigration
	for _, migration := range notFinishedMigrations {
		if migration.IsRunning() {
			runningMigrations = append(runningMigrations, migration)
			continue
		}
		key := controller.NamespacedKey(migration.Namespace, migration.Spec.VMIName)
		vmi, exists, err := c.vmiStore.GetByKey(key)
		if err != nil {
			return nil, err
		}
		if !exists {
			continue
		}
		pods, err := c.listMatchingTargetPods(migration, vmi.(*virtv1.VirtualMachineInstance))
		if err != nil {
			return nil, err
		}
		if len(pods) > 0 {
			runningMigrations = append(runningMigrations, migration)
		}
	}
	return runningMigrations, nil
}

func (c *Controller) getNodeForVMI(vmi *virtv1.VirtualMachineInstance) (*k8sv1.Node, error) {
	obj, exists, err := c.nodeStore.GetByKey(vmi.Status.NodeName)

	if err != nil {
		return nil, fmt.Errorf("cannot get nodes to migrate VMI with host-model CPU. error: %v", err)
	} else if !exists {
		return nil, fmt.Errorf("node \"%s\" associated with vmi \"%s\" does not exist", vmi.Status.NodeName, vmi.Name)
	}

	node := obj.(*k8sv1.Node)
	return node, nil
}

func (c *Controller) getSyncNodeForVMI(vmi *virtv1.VirtualMachineInstance) (*k8sv1.Node, error) {
	nodeName, ok := vmi.Annotations[virtv1.SyncTargetNodeName]
	if !ok {
		log.Log.Infof("cannot get sync target node for vmi %s/%s, sync target node name is not set", vmi.Namespace, vmi.Name)
		return nil, nil
	}
	obj, exists, err := c.nodeStore.GetByKey(nodeName)

	if err != nil {
		return nil, fmt.Errorf("cannot get node to sync VMI, error: %v", err)
	} else if !exists {
		return nil, fmt.Errorf("sync node \"%s\" associated with vmi \"%s\" does not exist", nodeName, vmi.Name)
	}

	node := obj.(*k8sv1.Node)
	return node, nil
}

func (c *Controller) getMigrationTargetNodeForVMI(vmi *virtv1.VirtualMachineInstance) (*k8sv1.Node, error) {
	if vmi.Status.MigrationState == nil {
		return nil, fmt.Errorf("cannot get migration target node for vmi %s, migration state is not set", vmi.Name)
	}
	obj, exists, err := c.nodeStore.GetByKey(vmi.Status.MigrationState.TargetNode)

	if err != nil {
		return nil, fmt.Errorf("cannot get nodes to migrate VMI with host-model CPU. error: %v", err)
	} else if !exists {
		return nil, fmt.Errorf("node \"%s\" associated with vmi \"%s\" does not exist", vmi.Status.MigrationState.TargetNode, vmi.Name)
	}

	node := obj.(*k8sv1.Node)
	return node, nil
}

func (c *Controller) alertIfHostModelIsUnschedulable(vmi *virtv1.VirtualMachineInstance, targetPod *k8sv1.Pod) {
	fittingNodeFound := false

	if cpu := vmi.Spec.Domain.CPU; cpu == nil || cpu.Model != virtv1.CPUModeHostModel {
		return
	}

	requiredNodeLabels := map[string]string{}
	for key, value := range targetPod.Spec.NodeSelector {
		if strings.HasPrefix(key, virtv1.SupportedHostModelMigrationCPU) || strings.HasPrefix(key, virtv1.CPUFeatureLabel) {
			requiredNodeLabels[key] = value
		}
	}

	nodes := c.nodeStore.List()
	for _, nodeInterface := range nodes {
		node := nodeInterface.(*k8sv1.Node)

		if node.Name == vmi.Status.NodeName {
			continue // avoid checking the VMI's source node
		}

		if isNodeSuitableForHostModelMigration(node, requiredNodeLabels) {
			log.Log.Object(vmi).Infof("Node %s is suitable to run vmi %s host model cpu mode (more nodes may fit as well)", node.Name, vmi.Name)
			fittingNodeFound = true
			break
		}
	}

	if !fittingNodeFound {
		warningMsg := fmt.Sprintf("Migration cannot proceed since no node is suitable to run the required CPU model / required features: %v", requiredNodeLabels)
		c.recorder.Eventf(vmi, k8sv1.EventTypeWarning, controller.NoSuitableNodesForHostModelMigration, warningMsg)
		log.Log.Object(vmi).Warning(warningMsg)
	}
}

func getNodeSelectorsFromVMIMigrationState(migrationState *virtv1.VirtualMachineInstanceMigrationState) (map[string]string, error) {
	log.Log.Info("Getting node selector labels from VMI")
	result, nodeSelectorKeyForHostModel, err := getHostCpuModelFromMap(migrationState.SourceNodeSelectors)
	if err != nil {
		return nil, err
	}
	log.Log.Infof("cpu model label selector (\"%s\") defined for migration target pod", nodeSelectorKeyForHostModel)

	return result, nil
}

func prepareNodeSelectorForHostCpuModel(node *k8sv1.Node, pod *k8sv1.Pod, sourcePod *k8sv1.Pod) (map[string]string, error) {
	result := make(map[string]string)

	// if the vmi already migrated before it should include node selector that consider CPUModelLabel
	for key, value := range sourcePod.Spec.NodeSelector {
		if strings.Contains(key, virtv1.CPUFeatureLabel) || strings.Contains(key, virtv1.SupportedHostModelMigrationCPU) {
			pod.Spec.NodeSelector[key] = value
			return pod.Spec.NodeSelector, nil
		}
	}

	result, nodeSelectorKeyForHostModel, err := getHostCpuModelFromMap(node.Labels)
	if err != nil {
		return nil, err
	}

	log.Log.Object(pod).Infof("cpu model label selector (\"%s\") defined for migration target pod", nodeSelectorKeyForHostModel)

	return result, nil
}

func getHostCpuModelFromMap(selectorMap map[string]string) (map[string]string, string, error) {
	result := make(map[string]string)
	var hostCpuModel, nodeSelectorKeyForHostModel, hostModelLabelValue string

	for key, value := range selectorMap {
		log.DefaultLogger().Infof("Checking if label \"%s\" contains %s", key, virtv1.HostModelCPULabel)
		if strings.HasPrefix(key, virtv1.HostModelCPULabel) {
			hostCpuModel = strings.TrimPrefix(key, virtv1.HostModelCPULabel)
			hostModelLabelValue = value
		}

		if strings.HasPrefix(key, virtv1.HostModelRequiredFeaturesLabel) {
			requiredFeature := strings.TrimPrefix(key, virtv1.HostModelRequiredFeaturesLabel)
			result[virtv1.CPUFeatureLabel+requiredFeature] = value
		}
	}

	if hostCpuModel == "" {
		return nil, "", fmt.Errorf("unable to locate host cpu model, does not contain label \"%s\" with information", virtv1.HostModelCPULabel)
	}

	nodeSelectorKeyForHostModel = virtv1.SupportedHostModelMigrationCPU + hostCpuModel
	result[nodeSelectorKeyForHostModel] = hostModelLabelValue

	return result, nodeSelectorKeyForHostModel, nil
}

func isNodeSuitableForHostModelMigration(node *k8sv1.Node, requiredNodeLabels map[string]string) bool {
	for key, value := range requiredNodeLabels {
		nodeValue, ok := node.Labels[key]

		if !ok || nodeValue != value {
			return false
		}
	}

	return true
}

func (c *Controller) matchMigrationPolicy(vmi *virtv1.VirtualMachineInstance, clusterMigrationConfiguration *virtv1.MigrationConfiguration) error {
	vmiNamespace, err := c.clientset.CoreV1().Namespaces().Get(context.Background(), vmi.Namespace, v1.GetOptions{})
	if err != nil {
		return err
	}

	// Fetch cluster policies
	var policies []v1alpha1.MigrationPolicy
	migrationInterfaceList := c.migrationPolicyStore.List()
	for _, obj := range migrationInterfaceList {
		policy := obj.(*v1alpha1.MigrationPolicy)
		policies = append(policies, *policy)
	}
	policiesListObj := v1alpha1.MigrationPolicyList{Items: policies}

	// Override cluster-wide migration configuration if migration policy is matched
	matchedPolicy := matchPolicy(&policiesListObj, vmi, vmiNamespace)

	if matchedPolicy == nil {
		log.Log.Object(vmi).Reason(err).Infof("no migration policy matched for VMI %s", vmi.Name)
		return nil
	}

	isUpdated, err := matchedPolicy.GetMigrationConfByPolicy(clusterMigrationConfiguration)
	if err != nil {
		return err
	}

	if isUpdated {
		vmi.Status.MigrationState.MigrationPolicyName = &matchedPolicy.Name
		vmi.Status.MigrationState.MigrationConfiguration = clusterMigrationConfiguration
		log.Log.Object(vmi).Infof("migration is updated by migration policy named %s.", matchedPolicy.Name)
	}

	return nil
}

func (c *Controller) isMigrationPolicyMatched(vmi *virtv1.VirtualMachineInstance) bool {
	if vmi == nil {
		return false
	}

	migrationPolicyName := vmi.Status.MigrationState.MigrationPolicyName
	return migrationPolicyName != nil && *migrationPolicyName != ""
}

func (c *Controller) isMigrationHandedOff(migration *virtv1.VirtualMachineInstanceMigration, vmi *virtv1.VirtualMachineInstance) bool {
	if vmi.Status.MigrationState != nil && vmi.Status.MigrationState.TargetMigrationUID == migration.UID {
		return true
	}

	migrationKey := controller.MigrationKey(migration)

	c.handOffLock.Lock()
	defer c.handOffLock.Unlock()

	_, isHandedOff := c.handOffMap[migrationKey]
	return isHandedOff
}

func (c *Controller) addHandOffKey(migrationKey string) {
	c.handOffLock.Lock()
	defer c.handOffLock.Unlock()

	c.handOffMap[migrationKey] = struct{}{}
}

func (c *Controller) removeHandOffKey(migrationKey string) {
	c.handOffLock.Lock()
	defer c.handOffLock.Unlock()

	delete(c.handOffMap, migrationKey)
}

func getComputeContainer(pod *k8sv1.Pod) *k8sv1.Container {
	for _, container := range pod.Spec.Containers {
		if container.Name == "compute" {
			return &container
		}
	}
	return nil
}

func getTargetPodLimitsCount(pod *k8sv1.Pod) (int64, error) {
	cc := getComputeContainer(pod)
	if cc == nil {
		return 0, fmt.Errorf("Could not find VMI compute container")
	}

	cpuLimit, ok := cc.Resources.Limits[k8sv1.ResourceCPU]
	if !ok {
		return 0, fmt.Errorf("Could not find dedicated CPU limit in VMI compute container")
	}
	return cpuLimit.Value(), nil
}

func getTargetPodMemoryRequests(pod *k8sv1.Pod) (string, error) {
	cc := getComputeContainer(pod)
	if cc == nil {
		return "", fmt.Errorf("Could not find VMI compute container")
	}

	memReq, ok := cc.Resources.Requests[k8sv1.ResourceMemory]
	if !ok {
		return "", fmt.Errorf("Could not find memory request in VMI compute container")
	}

	if hugePagesReq, ok := cc.Resources.Requests[k8sv1.ResourceHugePagesPrefix+"2Mi"]; ok {
		memReq.Add(hugePagesReq)
	}

	if hugePagesReq, ok := cc.Resources.Requests[k8sv1.ResourceHugePagesPrefix+"1Gi"]; ok {
		memReq.Add(hugePagesReq)
	}

	return memReq.String(), nil
}
