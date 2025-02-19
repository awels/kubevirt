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
 * Copyright 2025 KubeVirt developers.
 *
 */

package migrationproxy

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"io"
	"net"
	"strconv"
	"time"

	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"

	virtv1 "kubevirt.io/api/core/v1"

	"kubevirt.io/kubevirt/pkg/controller"
	"kubevirt.io/kubevirt/pkg/util/net/ip"

	"kubevirt.io/client-go/kubecli"
	"kubevirt.io/client-go/log"
)

type MigrationSyncProxy interface {
	StartTargetSync() error
	StartSourceSync(remoteURL string) error
	StopSync()
	GetBindPort() string
}

type migrationSyncProxy struct {
	client           kubecli.KubevirtClient
	stopChan         chan struct{}
	errChan          chan error
	connChan         chan io.ReadWriteCloser
	conn             io.ReadWriteCloser
	clientTLSConfig  *tls.Config
	serverTLSConfig  *tls.Config
	queue            workqueue.TypedRateLimitingInterface[string]
	localVMIStore    cache.Store
	localVMIInformer cache.SharedIndexInformer
	remoteVMIStore   cache.Store
	bindPort         int
	listener         net.Listener

	logger    *log.FilteredLogger
	hasSynced func() bool
}

func NewMigrationSyncProxy(vmiInformer cache.SharedIndexInformer, client kubecli.KubevirtClient, clientTLSConfig, serverTLSConfig *tls.Config) (MigrationSyncProxy, error) {
	proxy := &migrationSyncProxy{
		client:           client,
		stopChan:         make(chan struct{}),
		errChan:          make(chan error),
		connChan:         make(chan io.ReadWriteCloser),
		localVMIStore:    vmiInformer.GetStore(),
		localVMIInformer: vmiInformer,
		remoteVMIStore:   cache.NewStore(cache.DeletionHandlingMetaNamespaceKeyFunc),
		clientTLSConfig:  clientTLSConfig,
		serverTLSConfig:  serverTLSConfig,
		logger:           log.Log.With("component", "migration-sync-proxy"),
	}

	queue := workqueue.NewTypedRateLimitingQueueWithConfig[string](
		workqueue.DefaultTypedControllerRateLimiter[string](),
		workqueue.TypedRateLimitingQueueConfig[string]{Name: "sync-vm"},
	)
	proxy.queue = queue

	proxy.hasSynced = func() bool {
		return vmiInformer.HasSynced()
	}

	_, err := vmiInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: proxy.addFunc,
		//		DeleteFunc: handler.deleteFunc,
		UpdateFunc: proxy.updateFunc,
	})
	if err != nil {
		return nil, err
	}

	go proxy.Run(proxy.stopChan)

	return proxy, nil
}

func (m *migrationSyncProxy) addFunc(addObj interface{}) {
	vmi, ok := addObj.(*virtv1.VirtualMachineInstance)
	if ok {
		if err := m.SynchronizeVMI(vmi); err != nil {
			m.logger.Reason(err).Error("failed to synchronize VMI, after add")
		}
	} else {
		m.logger.Error("failed to cast addObj to VMI")
	}
}

// func (m *migrationSyncHandler) deleteFunc(delObj interface{}) {
// 	key, err := controller.KeyFunc(delObj)
// 	if err == nil {
// 		m.queue.Add(key)
// 	}
// }

func (m *migrationSyncProxy) updateFunc(_, new interface{}) {
	vmi, ok := new.(*virtv1.VirtualMachineInstance)
	if ok {
		m.logger.Infof("VMI %s was updated, sending to other side", vmi.Name)
		if err := m.SynchronizeVMI(vmi); err != nil {
			m.logger.Reason(err).Error("failed to synchronize VMI, after update")
		}
	} else {
		m.logger.Error("failed to cast addObj to VMI")
	}
}

func (m *migrationSyncProxy) createTcpListener() (net.Listener, error) {
	if m.listener != nil {
		return m.listener, nil
	}
	var ln net.Listener
	var err error
	addr := net.JoinHostPort(ip.GetIPZeroAddress(), "0")
	m.logger.Info("Creating TCP listener")
	if m.serverTLSConfig != nil {
		m.logger.Info("Created TLS listener")
		ln, err = tls.Listen("tcp", addr, m.serverTLSConfig)
	} else {
		m.logger.Info("Created standard tcp listener")
		ln, err = net.Listen("tcp", addr)
	}
	if err != nil {
		m.logger.Reason(err).Error("failed to create tcp listener for sync proxy")
		return nil, err
	}
	m.bindPort = ln.Addr().(*net.TCPAddr).Port
	m.listener = ln
	m.logger = m.logger.With("ip", ln.Addr().(*net.TCPAddr).IP, "port", m.bindPort)
	m.logger.Infof("Successfully created TCP listener")
	return ln, nil
}

func (m *migrationSyncProxy) GetBindPort() string {
	return strconv.Itoa(m.bindPort)
}

func (m *migrationSyncProxy) StartTargetSync() error {
	m.logger.Info("Starting migration sync proxy")
	go func(connChan chan io.ReadWriteCloser, errChan chan error, stopChan chan struct{}) {
		ln, err := m.createTcpListener()
		if err != nil {
			errChan <- err
		}
		for {
			m.logger.Info("Waiting for incoming sync connection")
			rwc, err := ln.Accept()
			if err != nil {
				errChan <- err

				select {
				case <-stopChan:
					// If the stopChan is closed, then this is expected. Log at a lesser debug level
					m.logger.Reason(err).V(3).Infof("stopChan is closed. Listener exited with expected error.")
				default:
					m.logger.Reason(err).Error("sync proxy listener returned error.")
				}
				break
			} else {
				m.logger.Info("Accepted incoming sync connection")
				connChan <- rwc
			}
		}
	}(m.connChan, m.errChan, m.stopChan)

	go func(m *migrationSyncProxy) {
		for {
			select {
			case rwc := <-m.connChan:
				go m.handleIncomingVMI(rwc)
			case <-m.stopChan:
				return
			}
		}

	}(m)

	go m.Run(m.stopChan)

	return nil
}

func (m *migrationSyncProxy) StartSourceSync(url string) error {
	m.logger = m.logger.With("outbound", url)
	m.logger.Info("dialing sync tcp outbound connection")
	var conn net.Conn
	var err error
	if m.clientTLSConfig != nil {
		conn, err = tls.Dial("tcp", url, m.clientTLSConfig)
	} else {
		conn, err = net.Dial("tcp", url)
	}
	if err != nil {
		m.logger.Reason(err).Error("failed to dial sync proxy")
		return err
	}
	m.logger.Infof("Setting sync connection in sync proxy: %s", conn.RemoteAddr().String())
	m.conn = conn

	go func() {
		m.handleIncomingVMI(conn)
	}()

	return nil
}

func (m *migrationSyncProxy) Run(stopCh chan struct{}) {
	defer m.queue.ShutDown()
	m.logger.Info("Starting vmi sync handler.")

	cache.WaitForCacheSync(stopCh, m.hasSynced)

	// Start the actual work
	go wait.Until(m.runWorker, time.Second, stopCh)

	<-stopCh
	log.Log.Info("Stopping migration sync controller.")
}

func (m *migrationSyncProxy) runWorker() {
	for m.Execute() {
	}
}

func (m *migrationSyncProxy) Execute() bool {
	key, quit := m.queue.Get()
	if quit {
		return false
	}
	defer m.queue.Done(key)
	if err := m.execute(key); err != nil {
		m.logger.Reason(err).Infof("re-enqueuing VirtualMachineInstance %v", key)
		m.queue.AddRateLimited(key)
	} else {
		m.logger.Infof("processed VirtualMachineInstance %v in sync controller", key)
		m.queue.Forget(key)
	}
	return true
}

func (m *migrationSyncProxy) execute(key string) error {
	localVMI, localVMIExists, err := m.getVMIFromLocalCache(key)
	if err != nil {
		return err
	}
	m.logger.Infof("localVMI keys: %v, %v", m.localVMIStore.ListKeys(), m.localVMIInformer.GetStore().ListKeys())
	remoteVMI, remoteVMIExists, err := m.getVMIFromRemoteCache(key)
	if err != nil {
		return err
	}
	// Only attemp to update the local VMI if it exists in both caches
	// TODO: Update labels if needed.
	m.logger.Infof("localVMIExists: %v, remoteVMIExists: %v", localVMIExists, remoteVMIExists)
	if localVMIExists && remoteVMIExists {
		localVMIStatus := localVMI.Status.DeepCopy()
		remoteVMIStatus := remoteVMI.Status.DeepCopy()
		if !equality.Semantic.DeepEqual(localVMIStatus.MigrationState, remoteVMIStatus.MigrationState) {
			localVMI.Status.MigrationState = remoteVMI.Status.MigrationState
			m.logger.Infof("Updating VMI %s with new migration state: %v", localVMI.Name, localVMI.Status.MigrationState)
			if _, err := m.client.VirtualMachineInstance(localVMI.Namespace).UpdateStatus(context.Background(), localVMI, metav1.UpdateOptions{}); err != nil {
				return err
			}
		} else {
			m.logger.Info("No update needed, migration states match")
		}
	}
	return nil
}
func (m *migrationSyncProxy) getVMIFromRemoteCache(key string) (vmi *virtv1.VirtualMachineInstance, exists bool, err error) {
	return getVMIFromCache(key, m.remoteVMIStore)
}

func (m *migrationSyncProxy) getVMIFromLocalCache(key string) (vmi *virtv1.VirtualMachineInstance, exists bool, err error) {
	return getVMIFromCache(key, m.localVMIStore)
}

func getVMIFromCache(key string, store cache.Store) (vmi *virtv1.VirtualMachineInstance, exists bool, err error) {
	// Fetch the latest Vm state from cache
	obj, exists, err := store.GetByKey(key)
	if err != nil {
		return nil, false, err
	}

	// Retrieve the VirtualMachineInstance
	if !exists {
		namespace, name, err := cache.SplitMetaNamespaceKey(key)
		if err != nil {
			return nil, false, err
		}
		vmi = virtv1.NewVMIReferenceFromNameWithNS(namespace, name)
	} else {
		vmi = obj.(*virtv1.VirtualMachineInstance)
	}
	return vmi, exists, nil
}

func (m *migrationSyncProxy) StopSync() {
	m.logger.Info("Stopping migration sync proxy")
	close(m.stopChan)
}

func (m *migrationSyncProxy) handleIncomingVMI(rwc io.ReadWriteCloser) {
	defer rwc.Close()
	decoder := json.NewDecoder(rwc)
	for {
		select {
		case <-m.stopChan:
			return
		default:
			vmi := &virtv1.VirtualMachineInstance{}
			err := decoder.Decode(vmi)
			if err != nil {
				// TODO: Check if we get a valid EOF, if so we don't have to log an error
				m.logger.Reason(err).Error("failed to decode incoming VMI")
				return
			}
			m.logger.Infof("Received VMI: %s", vmi.Name)

			currentVMI, exists, err := m.remoteVMIStore.GetByKey(controller.VirtualMachineInstanceKey(vmi))
			if err != nil {
				m.logger.Reason(err).Error("failed to get VMI from remote store")
				return
			}
			if !exists {
				if err := m.remoteVMIStore.Add(vmi); err != nil {
					m.logger.Reason(err).Error("failed to add VMI to remote store")
					return
				}
			} else {
				// Only update if the incoming VMI is newer than the current one
				if vmi.ResourceVersion > currentVMI.(*virtv1.VirtualMachineInstance).ResourceVersion {
					if err := m.remoteVMIStore.Update(vmi); err != nil {
						m.logger.Reason(err).Error("failed to update VMI in remote store")
						return
					}
				}
			}
			m.logger.Infof("Adding VMI to sync queue: %s", controller.VirtualMachineInstanceKey(vmi))
			// Find the VMI labels and status that needs to be updated locally.
			m.queue.Add(controller.VirtualMachineInstanceKey(vmi))
		}
	}
}

func (m *migrationSyncProxy) SynchronizeVMI(vmi *virtv1.VirtualMachineInstance) error {
	if m.conn != nil {
		m.logger.Info("marshalling VMI")
		json, err := json.Marshal(vmi)
		m.logger.Infof("Marshalled VMI: %s", string(json))
		if err != nil {
			return err
		}

		m.logger.V(5).Infof("Sending VMI: %s", vmi.Name)
		//Write data to socket.
		writtenCount, err := m.conn.Write(json)
		if err != nil {
			return err
		}
		if writtenCount != len(json) {
			return io.ErrShortWrite
		}
	} else {
		m.logger.Info("No connection to send VMI")
	}
	return nil
}
