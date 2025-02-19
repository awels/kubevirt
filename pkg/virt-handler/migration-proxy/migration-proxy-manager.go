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
 * Copyright 2025 KubeVirt Developers.
 *
 */
package migrationproxy

import (
	"crypto/tls"
	"fmt"
	"net"
	"os"
	"sync"

	"k8s.io/client-go/tools/cache"
	"kubevirt.io/client-go/kubecli"
	"kubevirt.io/client-go/log"

	"kubevirt.io/kubevirt/pkg/util/net/ip"
	virtconfig "kubevirt.io/kubevirt/pkg/virt-config"
)

type ProxyManager interface {
	StartTargetListener(vmiUID string, targetUnixFiles []string, client kubecli.KubevirtClient, vmiInformer cache.SharedIndexInformer) error
	GetTargetListenerPorts(vmiUID string) map[string]int
	GetSyncPort(vmiUID string) string
	StopTargetListener(vmiUID string)

	StartSourceSync(vmiUID, syncAddress string, client kubecli.KubevirtClient, vmiInformer cache.SharedIndexInformer) error
	StartSourceListener(vmiUID string, targetAddress string, destSrcPortMap map[string]int, baseDir string) error
	GetSourceListenerFiles(vmiUID string) []string
	StopSourceListener(vmiUID string)

	OpenListenerCount() int

	InitiateGracefulShutdown()
}

type migrationProxyManager struct {
	sourceProxies   map[string][]MigrationProxyListener
	targetProxies   map[string][]MigrationProxyListener
	sourceSyncProxy map[string]MigrationSyncProxy
	targetSyncProxy map[string]MigrationSyncProxy
	managerLock     sync.Mutex
	serverTLSConfig *tls.Config
	clientTLSConfig *tls.Config

	isShuttingDown bool
	config         *virtconfig.ClusterConfig
	logger         *log.FilteredLogger
}

func NewMigrationProxyManager(serverTLSConfig *tls.Config, clientTLSConfig *tls.Config, config *virtconfig.ClusterConfig) ProxyManager {
	return &migrationProxyManager{
		sourceProxies:   make(map[string][]MigrationProxyListener),
		targetProxies:   make(map[string][]MigrationProxyListener),
		sourceSyncProxy: make(map[string]MigrationSyncProxy),
		targetSyncProxy: make(map[string]MigrationSyncProxy),
		serverTLSConfig: serverTLSConfig,
		clientTLSConfig: clientTLSConfig,
		config:          config,
		logger:          log.Log.With("component", "migration-proxy-manager"),
	}
}

func (m *migrationProxyManager) InitiateGracefulShutdown() {
	m.managerLock.Lock()
	defer m.managerLock.Unlock()

	m.isShuttingDown = true
}

func (m *migrationProxyManager) OpenListenerCount() int {
	m.managerLock.Lock()
	defer m.managerLock.Unlock()

	return len(m.sourceProxies) + len(m.targetProxies)
}

func (m *migrationProxyManager) isTargetExistingProxy(curProxies []MigrationProxyListener, targetUnixFiles []string) bool {
	// make sure that all elements in the existing proxy match to the provided targetUnixFiles
	if len(curProxies) != len(targetUnixFiles) {
		return false
	}
	existingSocketFiles := make(map[string]bool)
	for _, file := range targetUnixFiles {
		existingSocketFiles[file] = true
	}
	m.logger.Infof("existing socket files: %#v", existingSocketFiles)
	for _, curProxy := range curProxies {
		m.logger.Infof("curProxy path: %s", curProxy.GetTargetAddress())
		if _, ok := existingSocketFiles[curProxy.GetTargetAddress()]; !ok {
			return false
		}
	}
	return true
}

func (m *migrationProxyManager) createTargetSocketProxies(vmiUID string, targetUnixFiles []string, clientTLSConfig, serverTLSConfig *tls.Config) ([]MigrationProxyListener, error) {
	proxiesList := []MigrationProxyListener{}
	zeroAddress := ip.GetIPZeroAddress()
	for _, targetUnixFile := range targetUnixFiles {
		log.DefaultLogger().Infof("Creating proxy for file: %s", targetUnixFile)
		// 0 means random port is used
		proxy := NewTargetSocketProxy(zeroAddress, 0, serverTLSConfig, clientTLSConfig, targetUnixFile, vmiUID)
		log.DefaultLogger().Infof("Created proxy: %#v", proxy)
		err := proxy.Start()
		if err != nil {
			proxy.Stop()
			// close all already created proxies for this vmiUID
			for _, curProxy := range proxiesList {
				curProxy.Stop()
			}
			return nil, err
		}
		proxiesList = append(proxiesList, proxy)
		m.logger.Infof("Manager created proxy on target")
	}
	return proxiesList, nil
}

func (m *migrationProxyManager) StartTargetListener(vmiUID string, targetUnixFiles []string, client kubecli.KubevirtClient, vmiInformer cache.SharedIndexInformer) error {
	m.managerLock.Lock()
	defer m.managerLock.Unlock()

	if m.isShuttingDown {
		return fmt.Errorf("unable to process new migration connections during virt-handler shutdown")
	}

	curProxies, exists := m.targetProxies[vmiUID]
	if exists {
		if m.isTargetExistingProxy(curProxies, targetUnixFiles) {
			// No Op, already exists
			return m.startTargetSyncProxy(vmiUID, client, vmiInformer)
		} else {
			// stop the current proxy and point it somewhere new.
			for _, curProxy := range curProxies {
				m.logger.Infof("Manager stopping proxy on target node due to new unix filepath location")
				curProxy.Stop()
			}
			if m.targetSyncProxy[vmiUID] != nil {
				m.targetSyncProxy[vmiUID].StopSync()
				delete(m.targetSyncProxy, vmiUID)
			}
		}
	}
	log.DefaultLogger().Info("creating target socket proxies")

	serverTLSConfig := m.serverTLSConfig
	clientTLSConfig := m.clientTLSConfig
	if m.config.GetMigrationConfiguration().DisableTLS != nil && *m.config.GetMigrationConfiguration().DisableTLS {
		serverTLSConfig = nil
		clientTLSConfig = nil
	}
	proxiesList, err := m.createTargetSocketProxies(vmiUID, targetUnixFiles, clientTLSConfig, serverTLSConfig)
	if err != nil {
		return err
	}
	m.targetProxies[vmiUID] = proxiesList

	return m.startTargetSyncProxy(vmiUID, client, vmiInformer)
}

func (m *migrationProxyManager) startTargetSyncProxy(vmiUID string, client kubecli.KubevirtClient, vmiInformer cache.SharedIndexInformer) error {
	var err error
	_, exists := m.targetSyncProxy[vmiUID]
	if !exists {
		m.targetSyncProxy[vmiUID], err = NewMigrationSyncProxy(vmiInformer, client, m.clientTLSConfig, m.serverTLSConfig)
		m.targetSyncProxy[vmiUID].StartTargetSync()
	}
	return err
}

// This function is here for unit tests only.
func (m *migrationProxyManager) GetSourceListenerFiles(vmiUID string) []string {
	m.managerLock.Lock()
	defer m.managerLock.Unlock()

	curProxies, exists := m.sourceProxies[vmiUID]
	socketsList := []string{}
	if exists {
		for _, curProxy := range curProxies {
			if curProxy != nil {
				socketsList = append(socketsList, curProxy.GetSocketPath())
			}
		}
	}
	return socketsList
}

func (m *migrationProxyManager) GetTargetListenerPorts(vmiUID string) map[string]int {
	m.managerLock.Lock()
	defer m.managerLock.Unlock()

	m.logger.Infof("Getting target listener ports for VMI UID %s", vmiUID)
	curProxies, exists := m.targetProxies[vmiUID]
	targetSrcPortMap := make(map[string]int)

	if exists {
		for _, curProxy := range curProxies {
			m.logger.Infof("target proxy bind port: %s, target port: %d", curProxy.GetBindPort(), curProxy.GetTargetPort(vmiUID))
			targetSrcPortMap[curProxy.GetBindPort()] = curProxy.GetTargetPort(vmiUID)
		}
	}
	log.DefaultLogger().Infof("Map: %#v", targetSrcPortMap)
	return targetSrcPortMap
}

func (m *migrationProxyManager) GetSyncPort(vmiUID string) string {
	m.managerLock.Lock()
	defer m.managerLock.Unlock()
	syncProxy, exists := m.targetSyncProxy[vmiUID]
	if exists {
		m.logger.Infof("sync proxy bind port: %s", syncProxy.GetBindPort())
		return syncProxy.GetBindPort()
	}
	return "0"
}

func (m *migrationProxyManager) StopTargetListener(vmiUID string) {
	m.managerLock.Lock()
	defer m.managerLock.Unlock()

	curProxies, exists := m.targetProxies[vmiUID]
	if exists {
		for _, curProxy := range curProxies {
			m.logger.Info("Manager stopping proxy on target node")
			curProxy.Stop()
		}
		delete(m.targetProxies, vmiUID)
	}
	syncProxy, exists := m.targetSyncProxy[vmiUID]
	if exists {
		syncProxy.StopSync()
		delete(m.targetSyncProxy, vmiUID)
	}
}

func isSourceExistingProxy(curProxies []MigrationProxyListener, targetAddress string, destSrcPortMap map[string]int) bool {
	if len(curProxies) != len(destSrcPortMap) {
		return false
	}
	destSrcLookup := make(map[string]int)
	for dest, src := range destSrcPortMap {
		addr := net.JoinHostPort(targetAddress, dest)
		destSrcLookup[addr] = src
	}
	for _, curProxy := range curProxies {
		if _, ok := destSrcLookup[curProxy.GetTargetAddress()]; !ok {
			return false
		}
	}
	return true
}

func (m *migrationProxyManager) createSourceSocketProxies(vmiUID, targetAddress string, clientTLSConfig, serverTLSConfig *tls.Config, destSrcPortMap map[string]int, baseDir string) ([]MigrationProxyListener, error) {
	proxiesList := []MigrationProxyListener{}
	for destPort, srcPort := range destSrcPortMap {
		proxyKey := ConstructProxyKey(vmiUID, srcPort)
		targetFullAddr := net.JoinHostPort(targetAddress, destPort)
		filePath := SourceUnixFile(baseDir, proxyKey)

		os.RemoveAll(filePath)

		proxy := NewSourceSocketProxy(filePath, targetFullAddr, serverTLSConfig, clientTLSConfig, vmiUID)

		err := proxy.Start()
		if err != nil {
			proxy.Stop()
			// close all already created proxies for this vmiUID
			for _, curProxy := range proxiesList {
				curProxy.Stop()
			}
			return nil, err
		}
		proxiesList = append(proxiesList, proxy)
		m.logger.Infof("Manager created proxy on source node")
	}
	return proxiesList, nil
}

func (m *migrationProxyManager) StartSourceSync(vmiUID, syncAddress string, client kubecli.KubevirtClient, vmiInformer cache.SharedIndexInformer) error {
	m.logger.Infof("Starting source sync for VMI UID %s", vmiUID)
	var err error
	_, exists := m.sourceSyncProxy[vmiUID]
	if !exists {
		m.sourceSyncProxy[vmiUID], err = NewMigrationSyncProxy(vmiInformer, client, m.clientTLSConfig, m.serverTLSConfig)
		if err != nil {
			return err
		}
		err = m.sourceSyncProxy[vmiUID].StartSourceSync(syncAddress)
	}
	return err
}

func (m *migrationProxyManager) StartSourceListener(vmiUID string, targetAddress string, destSrcPortMap map[string]int, baseDir string) error {
	m.managerLock.Lock()
	defer m.managerLock.Unlock()

	if m.isShuttingDown {
		return fmt.Errorf("unable to process new migration connections during virt-handler shutdown")
	}

	curProxies, exists := m.sourceProxies[vmiUID]

	if exists {
		if isSourceExistingProxy(curProxies, targetAddress, destSrcPortMap) {
			// No Op, already exists
			return nil
		} else {
			// stop the current proxy and point it somewhere new.
			for _, curProxy := range curProxies {
				m.logger.Infof("Manager is stopping proxy on source node due to new target location")
				curProxy.Stop()
			}
		}
	}

	serverTLSConfig := m.serverTLSConfig
	clientTLSConfig := m.clientTLSConfig
	if m.config.GetMigrationConfiguration().DisableTLS != nil && *m.config.GetMigrationConfiguration().DisableTLS {
		serverTLSConfig = nil
		clientTLSConfig = nil
	}

	proxiesList, err := m.createSourceSocketProxies(vmiUID, targetAddress, clientTLSConfig, serverTLSConfig, destSrcPortMap, baseDir)
	m.sourceProxies[vmiUID] = proxiesList
	return err
}

func (m *migrationProxyManager) StopSourceListener(vmiUID string) {
	m.managerLock.Lock()
	defer m.managerLock.Unlock()

	curProxies, exists := m.sourceProxies[vmiUID]
	if exists {
		for _, curProxy := range curProxies {
			m.logger.Infof("Manager stopping proxy on source node")
			curProxy.Stop()
		}
		delete(m.sourceProxies, vmiUID)
	}
}
