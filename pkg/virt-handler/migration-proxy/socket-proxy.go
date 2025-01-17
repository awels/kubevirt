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
 * Copyright 2018 Red Hat, Inc.
 *
 */

package migrationproxy

import (
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"kubevirt.io/client-go/log"

	diskutils "kubevirt.io/kubevirt/pkg/ephemeral-disk-utils"
	"kubevirt.io/kubevirt/pkg/util"
)

const (
	LibvirtDirectMigrationPort = 49152
	LibvirtBlockMigrationPort  = 49153
	VMISyncPort                = 49154
)

var migrationPortsRange = []int{LibvirtDirectMigrationPort, LibvirtBlockMigrationPort}

type MigrationProxyListener interface {
	Start() error
	Stop()
	GetTargetAddress() string
	GetTargetPort(vmiUID string) int
	GetTargetProtocol() string
	GetBindPort() string
	GetSocketPath() string
	GetSocketListener() net.Listener
}

type migrationProxy struct {
	unixSocketPath    string
	tcpBindAddress    string
	tcpBindPort       int
	targetAddress     string
	targetProtocol    string
	stopChan          chan struct{}
	listenErrChan     chan error
	fdChan            chan io.ReadWriteCloser
	migrationTypePort int

	listener        net.Listener
	serverTLSConfig *tls.Config
	clientTLSConfig *tls.Config

	logger *log.FilteredLogger
}

func GetMigrationPortsList(isBlockMigration bool) (ports []int) {
	ports = append(ports, migrationPortsRange[0])
	if isBlockMigration {
		ports = append(ports, migrationPortsRange[1])
	}
	return
}

func SourceUnixFile(baseDir string, key string) string {
	return filepath.Join(baseDir, "migrationproxy", key+"-source.sock")
}

func ConstructProxyKey(id string, port int) string {
	key := id
	if port != 0 {
		key += fmt.Sprintf("-%d", port)
	}
	return key
}

// SRC POD ENV(migration unix socket) <-> HOST ENV (tcp client) <-----> HOST ENV (tcp server) <-> TARGET POD ENV (virtqemud unix socket)

// Source proxy exposes a unix socket server and pipes to an outbound TCP connection.
func NewSourceSocketProxy(unixSocketPath string, tcpTargetAddress string, serverTLSConfig *tls.Config, clientTLSConfig *tls.Config, vmiUID string) MigrationProxyListener {
	return &migrationProxy{
		unixSocketPath:  unixSocketPath,
		targetAddress:   tcpTargetAddress,
		targetProtocol:  "tcp",
		stopChan:        make(chan struct{}),
		fdChan:          make(chan io.ReadWriteCloser, 1),
		listenErrChan:   make(chan error, 1),
		serverTLSConfig: serverTLSConfig,
		clientTLSConfig: clientTLSConfig,
		logger:          log.Log.With("uid", vmiUID).With("listening", filepath.Base(unixSocketPath)).With("outbound", tcpTargetAddress),
	}
}

// Target proxy listens on a tcp socket and pipes to a virtqemud unix socket
func NewTargetSocketProxy(tcpBindAddress string, tcpBindPort int, serverTLSConfig *tls.Config, clientTLSConfig *tls.Config, virtqemudSocketPath string, vmiUID string) MigrationProxyListener {
	return &migrationProxy{
		tcpBindAddress:  tcpBindAddress,
		tcpBindPort:     tcpBindPort,
		targetAddress:   virtqemudSocketPath,
		targetProtocol:  "unix",
		stopChan:        make(chan struct{}),
		fdChan:          make(chan io.ReadWriteCloser, 1),
		listenErrChan:   make(chan error, 1),
		serverTLSConfig: serverTLSConfig,
		clientTLSConfig: clientTLSConfig,
		logger:          log.Log.With("uid", vmiUID).With("outbound", filepath.Base(virtqemudSocketPath)),
	}

}
func (m *migrationProxy) GetBindPort() string {
	return strconv.Itoa(m.tcpBindPort)
}

func (m *migrationProxy) GetSocketPath() string {
	return m.unixSocketPath
}

func (m *migrationProxy) GetSocketListener() net.Listener {
	return m.listener
}

func (m *migrationProxy) GetTargetPort(vmiUID string) int {
	for _, port := range migrationPortsRange {
		key := ConstructProxyKey(vmiUID, port)
		if strings.Contains(m.GetTargetAddress(), key) {
			return port
		}
	}
	return 0
}

func (m *migrationProxy) createTcpListener() error {
	var listener net.Listener
	var err error

	laddr := net.JoinHostPort(m.tcpBindAddress, strconv.Itoa(m.tcpBindPort))
	if m.serverTLSConfig != nil {
		listener, err = tls.Listen("tcp", laddr, m.serverTLSConfig)
	} else {
		listener, err = net.Listen("tcp", laddr)
	}
	if err != nil {
		m.logger.Reason(err).Error("failed to create tcp connection for proxy service")
		return err
	}

	if m.tcpBindPort == 0 {
		// update the random port that was selected
		m.tcpBindPort = listener.Addr().(*net.TCPAddr).Port
		// Add the listener to the log output once we know the port
		m.logger = m.logger.With("listening", fmt.Sprintf("%s:%d", m.tcpBindAddress, m.tcpBindPort))
	}

	m.listener = listener
	return nil
}

func (m *migrationProxy) createUnixListener() error {

	os.RemoveAll(m.unixSocketPath)
	err := util.MkdirAllWithNosec(filepath.Dir(m.unixSocketPath))
	if err != nil {
		m.logger.Reason(err).Error("unable to create directory for unix socket")
		return err
	}

	listener, err := net.Listen("unix", m.unixSocketPath)
	if err != nil {
		m.logger.Reason(err).Error("failed to create unix socket for proxy service")
		return err
	}
	if err := diskutils.DefaultOwnershipManager.UnsafeSetFileOwnership(m.unixSocketPath); err != nil {
		log.Log.Reason(err).Error("failed to change ownership on migration unix socket")
		return err
	}

	m.listener = listener
	return nil

}

func (m *migrationProxy) GetTargetAddress() string {
	return m.targetAddress
}

func (m *migrationProxy) GetTargetProtocol() string {
	return m.targetProtocol
}

func (m *migrationProxy) Stop() {
	close(m.stopChan)
	if m.listener != nil {
		m.logger.Infof("proxy stopped listening")
		m.listener.Close()
	}
	if err := os.RemoveAll(m.unixSocketPath); err != nil {
		m.logger.Reason(err).Error("unable to remove unix socket")
	}
}

func (m *migrationProxy) handleConnection(fd io.ReadWriteCloser) {
	defer fd.Close()

	outBoundErr := make(chan error, 1)
	inBoundErr := make(chan error, 1)

	var conn net.Conn
	var err error
	if m.targetProtocol == "tcp" && m.clientTLSConfig != nil {
		m.logger.With("target", m.targetAddress).Info("dialing tcp outbound connection")
		conn, err = tls.Dial(m.targetProtocol, m.targetAddress, m.clientTLSConfig)
	} else {
		m.logger.With("target", m.targetAddress).Info("dialing unix outbound connection")
		conn, err = net.Dial(m.targetProtocol, m.targetAddress)
	}
	if err != nil {
		m.logger.Reason(err).Error("unable to create outbound leg of proxy to host")
		return
	}

	go func() {
		//from outbound connection to proxy
		n, err := io.Copy(fd, conn)
		m.logger.Infof("%d bytes copied outbound to inbound", n)
		inBoundErr <- err
	}()
	go func() {
		//from proxy to outbound connection
		n, err := io.Copy(conn, fd)
		m.logger.Infof("%d bytes copied from inbound to outbound", n)
		outBoundErr <- err
	}()

	select {
	case err = <-outBoundErr:
		if err != nil {
			m.logger.Reason(err).Errorf("error encountered copying data to outbound connection")
		}
	case err = <-inBoundErr:
		if err != nil {
			m.logger.Reason(err).Errorf("error encountered copying data into inbound connection")
		}
	case <-m.stopChan:
		m.logger.Info("stop channel terminated proxy")
	}
}

func (m *migrationProxy) Start() error {

	if m.unixSocketPath != "" {
		m.logger.Infof("creating unix socket listener")
		err := m.createUnixListener()
		if err != nil {
			return err
		}
	} else {
		m.logger.Infof("creating tcp listener")
		err := m.createTcpListener()
		if err != nil {
			return err
		}
	}

	go func(ln net.Listener, fdChan chan io.ReadWriteCloser, listenErr chan error, stopChan chan struct{}) {
		for {
			fd, err := ln.Accept()
			if err != nil {
				listenErr <- err

				select {
				case <-stopChan:
					// If the stopChan is closed, then this is expected. Log at a lesser debug level
					m.logger.Reason(err).V(3).Infof("stopChan is closed. Listener exited with expected error.")
				default:
					m.logger.Reason(err).Error("proxy unix socket listener returned error.")
				}
				break
			} else {
				fdChan <- fd
			}
		}
	}(m.listener, m.fdChan, m.listenErrChan, m.stopChan)

	go func(m *migrationProxy) {
		for {
			select {
			case fd := <-m.fdChan:
				go m.handleConnection(fd)
			case <-m.stopChan:
				return
			case <-m.listenErrChan:
				return
			}
		}

	}(m)

	m.logger.Infof("proxy started listening")
	return nil
}
