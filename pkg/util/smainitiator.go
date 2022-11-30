/*
Copyright (c) Intel Corporation.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package util

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"k8s.io/klog"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	smarpc "github.com/spdk/spdk/apis/go/sma"
	"github.com/spdk/spdk/apis/go/sma/nvme"
	"github.com/spdk/spdk/apis/go/sma/nvmf"
	"github.com/spdk/spdk/apis/go/sma/nvmf_tcp"
	"github.com/spdk/spdk/apis/go/sma/virtio_blk"

	spdkcsiConfig "github.com/spdk/spdk-csi/pkg/config"
)

func NewSpdkCsiSmaInitiator(volumeContext map[string]string) (SpdkCsiInitiator, error) {
	smaConfigString, ok := volumeContext["sma"]
	if !ok {
		return nil, fmt.Errorf("volumeContext does not include \"sma\"")
	}
	smaConfig := spdkcsiConfig.SmaConfig{}
	err := json.Unmarshal([]byte(smaConfigString), &smaConfig)
	if err != nil {
		return nil, fmt.Errorf("invalid SMA configuration: %q (%w)", smaConfigString, err)
	}
	klog.Infof("smaConfig is %v", smaConfig.ClassConfig.Type)
	iSmaCommon := &smaCommon{
		volumeContext: volumeContext,
		serverURL:     smaConfig.Server,
		timeout:       60 * time.Second,
	}
	switch smaConfig.ClassConfig.Type {
	case "NvmfTcp":
		return &initiatorSmaNvmf{sma: iSmaCommon}, nil
	case "VirtioBlk":
		return &initiatorSmaVirtioBlk{sma: iSmaCommon}, nil
	case "Nvme":
		return &initiatorSmaNvme{sma: iSmaCommon}, nil
	default:
		klog.Errorf("Unsupported SMA target type in %v", volumeContext)
	}
	return nil, fmt.Errorf("unknown SMA target type: %q", volumeContext["targetType"])
}

type smaCommon struct {
	serverURL     string // SMA GRPC server
	deviceHandle  string // non-empty when connected
	volumeContext map[string]string
	timeout       time.Duration
}

type initiatorSmaNvmf struct {
	subnqn string
	sma    *smaCommon
}

type initiatorSmaVirtioBlk struct {
	sma *smaCommon
}

type initiatorSmaNvme struct {
	subnqn string
	sma    *smaCommon
}

func (sma *smaCommon) NewClient() (smarpc.StorageManagementAgentClient, error) {
	conn, err := grpc.Dial(sma.serverURL, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		klog.Errorf("failed to connect to SMA grpc server in: %s, %s", sma.serverURL, err)
		return nil, fmt.Errorf("SMA grpc connect error: %w", err)
	}
	client := smarpc.NewStorageManagementAgentClient(conn)
	return client, nil
}

func (sma *smaCommon) volumeUUID() []byte {
	if sma.volumeContext["model"] == "" {
		klog.Errorf("no volume available")
	}
	volUUID := uuid.MustParse(sma.volumeContext["model"])
	volUUIDBytes, err := volUUID.MarshalBinary()
	if err != nil {
		klog.Errorf("volUUID.MarshalBinary() failed: %s", err)
	}
	return volUUIDBytes
}

func (sma *smaCommon) ctxTimeout() (context.Context, context.CancelFunc) {
	ctxTimeout, cancel := context.WithTimeout(context.Background(), sma.timeout)
	return ctxTimeout, cancel
}

func (sma *smaCommon) nvmfVolumeParameters() *smarpc.VolumeParameters_Nvmf {
	vcp := &smarpc.VolumeParameters_Nvmf{
		Nvmf: &nvmf.VolumeConnectionParameters{
			Subnqn:  "",
			Hostnqn: sma.volumeContext["nqn"],
			ConnectionParams: &nvmf.VolumeConnectionParameters_Discovery{
				Discovery: &nvmf.VolumeDiscoveryParameters{
					DiscoveryEndpoints: []*nvmf.Address{
						{
							Trtype:  sma.volumeContext["targetType"],
							Traddr:  sma.volumeContext["targetAddr"],
							Trsvcid: sma.volumeContext["targetPort"],
						},
					},
				},
			},
		},
	}
	return vcp
}

func (sma *smaCommon) CreateDevice(client smarpc.StorageManagementAgentClient, req *smarpc.CreateDeviceRequest) error {
	ctxTimeout, cancel := sma.ctxTimeout()
	defer cancel()

	klog.Infof("SMA.CreateDevice(%s) = ...", req)
	response, err := client.CreateDevice(ctxTimeout, req)
	if err != nil {
		return fmt.Errorf("SMA.CreateDevice(%s) error: %w", req, err)
	}
	klog.Infof("SMA.CreateDevice(...) => %+v", response)

	if response == nil {
		return fmt.Errorf("SMA.CreateDevice(%s) error: nil response", req)
	}
	if response.Handle == "" {
		return fmt.Errorf("SMA.CreateDevice(%s) error: no device handle in response", req)
	}
	sma.deviceHandle = response.Handle

	return nil
}

func (sma *smaCommon) AttachVolume(client smarpc.StorageManagementAgentClient, req *smarpc.AttachVolumeRequest) error {
	ctxTimeout, cancel := sma.ctxTimeout()
	defer cancel()

	klog.Infof("SMA.AttachVolume(%s) = ...", req)
	response, err := client.AttachVolume(ctxTimeout, req)
	if err != nil {
		return fmt.Errorf("SMA.AttachVolume(%s) error: %w", req, err)
	}
	klog.Infof("SMA.AttachVolume(...) => %+v", response)

	if response == nil {
		return fmt.Errorf("SMA.AttachVolume(%s) error: nil response", req)
	}

	return nil
}

func (sma *smaCommon) DetachVolume(client smarpc.StorageManagementAgentClient, req *smarpc.DetachVolumeRequest) error {
	ctxTimeout, cancel := sma.ctxTimeout()
	defer cancel()

	klog.Infof("SMA.DetachVolume(%s) = ...", req)
	response, err := client.DetachVolume(ctxTimeout, req)
	if err != nil {
		return fmt.Errorf("SMA.DetachVolume(%s) error: %w", req, err)
	}
	klog.Infof("SMA.DetachVolume(...) => %+v", response)

	if response == nil {
		return fmt.Errorf("SMA.DetachVolume(%s) error: nil response", req)
	}

	return nil
}

func (sma *smaCommon) DeleteDevice(client smarpc.StorageManagementAgentClient, req *smarpc.DeleteDeviceRequest) error {
	ctxTimeout, cancel := sma.ctxTimeout()
	defer cancel()

	klog.Infof("SMA.DeleteDevice(%s) = ...", req)
	response, err := client.DeleteDevice(ctxTimeout, req)
	if err != nil {
		return fmt.Errorf("SMA.DeleteDevice(%s) error: %w", req, err)
	}
	klog.Infof("SMA.DeleteDevice(...) => %+v", response)

	if response == nil {
		return fmt.Errorf("SMA.DeleteDevice(%s) error: nil response", req)
	}
	sma.deviceHandle = ""

	return nil
}

func (sma *smaCommon) disconnect(sendDetachVolume bool) error {
	if sma.deviceHandle == "" {
		klog.Infof("SMA initiator: already disconnected")
		return nil
	}

	smaClient, err := sma.NewClient()
	if err != nil {
		return err
	}

	if sendDetachVolume {
		detachReq := &smarpc.DetachVolumeRequest{
			VolumeId:     sma.volumeUUID(),
			DeviceHandle: sma.deviceHandle,
		}
		if err := sma.DetachVolume(smaClient, detachReq); err != nil {
			return err
		}
	}

	// DeleteDevice for Nvme
	deleteReq := &smarpc.DeleteDeviceRequest{
		Handle: sma.deviceHandle,
	}
	if err := sma.DeleteDevice(smaClient, deleteReq); err != nil {
		return err
	}
	return nil
}

func (i *initiatorSmaNvme) Connect() (string, error) {
	// List all block devices before CreateDevice/AttachVolume for Nvme
	cmdLine := []string{
		"lsblk", "-o", "NAME", "-n", "-i", "-r",
	}
	output, err := execWithTimeoutWithOutput(cmdLine, 40)
	if err != nil {
		return "", fmt.Errorf("command %v failed: %s", cmdLine, err)
	}
	BeforeOutput := strings.Fields(output)

	smaClient, err := i.sma.NewClient()
	if err != nil {
		return "", err
	}

	// CreateDevice for Nvme
	createReq := &smarpc.CreateDeviceRequest{
		Params: &smarpc.CreateDeviceRequest_Nvme{
			Nvme: &nvme.DeviceParameters{
				PhysicalId: 0,
				VirtualId:  0,
			},
		},
	}
	if err := i.sma.CreateDevice(smaClient, createReq); err != nil {
		return "", err
	}

	// AttachVolume for Nvme
	attachReq := &smarpc.AttachVolumeRequest{
		Volume: &smarpc.VolumeParameters{
			VolumeId:         i.sma.volumeUUID(),
			ConnectionParams: i.sma.nvmfVolumeParameters(),
		},
		DeviceHandle: i.sma.deviceHandle,
	}

	if err := i.sma.AttachVolume(smaClient, attachReq); err != nil {
		return "", err
	}

	// List all block devices after CreateDevice/AttachVolume for Nvme
	time.Sleep(2 * time.Second)
	output, err = execWithTimeoutWithOutput(cmdLine, 40)
	if err != nil {
		return "", fmt.Errorf("command %v failed: %s", cmdLine, err)
	}
	AfterOutput := strings.Fields(output)

	// Compare block device list before and after CreateDevice/AttachVolume for Nvme and obtain device path
	if len(AfterOutput) == len(BeforeOutput)+1 {
		deviceGlob := fmt.Sprintf("/dev/%s*", AfterOutput[len(AfterOutput)-1])
		devicePath, err := waitForDeviceReady(deviceGlob, 20)
		if err != nil {
			return "", err
		}
		klog.Infof("Device path is %s", devicePath)
		return devicePath, nil
	}

	return "", fmt.Errorf("could not find the expected block device")
}

func (i *initiatorSmaNvme) Disconnect() error {
	return i.sma.disconnect(true)
}

func (i *initiatorSmaVirtioBlk) Connect() (string, error) {
	// List all block devices before CreateDevice for VirtioBlk
	cmdLine := []string{
		"lsblk", "-o", "NAME", "-n", "-i", "-r",
	}
	output, err := execWithTimeoutWithOutput(cmdLine, 40)
	if err != nil {
		return "", fmt.Errorf("command %v failed: %s", cmdLine, err)
	}
	BeforeOutput := strings.Fields(output)

	smaClient, err := i.sma.NewClient()
	if err != nil {
		return "", err
	}

	// CreateDevice for VirtioBlk
	createReq := &smarpc.CreateDeviceRequest{
		Volume: &smarpc.VolumeParameters{
			VolumeId:         i.sma.volumeUUID(),
			ConnectionParams: i.sma.nvmfVolumeParameters(),
		},
		Params: &smarpc.CreateDeviceRequest_VirtioBlk{
			VirtioBlk: &virtio_blk.DeviceParameters{
				PhysicalId: 0,
				VirtualId:  0,
			},
		},
	}

	if err := i.sma.CreateDevice(smaClient, createReq); err != nil {
		return "", err
	}

	time.Sleep(8 * time.Second)
	// List all block devices after CreateDevice for VirtioBlk
	output, err = execWithTimeoutWithOutput(cmdLine, 40)
	if err != nil {
		return "", fmt.Errorf("command %v failed: %s", cmdLine, err)
	}
	AfterOutput := strings.Fields(output)

	// Compare block device list before and after CreateDevice for VirtioBlk and obtain device path
	if len(AfterOutput) == len(BeforeOutput)+1 {
		deviceGlob := fmt.Sprintf("/dev/%s*", AfterOutput[len(AfterOutput)-1])
		devicePath, err := waitForDeviceReady(deviceGlob, 20)
		if err != nil {
			return "", err
		}
		klog.Infof("Device path is %s", devicePath)
		return devicePath, nil
	}

	return "", fmt.Errorf("could not find device path")
}

func (i *initiatorSmaVirtioBlk) Disconnect() error {
	return i.sma.disconnect(false)
}

func (i *initiatorSmaNvmf) Connect() (string, error) {
	smaClient, err := i.sma.NewClient()
	if err != nil {
		return "", err
	}

	// CreateDevice for NvmfTcp
	createReq := &smarpc.CreateDeviceRequest{
		Volume: nil,
		Params: &smarpc.CreateDeviceRequest_NvmfTcp{
			NvmfTcp: &nvmf_tcp.DeviceParameters{
				Subnqn:       "nqn.2022-04.io.spdk.csi:cnode0",
				Adrfam:       "ipv4",
				Traddr:       "127.0.0.1",
				Trsvcid:      "4421",
				AllowAnyHost: true,
			},
		},
	}

	if err := i.sma.CreateDevice(smaClient, createReq); err != nil {
		return "", err
	}

	// i.subnqn = req.GetNvmfTcp().Subnqn

	// AttachVolume for NvmfTcp
	attachReq := &smarpc.AttachVolumeRequest{
		Volume: &smarpc.VolumeParameters{
			VolumeId:         i.sma.volumeUUID(),
			ConnectionParams: i.sma.nvmfVolumeParameters(),
		},
		DeviceHandle: i.sma.deviceHandle,
	}

	if err := i.sma.AttachVolume(smaClient, attachReq); err != nil {
		return "", err
	}

	// Connect to the block device for NvmfTcp
	cmdLine := []string{
		"nvme", "connect", "-t", "tcp", "-a", "127.0.0.1", "-s", "4421", "-n", "nqn.2022-04.io.spdk.csi:cnode0",
	}
	err = execWithTimeout(cmdLine, 40)
	if err != nil {
		klog.Errorf("command %v failed: %s", cmdLine, err)
	} else {
		klog.Infof("nvme connect succeeded!")
	}

	// Obtain the path of the block device for NvmfTcp
	deviceGlob := fmt.Sprintf("/dev/disk/by-id/*%s*", i.sma.volumeContext["model"])
	devicePath, err := waitForDeviceReady(deviceGlob, 20)
	if err != nil {
		return "", err
	}
	klog.Infof("Device path is %s", devicePath)

	return devicePath, nil
}

func (i *initiatorSmaNvmf) Disconnect() error {
	if i.sma.deviceHandle == "" {
		klog.Infof("SMA NVME disconnect: already disconnected")
		return nil
	}

	// Disconnect the block device for NvmfTcp
	cmdLine := []string{
		"nvme", "disconnect", "-n", "nqn.2022-04.io.spdk.csi:cnode0",
	}
	err := execWithTimeout(cmdLine, 40)
	if err != nil {
		klog.Errorf("command %v failed: %s", cmdLine, err)
	} else {
		klog.Infof("nvme disconnect succeeded!")
	}

	smaClient, err := i.sma.NewClient()
	if err != nil {
		return err
	}

	// DetachVolume for NvmfTcp
	detachReq := &smarpc.DetachVolumeRequest{
		VolumeId:     i.sma.volumeUUID(),
		DeviceHandle: i.sma.deviceHandle,
	}

	if err := i.sma.DetachVolume(smaClient, detachReq); err != nil {
		return err
	}

	deviceGlob := fmt.Sprintf("/dev/disk/by-id/*%s*", i.sma.volumeContext["model"])
	if err := waitForDeviceGone(deviceGlob, 20); err != nil {
		return err
	}

	// DeleteDevice for NvmfTcp
	deleteReq := &smarpc.DeleteDeviceRequest{
		Handle: i.sma.deviceHandle,
	}
	if err := i.sma.DeleteDevice(smaClient, deleteReq); err != nil {
		return err
	}
	return nil
}
