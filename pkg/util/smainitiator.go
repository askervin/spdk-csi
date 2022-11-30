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

func (sma *smaCommon) NewClient() smarpc.StorageManagementAgentClient {
	var conn *grpc.ClientConn
	/* TODO: Fix error in dialing: either return (client, error)
	   and check the error from the caller, or
	   implement grpc ClientConnectionInterface where Invoke returns
	   a "service not available" error or similar */
	conn, err := grpc.Dial(sma.serverURL, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		klog.Errorf("failed to connect to SMA grpc server in: %s, %s", sma.serverURL, err)
		return nil
	}
	client := smarpc.NewStorageManagementAgentClient(conn)
	return client
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

func (sma *smaCommon) CreateDevice(req *smarpc.CreateDeviceRequest) string {
	ctxTimeout, cancel := sma.ctxTimeout()
	defer cancel()

	// New SMA client and do CreateDevice
	response, err := sma.NewClient().CreateDevice(ctxTimeout, req)
	if err != nil {
		klog.Errorf("DELME: SMA.CreateDevice failed: %s", err)
	}
	klog.Infof("DELME: SMA.CreateDevice response: %+v", response)

	if response == nil {
		return ""
	}

	return response.Handle
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

	ctxTimeout, cancel := i.sma.ctxTimeout()
	defer cancel()

	// CreateDevice for Nvme
	req := &smarpc.CreateDeviceRequest{
		Params: &smarpc.CreateDeviceRequest_Nvme{
			Nvme: &nvme.DeviceParameters{
				PhysicalId: 0,
				VirtualId:  0,
			},
		},
	}

	i.sma.deviceHandle = i.sma.CreateDevice(req)
	if i.sma.deviceHandle == "" {
		return "", fmt.Errorf("could not obtain the device handle after CreateDevice")
	}
	klog.Infof("DELME: i.sma.deviceHandle is: %+v", i.sma.deviceHandle)

	// AttachVolume for Nvme
	attachReq := &smarpc.AttachVolumeRequest{
		Volume: &smarpc.VolumeParameters{
			VolumeId:         i.sma.volumeUUID(),
			ConnectionParams: i.sma.nvmfVolumeParameters(),
		},
		DeviceHandle: i.sma.deviceHandle,
	}

	attachRes, err := i.sma.NewClient().AttachVolume(ctxTimeout, attachReq)
	if err != nil {
		return "", fmt.Errorf("DELME: SMA.AttachVolume failed: %+v, %s", attachRes, err)
	}
	klog.Infof("DELME: SMA.AttachVolume succeeded!")

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
	ctxTimeout, cancel := i.sma.ctxTimeout()
	defer cancel()

	if i.sma.deviceHandle == "" {
		return fmt.Errorf("could not obtain the device handle")
	}

	// DetachVolume for Nvme
	detachReq := &smarpc.DetachVolumeRequest{
		VolumeId:     i.sma.volumeUUID(),
		DeviceHandle: i.sma.deviceHandle,
	}
	detachRes, err := i.sma.NewClient().DetachVolume(ctxTimeout, detachReq)
	if err != nil {
		return fmt.Errorf("DELME: SMA.DetachVolume failed: %+v, %s", detachRes, err)
	}
	klog.Infof("DELME: SMA.DetachVolume succeeded!")

	// DeleteDevice for Nvme
	deleteReq := &smarpc.DeleteDeviceRequest{
		Handle: i.sma.deviceHandle,
	}
	deleteRes, err := i.sma.NewClient().DeleteDevice(ctxTimeout, deleteReq)
	if err != nil {
		return fmt.Errorf("DELME: SMA.DeleteDevice failed: %+v, %s", deleteRes, err)
	}
	klog.Infof("DELME: SMA.DeleteDevice succeeded!")

	return err
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

	// CreateDevice for VirtioBlk
	req := &smarpc.CreateDeviceRequest{
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

	i.sma.deviceHandle = i.sma.CreateDevice(req)
	if i.sma.deviceHandle == "" {
		return "", fmt.Errorf("could not obtain the device handle after CreateDevice")
	}
	klog.Infof("DELME: i.sma.deviceHandle is: %s", i.sma.deviceHandle)
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
	ctxTimeout, cancel := i.sma.ctxTimeout()
	defer cancel()

	if i.sma.deviceHandle == "" {
		return fmt.Errorf("could not obtain the device handle")
	}

	// DeleteDevice for VirtioBlk
	deleteReq := &smarpc.DeleteDeviceRequest{
		Handle: i.sma.deviceHandle,
	}
	detachRes, err := i.sma.NewClient().DeleteDevice(ctxTimeout, deleteReq)
	if err != nil {
		return fmt.Errorf("DELME: SMA.DeleteDevice failed: %+v, %s", detachRes, err)
	}
	klog.Infof("DELME: SMA.DeleteDevice succeeded!")

	return err
}

func (i *initiatorSmaNvmf) Connect() (string, error) {
	ctxTimeout, cancel := i.sma.ctxTimeout()
	defer cancel()

	// CreateDevice for NvmfTcp
	req := &smarpc.CreateDeviceRequest{
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
	i.sma.deviceHandle = i.sma.CreateDevice(req)
	// i.subnqn = req.GetNvmfTcp().Subnqn

	// AttachVolume for NvmfTcp
	attachReq := &smarpc.AttachVolumeRequest{
		Volume: &smarpc.VolumeParameters{
			VolumeId:         i.sma.volumeUUID(),
			ConnectionParams: i.sma.nvmfVolumeParameters(),
		},
		DeviceHandle: i.sma.deviceHandle,
	}

	attachRes, err := i.sma.NewClient().AttachVolume(ctxTimeout, attachReq)
	if err != nil {
		klog.Errorf("DELME: SMA.AttachVolume failed: %+v, %s", attachRes, err)
	} else {
		klog.Infof("DELME: SMA.AttachVolume succeeded!")
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
	ctxTimeout, cancel := i.sma.ctxTimeout()
	defer cancel()

	if i.sma.deviceHandle == "" {
		return fmt.Errorf("could not obtain the device handle")
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

	// DetachVolume for NvmfTcp
	detachReq := &smarpc.DetachVolumeRequest{
		VolumeId:     i.sma.volumeUUID(),
		DeviceHandle: i.sma.deviceHandle,
	}

	detachRes, err := i.sma.NewClient().DetachVolume(ctxTimeout, detachReq)
	if err != nil {
		klog.Errorf("DELME: SMA.DetachVolume failed: %+v, %s", detachRes, err)
	} else {
		klog.Infof("DELME: SMA.DetachVolume succeeded!")
	}

	// DeleteDevice for NvmfTcp
	deviceGlob := fmt.Sprintf("/dev/disk/by-id/*%s*", i.sma.volumeContext["model"])
	errwaitForDeviceGone := waitForDeviceGone(deviceGlob, 20)
	if errwaitForDeviceGone == nil {
		deleteReq := &smarpc.DeleteDeviceRequest{
			Handle: i.sma.deviceHandle,
		}
		deleteRes, err := i.sma.NewClient().DeleteDevice(ctxTimeout, deleteReq)
		if err != nil {
			klog.Errorf("DELME: SMA.DeleteDevice failed: %+v, %s", deleteRes, err)
		} else {
			klog.Infof("DELME: SMA.DeleteDevice succeeded!")
		}
		return err
	}
	return errwaitForDeviceGone
}
