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

	// grpc stuff

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	smarpc "github.com/spdk/spdk/apis/go/sma"
	"github.com/spdk/spdk/apis/go/sma/nvme"
	"github.com/spdk/spdk/apis/go/sma/nvmf_tcp"
	"github.com/spdk/spdk/apis/go/sma/virtio_blk"

	spdkcsiConfig "github.com/spdk/spdk-csi/pkg/config"
)

func NewSpdkCsiSmaInitiator(volumeContext map[string]string) (SpdkCsiInitiator, error) {
	targetType := strings.ToLower(volumeContext["targetType"])
	klog.Infof("SMA in volumeContext, use legacy connection to %v, target type is: %v", volumeContext["model"], targetType)
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
	serverURL     string
	deviceHandle  string // non-empty when connected
	volumeContext map[string]string
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

func (sma *smaCommon) SMAtovolUUID() []byte {
	klog.Infof("DELME: sma.volumeContext-model is: %+v", sma.volumeContext["model"])
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

func (sma *smaCommon) SMActx() (context.Context, context.CancelFunc) {
	ctxTimeout, cancel := context.WithTimeout(context.Background(), 42*time.Second)
	return ctxTimeout, cancel
}

func (sma *smaCommon) CreateDevice(req *smarpc.CreateDeviceRequest) string {
	ctxTimeout, cancel := sma.SMActx()
	defer cancel()

	// Create device
	response, err := sma.NewClient().CreateDevice(ctxTimeout, req)
	if err != nil {
		klog.Errorf("Creating device failed: %s", err)
	}
	klog.Infof("DELME: initiator Connect: CreateDevice response: %+v", response)

	return response.Handle
}

func (i *initiatorSmaNvme) Connect() (string, error) {
	// Check all block devices before CreateDevice
	cmdLine := []string{
		"lsblk", "-o", "NAME", "-n", "-i", "-r",
	}
	output, err := execWithTimeoutWithOutput(cmdLine, 40)
	if err != nil {
		return "", fmt.Errorf("command %v failed: %s", cmdLine, err)
	}
	BeforeOutput := strings.Fields(output)

	ctxTimeout, cancel := i.sma.SMActx()
	defer cancel()

	// Create device
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

	// Attach volume
	attachReq := &smarpc.AttachVolumeRequest{
		Volume:       &smarpc.VolumeParameters{VolumeId: i.sma.SMAtovolUUID()},
		DeviceHandle: i.sma.deviceHandle,
	}
	attachRes, err := i.sma.NewClient().AttachVolume(ctxTimeout, attachReq)
	if err != nil {
		return "", fmt.Errorf("Attaching volume failed: %s", err)
	}
	klog.Infof("Attaching volume succeeded! %s", attachRes.ProtoReflect())

	// Check all block devices after CreateDevice
	time.Sleep(2 * time.Second)
	output, err = execWithTimeoutWithOutput(cmdLine, 40)
	if err != nil {
		return "", fmt.Errorf("command %v failed: %s", cmdLine, err)
	}
	AfterOutput := strings.Fields(output)

	klog.Infof("DELME DIFF lsblk: %+v, %+v", BeforeOutput, AfterOutput)

	// Obtain the device path and return
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
	ctxTimeout, cancel := i.sma.SMActx()
	defer cancel()

	if i.sma.deviceHandle == "" {
		return fmt.Errorf("could not obtain the device handle")
	}

	// Detach volume
	detachReq := &smarpc.DetachVolumeRequest{
		VolumeId:     i.sma.SMAtovolUUID(),
		DeviceHandle: i.sma.deviceHandle,
	}
	detachRes, err := i.sma.NewClient().DetachVolume(ctxTimeout, detachReq)
	if err != nil {
		return fmt.Errorf("Detaching volume failed: %s", err)
	}
	klog.Infof("Detaching volume succeeded! %s", detachRes.ProtoReflect())
	// deviceGlob := fmt.Sprintf("/dev/disk/by-id/*%s*", i.sma.volumeContext["model"])
	// errwaitForDeviceGone := waitForDeviceGone(deviceGlob, 20)
	// if errwaitForDeviceGone == nil {

	deleteReq := &smarpc.DeleteDeviceRequest{
		Handle: i.sma.deviceHandle,
	}
	deleteRes, err := i.sma.NewClient().DeleteDevice(ctxTimeout, deleteReq)
	if err != nil {
		return fmt.Errorf("Deleting device failed: %s", err)
	}
	klog.Infof("Deleting device succeeded! %s", deleteRes.ProtoReflect())

	return err
}

func (i *initiatorSmaVirtioBlk) Connect() (string, error) {
	// Check all block devices before CreateDevice
	cmdLine := []string{
		"lsblk", "-o", "NAME", "-n", "-i", "-r",
	}
	output, err := execWithTimeoutWithOutput(cmdLine, 40)
	if err != nil {
		return "", fmt.Errorf("command %v failed: %s", cmdLine, err)
	}
	BeforeOutput := strings.Fields(output)

	// Create device
	req := &smarpc.CreateDeviceRequest{
		Volume: &smarpc.VolumeParameters{VolumeId: i.sma.SMAtovolUUID()},
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
	klog.Infof("DELME: i.sma.deviceHandle is: %+v", i.sma.deviceHandle)
	time.Sleep(8 * time.Second)

	// Check all block devices after CreateDevice
	output, err = execWithTimeoutWithOutput(cmdLine, 40)
	if err != nil {
		return "", fmt.Errorf("command %v failed: %s", cmdLine, err)
	}
	AfterOutput := strings.Fields(output)

	klog.Infof("DELME DIFF lsblk: %+v, %+v", BeforeOutput, AfterOutput)

	// Check the device path
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
	ctxTimeout, cancel := i.sma.SMActx()
	defer cancel()

	if i.sma.deviceHandle == "" {
		return fmt.Errorf("could not obtain the device handle")
	}

	// Delete device
	deleteReq := &smarpc.DeleteDeviceRequest{
		Handle: i.sma.deviceHandle,
	}
	detachRes, err := i.sma.NewClient().DeleteDevice(ctxTimeout, deleteReq)
	if err != nil {
		return fmt.Errorf("Deleting device failed: %s", err)
	}
	klog.Infof("Deleting device succeeded! %s", detachRes.ProtoReflect())

	// unmount the /dev/* device and wait for the device disappearing Delete device
	// deviceGlob := fmt.Sprintf("/dev/vda")
	// errwaitForDeviceGone := waitForDeviceGone(deviceGlob, 20)

	return err
}

func (i *initiatorSmaNvmf) Connect() (string, error) {
	ctxTimeout, cancel := i.sma.SMActx()
	defer cancel()

	// Create device
	req := &smarpc.CreateDeviceRequest{
		Volume: nil,
		Params: &smarpc.CreateDeviceRequest_NvmfTcp{
			NvmfTcp: &nvmf_tcp.DeviceParameters{
				Subnqn:  i.sma.volumeContext["nqn"],
				Adrfam:  "ipv4",
				Traddr:  i.sma.volumeContext["targetAddr"],
				Trsvcid: "4420",
			},
		},
	}
	klog.Infof("i.sma.volumeContext-nqn is %s", i.sma.volumeContext["nqn"])
	klog.Infof("req.GetNvmfTcp().Subnqn is %s", req.GetNvmfTcp().Subnqn)
	i.sma.deviceHandle = i.sma.CreateDevice(req)
	i.subnqn = req.GetNvmfTcp().Subnqn

	// Connect to device
	cmdLine := []string{
		"nvme", "connect", "-t", "tcp", "-a", "127.0.0.1", "-s", "4420", "-n", i.sma.volumeContext["nqn"],
	}
	err := execWithTimeout(cmdLine, 40)
	if err != nil {
		// go on checking device status in case caused by duplicated request
		klog.Errorf("command %v failed: %s", cmdLine, err)
	} else {
		klog.Infof("nvme connect succeeded!")
	}

	// Attach volume
	attachReq := &smarpc.AttachVolumeRequest{
		Volume:       &smarpc.VolumeParameters{VolumeId: i.sma.SMAtovolUUID()},
		DeviceHandle: i.sma.deviceHandle,
	}

	attachRes, err := i.sma.NewClient().AttachVolume(ctxTimeout, attachReq)
	if err != nil {
		klog.Errorf("Attaching volume failed: %s", err)
	} else {
		klog.Infof("Attaching volume succeeded! %s", attachRes.ProtoReflect())
	}

	// Check the device path
	deviceGlob := fmt.Sprintf("/dev/disk/by-id/*%s*", i.sma.volumeContext["model"])
	devicePath, err := waitForDeviceReady(deviceGlob, 20)
	if err != nil {
		return "", err
	}
	klog.Infof("Device path is %s", devicePath)

	return devicePath, nil
}

func (i *initiatorSmaNvmf) Disconnect() error {
	ctxTimeout, cancel := i.sma.SMActx()
	defer cancel()

	if i.sma.deviceHandle == "" {
		return fmt.Errorf("could not obtain the device handle")
	}

	// Detach volume
	detachReq := &smarpc.DetachVolumeRequest{
		VolumeId:     i.sma.SMAtovolUUID(),
		DeviceHandle: i.sma.deviceHandle,
	}

	detachRes, err := i.sma.NewClient().DetachVolume(ctxTimeout, detachReq)
	if err != nil {
		klog.Errorf("Detaching volume failed: %s", err)
	} else {
		klog.Infof("Detaching volume succeeded! %s", detachRes.ProtoReflect())
	}

	// Disconnect device
	cmdLine := []string{
		"nvme", "disconnect", "-n", i.subnqn,
	}
	err = execWithTimeout(cmdLine, 40)
	if err != nil {
		// go on checking device status in case caused by duplicated request
		klog.Errorf("command %v failed: %s", cmdLine, err)
	} else {
		klog.Infof("nvme disconnect succeeded!")
	}

	// Delete device
	deviceGlob := fmt.Sprintf("/dev/disk/by-id/*%s*", i.sma.volumeContext["model"])
	errwaitForDeviceGone := waitForDeviceGone(deviceGlob, 20)
	if errwaitForDeviceGone == nil {
		deleteReq := &smarpc.DeleteDeviceRequest{
			Handle: i.sma.deviceHandle,
		}
		detachRes, err := i.sma.NewClient().DeleteDevice(ctxTimeout, deleteReq)
		if err != nil {
			klog.Errorf("Deleting device failed: %s", err)
		} else {
			klog.Infof("Deleting device succeeded! %s", detachRes.ProtoReflect())
		}
		return err
	}
	return errwaitForDeviceGone
}
