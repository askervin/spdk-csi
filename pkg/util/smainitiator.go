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
	"github.com/spdk/spdk/apis/go/sma/nvmf_tcp"
	"github.com/spdk/spdk/apis/go/sma/virtio_blk"

	spdkcsiConfig "github.com/spdk/spdk-csi/pkg/config"
)

func NewSpdkCsiSmaInitiator(volumeContext map[string]string) (SpdkCsiInitiator, error) {
	targetType := strings.ToLower(volumeContext["targetType"])
	klog.Infof("SMA in volumeContext, use legacy connection to %v", volumeContext["model"])
	smaConfigString, ok := volumeContext["sma"]
	if !ok {
		return nil, fmt.Errorf("volumeContext does not include \"sma\"")
	}
	smaConfig := spdkcsiConfig.SmaConfig{}
	err := json.Unmarshal([]byte(smaConfigString), &smaConfig)
	if err != nil {
		return nil, fmt.Errorf("invalid SMA configuration: %q (%w)", smaConfigString, err)
	}
	iSmaCommon := &smaCommon{
		volumeContext: volumeContext,
		serverURL:     smaConfig.Server,
	}
	switch targetType {
	case "tcp":
		return &initiatorSmaNvmf{sma: iSmaCommon}, nil
	case "virtio-blk":
		return &initiatorSmaVirtioBlk{sma: iSmaCommon}, nil
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

func (i *initiatorSmaVirtioBlk) Connect() (string, error) {
	req := &smarpc.CreateDeviceRequest{
		Volume: nil,
		Params: &smarpc.CreateDeviceRequest_VirtioBlk{
			VirtioBlk: &virtio_blk.DeviceParameters{
				PhysicalId: 424242,
				VirtualId:  424242,
			},
		},
	}
	return "", fmt.Errorf("NOT IMPLEMENTED: i.sma.CreateDevice(%v)...", req)
}

func (i *initiatorSmaVirtioBlk) Disconnect() error {
	return fmt.Errorf("NOT IMPLEMENTED: i.sma.DeleteDevice(%v)...")
}

func (i *initiatorSmaNvmf) Connect() (string, error) {
	ctxTimeout, cancel := i.sma.SMActx()
	defer cancel()
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

	// Connect to device
	cmdLine := []string{
		"nvme", "connect", "-t", "tcp", "-a", "127.0.0.1", "-s", "4420", "-n", req.GetNvmfTcp().Subnqn,
	}
	err := execWithTimeout(cmdLine, 40)
	if err != nil {
		// go on checking device status in case caused by duplicated request
		klog.Errorf("command %v failed: %s", cmdLine, err)
	} else {
		klog.Infof("nvme connect succeeded!")
	}

	i.sma.deviceHandle = i.sma.CreateDevice(req)
	i.subnqn = req.GetNvmfTcp().Subnqn

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

	// deviceHandle := i.sma.SMACreateDevice()
	// TODO: if i.sma.deviceHandle is empty...

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
			klog.Errorf("Deleting subnqn failed: %s", err)
		} else {
			klog.Infof("Deleting subnqn succeeded! %s", detachRes.ProtoReflect())
		}
	}
	return errwaitForDeviceGone
}
