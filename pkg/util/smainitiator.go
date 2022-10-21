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

	spdkcsiConfig "github.com/spdk/spdk-csi/pkg/config"
)

func NewSpdkCsiSmaInitiator(volumeContext map[string]string) (SpdkCsiInitiator, error) {
	targetType := strings.ToLower(volumeContext["targetType"])
	klog.Infof("SMA in volumeContext, use legacy connection to %v", volumeContext["model"])
	isma := initiatorSMA{volumeContext: volumeContext}
	smaConfigString, ok := volumeContext["sma"]
	if !ok {
		return nil, fmt.Errorf("volumeContext does not include \"sma\"")
	}
	smaConfig := spdkcsiConfig.SmaConfig{}
	err := json.Unmarshal([]byte(smaConfigString), &smaConfig)
	if err != nil {
		return nil, fmt.Errorf("invalid SMA configuration: %q (%w)", smaConfigString, err)
	}
	isma.serverURL = smaConfig.Server
	switch targetType {
	case "tcp":
		isma.req = &smarpc.CreateDeviceRequest{
			Volume: nil,
			Params: &smarpc.CreateDeviceRequest_NvmfTcp{
				NvmfTcp: &nvmf_tcp.DeviceParameters{
					Subnqn:  volumeContext["nqn"],
					Adrfam:  "ipv4",
					Traddr:  volumeContext["targetAddr"],
					Trsvcid: "4420",
				},
			},
		}
	default:
		klog.Errorf("Unsupported SMA target type in %v", volumeContext)
		return nil, fmt.Errorf("unknown SMA target type: %q", volumeContext["targetType"])
	}
	return &isma, nil
}

type initiatorSMA struct {
	serverURL     string
	req           *smarpc.CreateDeviceRequest
	volumeContext map[string]string
}

func (sma *initiatorSMA) SMAClient() smarpc.StorageManagementAgentClient {
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

func (sma *initiatorSMA) SMAtovolUUID() []byte {
	volUUID := uuid.MustParse(sma.volumeContext["model"])
	volUUIDBytes, err := volUUID.MarshalBinary()
	if err != nil {
		klog.Errorf("volUUID.MarshalBinary() failed: %s", err)
	}
	return volUUIDBytes
}

func (sma *initiatorSMA) SMActx() (context.Context, context.CancelFunc) {
	ctxTimeout, cancel := context.WithTimeout(context.Background(), 42*time.Second)
	return ctxTimeout, cancel
}

func (sma *initiatorSMA) SMACreateDevice() string {
	ctxTimeout, cancel := sma.SMActx()
	defer cancel()
	// Create device
	response, err := sma.SMAClient().CreateDevice(ctxTimeout, sma.req)
	if err != nil {
		klog.Errorf("Creating device failed: %s", err)
	}
	klog.Infof("DELME: initiator Connect: CreateDevice response: %+v", response)

	return response.Handle
}

func (sma *initiatorSMA) Connect() (string, error) {
	ctxTimeout, cancel := sma.SMActx()
	defer cancel()
	// Connect to device
	cmdLine := []string{
		"nvme", "connect", "-t", "tcp", "-a", "127.0.0.1", "-s", "4420", "-n", sma.req.GetNvmfTcp().Subnqn,
	}
	err := execWithTimeout(cmdLine, 40)
	if err != nil {
		// go on checking device status in case caused by duplicated request
		klog.Errorf("command %v failed: %s", cmdLine, err)
	} else {
		klog.Infof("nvme connect succeeded!")
	}

	deviceHandle := sma.SMACreateDevice()

	// Attach volume
	attachReq := &smarpc.AttachVolumeRequest{
		Volume:       &smarpc.VolumeParameters{VolumeId: sma.SMAtovolUUID()},
		DeviceHandle: deviceHandle,
	}

	attachRes, err := sma.SMAClient().AttachVolume(ctxTimeout, attachReq)
	if err != nil {
		klog.Errorf("Attaching volume failed: %s", err)
	} else {
		klog.Infof("Attaching volume succeeded! %s", attachRes.ProtoReflect())
	}

	// Check the device path
	deviceGlob := fmt.Sprintf("/dev/disk/by-id/*%s*", sma.volumeContext["model"])
	devicePath, err := waitForDeviceReady(deviceGlob, 20)
	if err != nil {
		return "", err
	}
	klog.Infof("Device path is %s", devicePath)

	return devicePath, nil
}

func (sma *initiatorSMA) Disconnect() error {
	ctxTimeout, cancel := sma.SMActx()
	defer cancel()

	deviceHandle := sma.SMACreateDevice()

	// Detach volume
	detachReq := &smarpc.DetachVolumeRequest{
		VolumeId:     sma.SMAtovolUUID(),
		DeviceHandle: deviceHandle,
	}

	detachRes, err := sma.SMAClient().DetachVolume(ctxTimeout, detachReq)
	if err != nil {
		klog.Errorf("Detaching volume failed: %s", err)
	} else {
		klog.Infof("Detaching volume succeeded! %s", detachRes.ProtoReflect())
	}

	// Disconnect device
	cmdLine := []string{
		"nvme", "disconnect", "-n", sma.req.GetNvmfTcp().Subnqn,
	}
	err = execWithTimeout(cmdLine, 40)
	if err != nil {
		// go on checking device status in case caused by duplicated request
		klog.Errorf("command %v failed: %s", cmdLine, err)
	} else {
		klog.Infof("nvme disconnect succeeded!")
	}

	// Delete device
	deviceGlob := fmt.Sprintf("/dev/disk/by-id/*%s*", sma.volumeContext["model"])
	errwaitForDeviceGone := waitForDeviceGone(deviceGlob, 20)
	if errwaitForDeviceGone == nil {
		deleteReq := &smarpc.DeleteDeviceRequest{
			Handle: deviceHandle,
		}
		detachRes, err := sma.SMAClient().DeleteDevice(ctxTimeout, deleteReq)
		if err != nil {
			klog.Errorf("Deleting subnqn failed: %s", err)
		} else {
			klog.Infof("Deleting subnqn succeeded! %s", detachRes.ProtoReflect())
		}
	}
	return errwaitForDeviceGone
}
