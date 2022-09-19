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
	"fmt"
	"sync"

	"k8s.io/klog"
)

type nodeSMA struct {
	client *rpcClient

	targetAddr string
	targetPort string

	lvols map[string]*lvolSMA
	mtx   sync.Mutex // for concurrent access to lvols map
}

type lvolSMA struct {
	cfg string
}

func newSMA(client *rpcClient, targetAddr string) *nodeSMA {
	return &nodeSMA{
		client:     client,
		targetAddr: targetAddr,
		targetPort: cfgSMASvcPort,
		lvols:      make(map[string]*lvolSMA),
	}
}

func (node *nodeSMA) Info() string {
	return "TODO: node.client.info() or similar"
}

func (node *nodeSMA) LvStores() ([]LvStore, error) {
	return []LvStore{}, fmt.Errorf("LvStores not implemented: node.client.lvStores() or similar")
}

// VolumeInfo returns a string:string map containing information necessary
// for CSI node(initiator) to connect to this target and identify the disk.
func (node *nodeSMA) VolumeInfo(lvolID string) (map[string]string, error) {
	node.mtx.Lock()
	lvol, exists := node.lvols[lvolID]
	node.mtx.Unlock()

	if !exists {
		return nil, fmt.Errorf("volume not exists: %s", lvolID)
	}

	return map[string]string{
		"targetAddr": node.targetAddr,
		"targetPort": node.targetPort,
		"cfg":        lvol.cfg,
	}, nil
}

// CreateVolume creates a logical volume and returns volume ID
func (node *nodeSMA) CreateVolume(lvsName string, sizeMiB int64) (string, error) {
	klog.Errorf("nodeSMA.CreateVolume(%q, %d) not implemented\n", lvsName, sizeMiB)
	return "", fmt.Errorf("nodeSMA.CreateVolume(%q, %d) not implemented error", lvsName, sizeMiB)
}

func (node *nodeSMA) CreateSnapshot(lvolName, snapshotName string) (string, error) {
	klog.Errorf("nodeSMA.CreateSnapshot(%q, %q) not implemented\n", lvolName, snapshotName)
	return "", fmt.Errorf("nodeSMA.CreateSnapshot(%q, %q) not implemented error", lvolName, snapshotName)
	/*
		snapshotID, err := node.client.snapshot(lvolName, snapshotName)
		if err != nil {
			return "", err
		}

		klog.V(5).Infof("snapshot created: %s", snapshotID)
		return snapshotID, nil
	*/
}

func (node *nodeSMA) DeleteVolume(lvolID string) error {
	klog.Errorf("nodeSMA.DeleteVolume(%q) not implemented\n", lvolID)
	return fmt.Errorf("nodeSMA.DeleteVolume(%q) not implemented error", lvolID)
	/*
		err := node.client.deleteVolume(lvolID)
		if err != nil {
			return err
		}

		node.mtx.Lock()
		defer node.mtx.Unlock()

		delete(node.lvols, lvolID)

		klog.V(5).Infof("volume deleted: %s", lvolID)
		return nil
	*/
}

// PublishVolume exports a volume through SMA target
func (node *nodeSMA) PublishVolume(lvolID string) error {
	klog.Errorf("nodeSMA.PublishVolume(%q) not implemented\n", lvolID)
	return fmt.Errorf("nodeSMA.PublishVolume(%q) not implemented error", lvolID)
	/*
		var err error

		err = node.createTransport()
		if err != nil {
			return err
		}

		node.mtx.Lock()
		lvol, exists := node.lvols[lvolID]
		node.mtx.Unlock()

		if !exists {
			return ErrVolumeDeleted
		}
		if lvol.nqn != "" {
			return ErrVolumePublished
		}

		// cleanup lvol on error
		defer func() {
			if err != nil {
				lvol.reset()
			}
		}()

		lvol.model = lvolID
		lvol.nqn, err = node.createSubsystem(lvol.model)
		if err != nil {
			return err
		}

		lvol.nsID, err = node.subsystemAddNs(lvol.nqn, lvolID)
		if err != nil {
			node.deleteSubsystem(lvol.nqn) //nolint:errcheck // we can do few
			return err
		}

		err = node.subsystemAddListener(lvol.nqn)
		if err != nil {
			node.subsystemRemoveNs(lvol.nqn, lvol.nsID) //nolint:errcheck // ditto
			node.deleteSubsystem(lvol.nqn)              //nolint:errcheck // ditto
			return err
		}

		klog.V(5).Infof("volume published: %s", lvolID)
		return nil
	*/
}

func (node *nodeSMA) UnpublishVolume(lvolID string) error {
	klog.Errorf("nodeSMA.UnpublishVolume(%q) not implemented\n", lvolID)
	return fmt.Errorf("nodeSMA.UnpublishVolume(%q) not implemented error", lvolID)
	/*
		var err error

		node.mtx.Lock()
		lvol, exists := node.lvols[lvolID]
		node.mtx.Unlock()

		if !exists {
			return ErrVolumeDeleted
		}
		if lvol.nqn == "" {
			return ErrVolumeUnpublished
		}

		err = node.subsystemRemoveNs(lvol.nqn, lvol.nsID)
		if err != nil {
			// we should try deleting subsystem even if we fail here
			klog.Errorf("failed to remove namespace(nqn=%s, nsid=%d): %s", lvol.nqn, lvol.nsID, err)
		} else {
			lvol.nsID = invalidNSID
		}

		err = node.deleteSubsystem(lvol.nqn)
		if err != nil {
			return err
		}

		lvol.reset()
		klog.V(5).Infof("volume unpublished: %s", lvolID)
		return nil
	*/
}
