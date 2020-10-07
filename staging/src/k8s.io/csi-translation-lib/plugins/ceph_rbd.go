package plugins

import (
	"fmt"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubernetes/pkg/apis/storage"
	"strings"
)

/*
Copyright 2020 The Kubernetes Authors.

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

package plugins

import (
"fmt"
"strings"

v1 "k8s.io/api/core/v1"
storage "k8s.io/api/storage/v1"
metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
"k8s.io/klog/v2"
)

const (
	// RbdDriverName is the name of the CSI driver for RBD Volume
	RbdDriverName = "rbd.csi.ceph.com"

	// RBDInTreePluginName is the name of the in-tree plugin for RBD Volume
	RbdInTreePluginName = "kubernetes.io/rbd"

	// This param is used to tell Driver to return volumePath and not VolumeID
	// in-tree RBD plugin does not understand volume id, it uses volumePath
	paramcsiRbdMigration = "csimigration"

	paramPool = "pool-migrationparam"

	paramImageFormat = "imageformat-migrationparam"

	paramImageFeatures = "imagefeatures-migrationparam"

	paramUserSecretName      = "usersecretname-migrationparam"

	paramUserSecretNamespace       = "usersecretnamespace-migrationparam"

	paramAdminSecretName            = "adminSecret-migrationparam"

	paramAdminSecretNamespace = "adminsecretnamespace-migrationparam"

	paramMonitors              = "monitors-migrationparam"

	// AttributeInitialVolumeFilepath represents the path of volume where volume is created
	AttributeInitialRbdVolume = "initialrbdvolume"
)

var _ InTreePlugin = &rbdCSITranslator{}

// rbdCSITranslator handles translation of PV spec from In-tree RBD Volume to RBD CSI
type rbdCSITranslator struct{}

// NewRbdCSITranslator returns a new instance of rbdCSITranslator
func NewRbdCSITranslator() InTreePlugin {
	return &rbdCSITranslator{}
}

// TranslateInTreeStorageClassToCSI translates InTree RBD storage class parameters to CSI storage class
func (r rbdCSITranslator) TranslateInTreeStorageClassToCSI(sc *storagev1.StorageClass) (*storagev1.StorageClass, error) {

	if sc == nil {
		return nil, fmt.Errorf("sc is nil")
	}
	var params = map[string]string{}
	for k, v := range sc.Parameters {
		switch strings.ToLower(k) {
		case fsTypeKey:
			params[csiFsTypeKey] = v
		case "pool":
			params[paramPool] = v
		case "imageformat":
			params[paramImageFormat] = v
		case "imagefeatures":
			params[paramImageFeatures] = v
		case "usersecretname":
			params[paramUserSecretName] = v
		case "usersecretnamespace":
			params[paramUserSecretNamespace] = v
		case "adminsecretname":
			params[paramAdminSecretName] = v
		case "adminsecretnamespace":
			params[paramAdminSecretNamespace] = v
		case "monitors":
			params[paramMonitors] = v
		// todo: consider `userid` and `adminid`
		default:
			klog.V(2).Infof("StorageClass parameter [name:%q, value:%q] is not supported", k, v)
		}
	}

	// This helps RBD CSI driver to identify in-tree provisioner request vs CSI provisioner request
	// When this is true, Driver returns initialrbdvolume in the VolumeContext, which is
	// used in TranslateCSIPVToInTree
	params[paramcsiRbdMigration] = "true"
	// Note: sc.AllowedTopologies for Topology based volume provisioning will be supplied as it is.
	sc.Parameters = params
	return sc, nil

}

// TranslateInTreeInlineVolumeToCSI takes a Volume with RBDVolume set from in-tree
// and converts the RBDVolume source to a CSIPersistentVolumeSource
func (r rbdCSITranslator) TranslateInTreeInlineVolumeToCSI(volume *v1.Volume) (*v1.PersistentVolume, error) {
	if volume == nil || volume.RBD == nil {
		return nil, fmt.Errorf("volume is nil or RBD not defined on volume")
	}
	pv := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			// Must be unique per RBD image as it is used as the unique part of the
			// staging path
			Name: fmt.Sprintf("%s-%s", RbdDriverName, volume.RBD.RBDImage),
		},
		Spec: v1.PersistentVolumeSpec{
			PersistentVolumeSource: v1.PersistentVolumeSource{
				CSI: &v1.CSIPersistentVolumeSource{
					Driver:           RbdDriverName,
					VolumeHandle:     volume.RBD.RBDImage,
					FSType:           volume.RBD.FSType,
					VolumeAttributes: make(map[string]string),
				},
			},
			AccessModes: []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce},
		},
	}

	return pv, nil
}

// TranslateInTreePVToCSI takes a PV with RBDVolume set from in-tree
// and converts the RBDVolume source to a CSIPersistentVolumeSource
func (r rbdCSITranslator) TranslateInTreePVToCSI(pv *v1.PersistentVolume) (*v1.PersistentVolume, error) {
	if pv == nil || pv.Spec.RBD == nil {
		return nil, fmt.Errorf("pv is nil or VsphereVolume not defined on pv")
	}
	// todo: Derive VolumeHandle
	csiSource := &v1.CSIPersistentVolumeSource{
		Driver:           RbdDriverName,
		VolumeHandle:     pv.Spec.RBD.RBDImage,
		FSType:           pv.Spec.RBD.FSType,
		VolumeAttributes: make(map[string]string),
	}
	pv.Spec.RBD = nil
	pv.Spec.CSI = csiSource
	return pv, nil
}

// TranslateCSIPVToInTree takes a PV with CSIPersistentVolumeSource set and
// translates the RBD CSI source to a RBD source.
func (r rbdCSITranslator) TranslateCSIPVToInTree(pv *v1.PersistentVolume) (*v1.PersistentVolume, error) {

	if pv == nil || pv.Spec.CSI == nil {
		return nil, fmt.Errorf("pv is nil or CSI source not defined on pv")
	}
	csiSource := pv.Spec.CSI

	rbdVolumeSource := &v1.RBDPersistentVolumeSource{
		FSType: csiSource.FSType,
	}
	volumeFilePath, ok := csiSource.VolumeAttributes[AttributeInitialVolumeFilepath]
	if ok {
		rbdVolumeSource.RBDImage = volumeFilePath
	}
	pv.Spec.CSI = nil
	pv.Spec.RBD = rbdVolumeSource
	return pv, nil
}

// CanSupport tests whether the plugin supports a given persistent volume
// specification from the API.
func (r rbdCSITranslator) CanSupport(pv *v1.PersistentVolume) bool {
	return pv != nil && pv.Spec.RBD != nil

}

// CanSupportInline tests whether the plugin supports a given inline volume
// specification from the API.
func (r rbdCSITranslator) CanSupportInline(volume *v1.Volume) bool {
	return volume != nil && volume.RBD != nil

}

// GetInTreePluginName returns the name of the in-tree plugin driver
func (r rbdCSITranslator) GetInTreePluginName() string {
	return RbdInTreePluginName
}

// GetCSIPluginName returns the name of the CSI plugin
func (r rbdCSITranslator) GetCSIPluginName() string {
	return RbdDriverName
}

// RepairVolumeHandle is needed in VerifyVolumesAttached on the external attacher when we need to do strict volume
// handle matching to check VolumeAttachment attached status.
// RBD volume does not need patch to help verify whether that volume is attached.
func (r rbdCSITranslator) RepairVolumeHandle(volumeHandle, nodeID string) (string, error) {
	return volumeHandle, nil
}
