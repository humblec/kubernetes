/*
Copyright 2021 The Kubernetes Authors.
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
	"k8s.io/klog/v2"
	"strings"

	"k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	RbdVolumePluginName = "kubernetes.io/rbd"
	RbdDriverName       = "rbd.csi.ceph.com"
	defaultClusterID = "rook-ceph"
	defaultAdminSecretName = "csi-rbd-secret"
	defaultAdminSecretNamespace = "default"
	defaultPool = "replicapool"
)

var _ InTreePlugin = &rbdCSITranslator{}

type rbdCSITranslator struct{}

func NewRbdCSITranslator() InTreePlugin {
	return &rbdCSITranslator{}
}

// TranslateInTreeStorageClassToCSI takes in-tree storage class used by in-tree plugin
// and translates them to a storageclass consumable by CSI plugin
func (p rbdCSITranslator) TranslateInTreeStorageClassToCSI(sc *storagev1.StorageClass) (*storagev1.StorageClass, error) {
	if sc == nil {
		return nil, fmt.Errorf("sc is nil")
	}
	var params = map[string]string{}
	for k, v := range sc.Parameters {
		switch strings.ToLower(k) {
		case fsTypeKey:
			params[csiFsTypeKey] = v
		case "imagefeatures":
			params["imageFeatures"] = v
		case "pool":
			params["pool"] = v
		case "imageformat":
			params["imageFormat"] = v
		case "monitors":
			params["monitors"] = v
		default:
			klog.V(2).Infof("StorageClass parameter [name:%q, value:%q] is not supported", k, v)
		}
	}
	// fill extra
	params["clusterID"] = defaultClusterID
	sc.Provisioner = RbdDriverName

	// extra
	params["csi.storage.k8s.io/provisioner-secret-name"] = defaultAdminSecretName
	params["csi.storage.k8s.io/provisioner-secret-namespace"] = defaultAdminSecretNamespace
	params["csi.storage.k8s.io/controller-expand-secret-name"] = defaultAdminSecretName
	params["csi.storage.k8s.io/controller-expand-secret-namespace"] = defaultAdminSecretNamespace
	params["csi.storage.k8s.io/node-stage-secret-name"] = defaultAdminSecretName
	params["csi.storage.k8s.io/node-stage-secret-namespace"] = defaultAdminSecretNamespace

	sc.Parameters = params
	return sc, nil
}

// TranslateInTreeInlineVolumeToCSI takes a inline volume and will translate
// the in-tree inline volume source to a CSIPersistentVolumeSource
func (p rbdCSITranslator) TranslateInTreeInlineVolumeToCSI(volume *v1.Volume, podNamespace string) (*v1.PersistentVolume, error) {
	if volume == nil || volume.RBD == nil {
		return nil, fmt.Errorf("volume is nil or RBDVolume not defined on volume")
	}

	var am v1.PersistentVolumeAccessMode
	if volume.RBD.ReadOnly {
		am = v1.ReadOnlyMany
	} else {
		am = v1.ReadWriteOnce
	}

	pv := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
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
			AccessModes: []v1.PersistentVolumeAccessMode{am},
		},
	}
	return pv, nil
}

// TranslateInTreePVToCSI takes a RBD persistent volume and will translate
// the in-tree pv source to a CSI Source
func (p rbdCSITranslator) TranslateInTreePVToCSI(pv *v1.PersistentVolume) (*v1.PersistentVolume, error) {
	if pv == nil || pv.Spec.RBD == nil {
		return nil, fmt.Errorf("pv is nil or RBD Volume not defined on pv")
	}

 volumeAttributes := make(map[string]string)
 volumeAttributes["clusterID"] = defaultClusterID
	volumeAttributes["pool"] = defaultPool
	volumeAttributes["staticVolume"] = "true"
	volumeAttributes["imageFeatures"] = "layering"
	csiSource := &v1.CSIPersistentVolumeSource{
		Driver:           RbdDriverName,
		FSType:           pv.Spec.RBD.FSType,
		NodeStageSecretRef: &v1.SecretReference{
			Name:      defaultAdminSecretName,
			Namespace: defaultAdminSecretNamespace,
		},
		VolumeHandle:     pv.Spec.RBD.RBDImage,
		VolumeAttributes: volumeAttributes,
	// copy access mode
	}
	pv.Spec.RBD = nil
	pv.Spec.CSI = csiSource

	return pv, nil
}

// TranslateCSIPVToInTree takes a PV with a CSI PersistentVolume Source and will translate
// it to a in-tree Persistent Volume Source for the in-tree volume
func (p rbdCSITranslator) TranslateCSIPVToInTree(pv *v1.PersistentVolume) (*v1.PersistentVolume, error) {
	if pv == nil || pv.Spec.CSI == nil {
		return nil, fmt.Errorf("pv is nil or CSI source not defined on pv")
	}
	csiSource := pv.Spec.CSI

	// check for volumeHandle

	inVolID := ""
	if csiSource.VolumeHandle != "" {
		klog.V(2).Infof("volume handle field is non nil (%q) in this CSI pv", csiSource.VolumeHandle)
		if csiSource.VolumeAttributes["imageName"] != "" {
			klog.V(2).Infof("rbd image (%q) in this CSI pv", csiSource.VolumeAttributes["imageName"])
			inVolID = csiSource.VolumeAttributes["imageName"]
		}
	}
	RBDSource := &v1.RBDPersistentVolumeSource{
		RBDImage: inVolID,
		FSType:   csiSource.FSType,
		ReadOnly: csiSource.ReadOnly,
		CephMonitors: []string{},
	}
	pv.Spec.CSI = nil
	pv.Spec.RBD = RBDSource

	return pv, nil
}

// CanSupport tests whether the plugin supports a given persistent volume
// specification from the API.
func (p rbdCSITranslator) CanSupport(pv *v1.PersistentVolume) bool {
	return pv != nil && pv.Spec.RBD != nil
}

// CanSupportInline tests whether the plugin supports a given inline volume
// specification from the API.
func (p rbdCSITranslator) CanSupportInline(volume *v1.Volume) bool {
	return volume != nil && volume.RBD != nil
}

// GetInTreePluginName returns the in-tree plugin name this migrates
func (p rbdCSITranslator) GetInTreePluginName() string {
	return RbdVolumePluginName
}

// GetCSIPluginName returns the name of the CSI plugin that supersedes the in-tree plugin
func (p rbdCSITranslator) GetCSIPluginName() string {
	return RbdDriverName
}

// RepairVolumeHandle generates a correct volume handle based on node ID information.
func (p rbdCSITranslator) RepairVolumeHandle(volumeHandle, nodeID string) (string, error) {
	return volumeHandle, nil
}