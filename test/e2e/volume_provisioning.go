/*
Copyright 2016 The Kubernetes Authors.

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

package e2e

import (
	"time"


	gcli "github.com/heketi/heketi/client/api/go-client"
	gapi "github.com/heketi/heketi/pkg/glusterfs/api"

	"k8s.io/kubernetes/pkg/api"
	"k8s.io/kubernetes/pkg/api/resource"
	"k8s.io/kubernetes/pkg/api/unversioned"
	"k8s.io/kubernetes/pkg/apis/storage"
	client "k8s.io/kubernetes/pkg/client/unversioned"
	"k8s.io/kubernetes/test/e2e/framework"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

const (
	// Requested size of the volume
	requestedSize = "1500Mi"
	// Expected size of the volume is 2GiB, because all three supported cloud
	// providers allocate volumes in 1GiB chunks.
	expectedSize = "2Gi"
)

func testDynamicProvisioning(client *client.Client, claim *api.PersistentVolumeClaim) {
	err := framework.WaitForPersistentVolumeClaimPhase(api.ClaimBound, client, claim.Namespace, claim.Name, framework.Poll, framework.ClaimProvisionTimeout)
	Expect(err).NotTo(HaveOccurred())

	By("checking the claim")
	// Get new copy of the claim
	claim, err = client.PersistentVolumeClaims(claim.Namespace).Get(claim.Name)
	Expect(err).NotTo(HaveOccurred())

	// Get the bound PV
	pv, err := client.PersistentVolumes().Get(claim.Spec.VolumeName)
	Expect(err).NotTo(HaveOccurred())

	// Check sizes
	expectedCapacity := resource.MustParse(expectedSize)
	pvCapacity := pv.Spec.Capacity[api.ResourceName(api.ResourceStorage)]
	Expect(pvCapacity.Value()).To(Equal(expectedCapacity.Value()))

	requestedCapacity := resource.MustParse(requestedSize)
	claimCapacity := claim.Spec.Resources.Requests[api.ResourceName(api.ResourceStorage)]
	Expect(claimCapacity.Value()).To(Equal(requestedCapacity.Value()))

	// Check PV properties
	Expect(pv.Spec.PersistentVolumeReclaimPolicy).To(Equal(api.PersistentVolumeReclaimDelete))
	expectedAccessModes := []api.PersistentVolumeAccessMode{api.ReadWriteOnce}
	Expect(pv.Spec.AccessModes).To(Equal(expectedAccessModes))
	Expect(pv.Spec.ClaimRef.Name).To(Equal(claim.ObjectMeta.Name))
	Expect(pv.Spec.ClaimRef.Namespace).To(Equal(claim.ObjectMeta.Namespace))

	// We start two pods:
	// - The first writes 'hello word' to the /mnt/test (= the volume).
	// - The second one runs grep 'hello world' on /mnt/test.
	// If both succeed, Kubernetes actually allocated something that is
	// persistent across pods.
	By("checking the created volume is writable")
	runInPodWithVolume(client, claim.Namespace, claim.Name, "echo 'hello world' > /mnt/test/data")

	By("checking the created volume is readable and retains data")
	runInPodWithVolume(client, claim.Namespace, claim.Name, "grep 'hello world' /mnt/test/data")

	// Ugly hack: if we delete the AWS/GCE/OpenStack volume here, it will
	// probably collide with destruction of the pods above - the pods
	// still have the volume attached (kubelet is slow...) and deletion
	// of attached volume is not allowed by AWS/GCE/OpenStack.
	// Kubernetes *will* retry deletion several times in
	// pvclaimbinder-sync-period.
	// So, technically, this sleep is not needed. On the other hand,
	// the sync perion is 10 minutes and we really don't want to wait
	// 10 minutes here. There is no way how to see if kubelet is
	// finished with cleaning volumes. A small sleep here actually
	// speeds up the test!
	// Three minutes should be enough to clean up the pods properly.
	// We've seen GCE PD detach to take more than 1 minute.
	By("Sleeping to let kubelet destroy all pods")
	time.Sleep(3 * time.Minute)

	By("deleting the claim")
	framework.ExpectNoError(client.PersistentVolumeClaims(claim.Namespace).Delete(claim.Name))

	// Wait for the PV to get deleted too.
	framework.ExpectNoError(framework.WaitForPersistentVolumeDeleted(client, pv.Name, 5*time.Second, 20*time.Minute))
}

var _ = framework.KubeDescribe("Dynamic provisioning", func() {
	f := framework.NewDefaultFramework("volume-provisioning")

	// filled in BeforeEach
	var c *client.Client
	var ns string

	BeforeEach(func() {
		c = f.Client
		ns = f.Namespace.Name
	})

	framework.KubeDescribe("DynamicProvisioner", func() {
		It("should create and delete persistent volumes [Slow]", func() {
			// added until the GKE startup includes storage.k8s.io/v1beta1
			framework.SkipIfProviderIs("gke")
			framework.SkipUnlessProviderIs("openstack", "gce", "aws", "gke", "glusterfs")

			By("creating a StorageClass")
			class := newStorageClass()
			_, err := c.Storage().StorageClasses().Create(class)
			defer c.Storage().StorageClasses().Delete(class.Name)
			Expect(err).NotTo(HaveOccurred())

			By("creating a claim with a dynamic provisioning annotation")
			claim := newClaim(ns, false)
			defer func() {
				c.PersistentVolumeClaims(ns).Delete(claim.Name)
			}()
			claim, err = c.PersistentVolumeClaims(ns).Create(claim)
			Expect(err).NotTo(HaveOccurred())

			testDynamicProvisioning(c, claim)
		})
	})

	framework.KubeDescribe("DynamicProvisioner Alpha", func() {
		It("should create and delete alpha persistent volumes [Slow]", func() {
			framework.SkipUnlessProviderIs("openstack", "gce", "aws", "gke", "glusterfs")

			By("creating a claim with an alpha dynamic provisioning annotation")
			claim := newClaim(ns, true)
			defer func() {
				c.PersistentVolumeClaims(ns).Delete(claim.Name)
			}()
			claim, err := c.PersistentVolumeClaims(ns).Create(claim)
			Expect(err).NotTo(HaveOccurred())

			testDynamicProvisioning(c, claim)
		})
	})
})

func newClaim(ns string, alpha bool) *api.PersistentVolumeClaim {
	claim := api.PersistentVolumeClaim{
		ObjectMeta: api.ObjectMeta{
			GenerateName: "pvc-",
			Namespace:    ns,
		},
		Spec: api.PersistentVolumeClaimSpec{
			AccessModes: []api.PersistentVolumeAccessMode{
				api.ReadWriteOnce,
			},
			Resources: api.ResourceRequirements{
				Requests: api.ResourceList{
					api.ResourceName(api.ResourceStorage): resource.MustParse(requestedSize),
				},
			},
		},
	}

	if alpha {
		claim.Annotations = map[string]string{
			"volume.alpha.kubernetes.io/storage-class": "",
		}
	} else {
		claim.Annotations = map[string]string{
			"volume.beta.kubernetes.io/storage-class": "fast",
		}

	}

	return &claim
}

// runInPodWithVolume runs a command in a pod with given claim mounted to /mnt directory.
func runInPodWithVolume(c *client.Client, ns, claimName, command string) {
	pod := &api.Pod{
		TypeMeta: unversioned.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: api.ObjectMeta{
			GenerateName: "pvc-volume-tester-",
		},
		Spec: api.PodSpec{
			Containers: []api.Container{
				{
					Name:    "volume-tester",
					Image:   "gcr.io/google_containers/busybox:1.24",
					Command: []string{"/bin/sh"},
					Args:    []string{"-c", command},
					VolumeMounts: []api.VolumeMount{
						{
							Name:      "my-volume",
							MountPath: "/mnt/test",
						},
					},
				},
			},
			RestartPolicy: api.RestartPolicyNever,
			Volumes: []api.Volume{
				{
					Name: "my-volume",
					VolumeSource: api.VolumeSource{
						PersistentVolumeClaim: &api.PersistentVolumeClaimVolumeSource{
							ClaimName: claimName,
							ReadOnly:  false,
						},
					},
				},
			},
		},
	}
	pod, err := c.Pods(ns).Create(pod)
	defer func() {
		framework.ExpectNoError(c.Pods(ns).Delete(pod.Name, nil))
	}()
	framework.ExpectNoError(err, "Failed to create pod: %v", err)
	framework.ExpectNoError(framework.WaitForPodSuccessInNamespaceSlow(c, pod.Name, pod.Spec.Containers[0].Name, pod.Namespace))
}

func newStorageClass() *storage.StorageClass {
	var pluginName string

	switch {
	case framework.ProviderIs("gke"), framework.ProviderIs("gce"):
		pluginName = "kubernetes.io/gce-pd"
	case framework.ProviderIs("aws"):
		pluginName = "kubernetes.io/aws-ebs"
	case framework.ProviderIs("openstack"):
		pluginName = "kubernetes.io/cinder"
	case framework.ProviderIs("glusterfs"):
		pluginName = "kubernetes.io/glusterfs"
	}

	return &storage.StorageClass{
		TypeMeta: unversioned.TypeMeta{
			Kind: "StorageClass",
		},
		ObjectMeta: api.ObjectMeta{
			Name: "fast",
		},
		Provisioner: pluginName,
	}
}

func startGlusterDpServerPod() {

	f := framework.NewDefaultFramework("volume-provisioning")


	// If 'false', the test won't clear its volumes upon completion. Useful for debugging,
	// note that namespace deletion is handled by delete-namespace flag
	clean := true
	// filled in BeforeEach
	var c *client.Client
	var namespace *api.Namespace

	BeforeEach(func() {
		c = f.Client
		namespace = f.Namespace
	})

	config := VolumeTestConfig{
				namespace:   namespace.Name,
				prefix:      "gluster",
				serverImage: "gcr.io/google_containers/volume-manager-gluster:0.1",
				serverPorts: []int{8081,},
			}
	pod := startVolumeServer(c, config)
	serverIP := pod.Status.PodIP
	framework.Logf("Gluster volume manager server IP address: %v", serverIP)

	//Contact the volume manager server, then create/delete volumes

	cli := gcli.NewClient("http://"+serverIP+":8081", "","")
	if cli == nil {
			framework.Errorf("glusterfs: failed to create gluster rest client")
			return
	}
	sz:= 10
	replicacount := 3
	volumeReq := &gapi.VolumeCreateRequest{Size: sz, Durability: gapi.VolumeDurabilityInfo{Type: durabilitytype, Replicate: gapi.ReplicaDurability{Replica: replicacount}}}
	volume, err := cli.VolumeCreate(volumeReq)
	if err != nil {
		framework.Errorf("glusterfs: error creating volume %s ", err)
		return
	}
	framework.Infof("glusterfs: volume with size :%d and name:%s created", volume.Size, volume.Name)
	err = cli.VolumeDelete(volume.Id)
	if err != nil {
		framework.V(4).Infof("glusterfs: error when deleting the volume :%s", err)
		return err
	}
	framework.Infof("glusterfs: volume %s deleted successfully", volume.Id)
}



// Starts a container specified by config.serverImage and exports all
// config.serverPorts from it. The returned pod should be used to get the server
// IP address and create appropriate VolumeSource.
func startVolumeServer(client *client.Client, config VolumeTestConfig) *api.Pod {
	podClient := client.Pods(config.namespace)

	portCount := len(config.serverPorts)
	serverPodPorts := make([]api.ContainerPort, portCount)

	for i := 0; i < portCount; i++ {
		portName := fmt.Sprintf("%s-%d", config.prefix, i)

		serverPodPorts[i] = api.ContainerPort{
			Name:          portName,
			ContainerPort: int32(config.serverPorts[i]),
			Protocol:      api.ProtocolTCP,
		}
	}

	volumeCount := len(config.volumes)
	volumes := make([]api.Volume, volumeCount)
	mounts := make([]api.VolumeMount, volumeCount)

	i := 0
	for src, dst := range config.volumes {
		mountName := fmt.Sprintf("path%d", i)
		volumes[i].Name = mountName
		volumes[i].VolumeSource.HostPath = &api.HostPathVolumeSource{
			Path: src,
		}

		mounts[i].Name = mountName
		mounts[i].ReadOnly = false
		mounts[i].MountPath = dst

		i++
	}

	By(fmt.Sprint("creating ", config.prefix, " server pod"))
	privileged := new(bool)
	*privileged = true
	serverPod := &api.Pod{
		TypeMeta: unversioned.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: api.ObjectMeta{
			Name: config.prefix + "-server",
			Labels: map[string]string{
				"role": config.prefix + "-server",
			},
		},

		Spec: api.PodSpec{
			Containers: []api.Container{
				{
					Name:  config.prefix + "-server",
					Image: config.serverImage,
					SecurityContext: &api.SecurityContext{
						Privileged: privileged,
					},
					Args:         config.serverArgs,
					Ports:        serverPodPorts,
					VolumeMounts: mounts,
				},
			},
			Volumes: volumes,
		},
	}
	serverPod, err := podClient.Create(serverPod)
	framework.ExpectNoError(err, "Failed to create %s pod: %v", serverPod.Name, err)

	framework.ExpectNoError(framework.WaitForPodRunningInNamespace(client, serverPod))

	By("locating the server pod")
	pod, err := podClient.Get(serverPod.Name)
	framework.ExpectNoError(err, "Cannot locate the server pod %v: %v", serverPod.Name, err)

	By("sleeping a bit to give the server time to start")
	time.Sleep(20 * time.Second)
	return pod
}