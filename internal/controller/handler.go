/*
Copyright 2024 zncdatadev.

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

package controller

import (
	"context"
	"fmt"

	"github.com/zncdatadev/operator-go/pkg/config"
	"github.com/zncdatadev/operator-go/pkg/reconciler"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	hdfsv1alpha1 "github.com/zncdatadev/hdfs-operator/api/v1alpha1"
	"github.com/zncdatadev/hdfs-operator/internal/constants"
)

// HdfsRoleGroupHandler builds HDFS role group resources. It embeds the SDK
// BaseRoleGroupHandler so the framework owns resource orchestration — the ConfigMap (rendered
// from the merged config, including the product config from product.ComputeConfig), Services,
// the StatefulSet, and the PDB.
//
// NOTE (skeleton): the product-specific pieces HDFS needs beyond the framework defaults —
// ZKFC sidecar, format-namenode / format-zk / wait-for-namenodes init containers, the
// discovery ConfigMap, Kerberos/TLS volumes — are reintroduced in later refactor phases via a
// BuildResources override and the SDK's declarative provisioners.
type HdfsRoleGroupHandler struct {
	*reconciler.BaseRoleGroupHandler[*hdfsv1alpha1.HdfsCluster]
}

// NewHdfsRoleGroupHandler creates the handler and configures the framework defaults for the
// three HDFS roles.
func NewHdfsRoleGroupHandler(scheme *runtime.Scheme) *HdfsRoleGroupHandler {
	base := reconciler.NewBaseRoleGroupHandler[*hdfsv1alpha1.HdfsCluster](defaultImage(), scheme)

	// core-site.xml / hdfs-site.xml are rendered as Hadoop XML by the default formats.
	base.ConfigGenerator = config.NewMultiFormatConfigGenerator()
	base.ConfigGenerator.RegisterDefaultFormats()

	// HDFS reads its config from the Hadoop config dir.
	base.ConfigMountPath = hdfsv1alpha1.HadoopHome + "/etc/hadoop"

	setRolePorts(base)

	return &HdfsRoleGroupHandler{BaseRoleGroupHandler: base}
}

// setRolePorts declares the container/service ports for each role.
func setRolePorts(base *reconciler.BaseRoleGroupHandler[*hdfsv1alpha1.HdfsCluster]) {
	rolePorts := map[string][]struct {
		name string
		port int32
	}{
		hdfsv1alpha1.NameNodeRoleName: {
			{hdfsv1alpha1.RpcName, hdfsv1alpha1.NameNodeRpcPort},
			{hdfsv1alpha1.HttpName, hdfsv1alpha1.NameNodeHttpPort},
		},
		hdfsv1alpha1.DataNodeRoleName: {
			{hdfsv1alpha1.DataName, hdfsv1alpha1.DataNodeDataPort},
			{hdfsv1alpha1.HttpName, hdfsv1alpha1.DataNodeHttpPort},
			{hdfsv1alpha1.IpcName, hdfsv1alpha1.DataNodeIpcPort},
		},
		hdfsv1alpha1.JournalNodeRoleName: {
			{hdfsv1alpha1.RpcName, hdfsv1alpha1.JournalNodeRpcPort},
			{hdfsv1alpha1.HttpName, hdfsv1alpha1.JournalNodeHttpPort},
		},
	}

	for role, ports := range rolePorts {
		containerPorts := make([]corev1.ContainerPort, 0, len(ports))
		servicePorts := make([]corev1.ServicePort, 0, len(ports))
		for _, p := range ports {
			containerPorts = append(containerPorts, corev1.ContainerPort{
				Name:          p.name,
				ContainerPort: p.port,
				Protocol:      corev1.ProtocolTCP,
			})
			servicePorts = append(servicePorts, corev1.ServicePort{
				Name:     p.name,
				Port:     p.port,
				Protocol: corev1.ProtocolTCP,
			})
		}
		base.SetRoleContainerPorts(role, containerPorts)
		base.SetRoleServicePorts(role, servicePorts)
	}
}

// BuildResources delegates to the framework. Product-specific resources are reintroduced here
// in later phases (see type doc).
func (h *HdfsRoleGroupHandler) BuildResources(
	ctx context.Context,
	k8sClient client.Client,
	cr *hdfsv1alpha1.HdfsCluster,
	buildCtx *reconciler.RoleGroupBuildContext,
) (*reconciler.RoleGroupResources, error) {
	resources, err := h.BaseRoleGroupHandler.BuildResources(ctx, k8sClient, cr, buildCtx)
	if err != nil {
		return nil, err
	}

	// The image is declared in the CR spec and resolved with the product name; override the
	// framework's operator-default image on the StatefulSet when the user set spec.image.
	if cr.Spec.Image != nil && resources.StatefulSet != nil {
		image := cr.Spec.Image.GetImage(constants.ProductName)
		if image != "" {
			containers := resources.StatefulSet.Spec.Template.Spec.Containers
			for i := range containers {
				containers[i].Image = image
				containers[i].ImagePullPolicy = cr.Spec.Image.GetPullPolicy()
			}
		}
	}

	return resources, nil
}

// defaultImage is the operator's default HDFS image. The CR's spec.image overrides it per
// reconcile in BuildResources.
func defaultImage() string {
	return fmt.Sprintf("%s/%s:%s-kubedoop%s",
		constants.DefaultImageRepo,
		constants.ProductName,
		constants.DefaultProductVersion,
		constants.DefaultKubedoopVersion,
	)
}

// Ensure interface implementation.
var _ reconciler.RoleGroupHandler[*hdfsv1alpha1.HdfsCluster] = &HdfsRoleGroupHandler{}
