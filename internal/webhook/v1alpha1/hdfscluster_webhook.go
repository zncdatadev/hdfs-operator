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

// Package v1alpha1 holds the HdfsCluster admission webhooks.
package v1alpha1

import (
	"context"

	commonsv1alpha1 "github.com/zncdatadev/operator-go/pkg/apis/commons/v1alpha1"
	ctrl "sigs.k8s.io/controller-runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	hdfsv1alpha1 "github.com/zncdatadev/hdfs-operator/api/v1alpha1"
)

var hdfsclusterlog = logf.Log.WithName("hdfscluster-webhook")

// SetupHdfsClusterWebhookWithManager registers the HdfsCluster webhooks with the manager.
func SetupHdfsClusterWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr, &hdfsv1alpha1.HdfsCluster{}).
		WithDefaulter(&HdfsClusterCustomDefaulter{}).
		Complete()
}

// +kubebuilder:webhook:path=/mutate-hdfs-kubedoop-dev-v1alpha1-hdfscluster,mutating=true,failurePolicy=fail,sideEffects=None,groups=hdfs.kubedoop.dev,resources=hdfsclusters,verbs=create;update,versions=v1alpha1,name=mhdfscluster-v1alpha1.kb.io,admissionReviewVersions=v1

// HdfsClusterCustomDefaulter fills product defaults on an HdfsCluster at admission.
type HdfsClusterCustomDefaulter struct{}

// Default sets the image defaults (repo / product version / kubedoop version) when the user did
// not provide a fully custom image, so the CR is self-describing after admission.
func (d *HdfsClusterCustomDefaulter) Default(_ context.Context, obj *hdfsv1alpha1.HdfsCluster) error {
	hdfsclusterlog.Info("Defaulting HdfsCluster", "name", obj.GetName())

	if obj.Spec.Image == nil {
		obj.Spec.Image = &commonsv1alpha1.ImageSpec{}
	}
	if obj.Spec.Image.Custom != "" {
		return nil // a fully custom image overrides repo/version fields
	}
	if obj.Spec.Image.Repo == "" {
		obj.Spec.Image.Repo = hdfsv1alpha1.DefaultRepository
	}
	if obj.Spec.Image.ProductVersion == "" {
		obj.Spec.Image.ProductVersion = hdfsv1alpha1.DefaultProductVersion
	}
	if obj.Spec.Image.KubedoopVersion == "" {
		obj.Spec.Image.KubedoopVersion = hdfsv1alpha1.DefaultKubedoopVersion
	}
	return nil
}
