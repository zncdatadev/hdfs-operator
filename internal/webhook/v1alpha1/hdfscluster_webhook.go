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

// Package v1alpha1 holds the HdfsCluster admission webhook.
package v1alpha1

import (
	"context"
	"fmt"
	"reflect"
	"regexp"

	"github.com/zncdatadev/operator-go/pkg/webhook"
	ctrl "sigs.k8s.io/controller-runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	hdfsv1alpha1 "github.com/zncdatadev/hdfs-operator/api/v1alpha1"
	"github.com/zncdatadev/hdfs-operator/internal/constants"
)

var hdfsclusterlog = logf.Log.WithName("hdfscluster-webhook")

// SetupHdfsClusterWebhookWithManager registers the HdfsCluster validating webhook.
//
// There is no defaulting webhook: the only thing that ever needed defaulting was spec.image, and
// webhook defaults are persisted into the spec at admission and never recomputed — so a
// kubedoopVersion written here would freeze every cluster on the operator version that first
// admitted it. The handler's ImageDefaults fills those fields on every reconcile instead
// (operator-go #581); this webhook only validates.
func SetupHdfsClusterWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr, &hdfsv1alpha1.HdfsCluster{}).
		WithValidator(&HdfsClusterCustomValidator{}).
		Complete()
}

// +kubebuilder:webhook:path=/validate-hdfs-kubedoop-dev-v1alpha1-hdfscluster,mutating=false,failurePolicy=fail,sideEffects=None,groups=hdfs.kubedoop.dev,resources=hdfsclusters,verbs=create;update,versions=v1alpha1,name=vhdfscluster-v1alpha1.kb.io,admissionReviewVersions=v1

// HdfsClusterCustomValidator validates an HdfsCluster on create and update.
type HdfsClusterCustomValidator struct{}

// ValidateCreate validates a newly created HdfsCluster.
func (v *HdfsClusterCustomValidator) ValidateCreate(_ context.Context, obj *hdfsv1alpha1.HdfsCluster) (admission.Warnings, error) {
	hdfsclusterlog.Info("validate create", "name", obj.GetName())
	errs := v.validate(obj)
	if errs.HasErrors() {
		return nil, errs
	}
	return nil, nil
}

// ValidateUpdate validates an updated HdfsCluster; spec.image is immutable once set.
func (v *HdfsClusterCustomValidator) ValidateUpdate(_ context.Context, oldObj, newObj *hdfsv1alpha1.HdfsCluster) (admission.Warnings, error) {
	hdfsclusterlog.Info("validate update", "name", newObj.GetName())
	errs := v.validate(newObj)
	if oldObj.Spec.Image != nil && !reflect.DeepEqual(oldObj.Spec.Image, newObj.Spec.Image) {
		errs.Add("spec.image", "image cannot be changed after creation")
	}
	if errs.HasErrors() {
		return nil, errs
	}
	return nil, nil
}

// ValidateDelete does nothing (no delete-time validation).
func (v *HdfsClusterCustomValidator) ValidateDelete(_ context.Context, obj *hdfsv1alpha1.HdfsCluster) (admission.Warnings, error) {
	hdfsclusterlog.Info("validate delete", "name", obj.GetName())
	return nil, nil
}

// validate checks that spec.image resolves to a usable reference.
func (v *HdfsClusterCustomValidator) validate(obj *hdfsv1alpha1.HdfsCluster) webhook.ValidationErrors {
	errs := webhook.ValidationErrors{}
	if obj.Spec.Image == nil {
		return errs
	}
	if obj.Spec.Image.Custom != "" {
		if err := validateImage(obj.Spec.Image.Custom); err != nil {
			errs.AddWithValue("spec.image.custom", err.Error(), obj.Spec.Image.Custom)
		}
		return errs
	}
	// Resolved against the same defaults the handler uses, so a spec stating only productVersion —
	// which the handler CAN resolve — is not rejected here.
	if _, err := obj.Spec.Image.ResolveImage(constants.ProductName, constants.ImageDefaults()); err != nil {
		errs.Add("spec.image", err.Error())
	}
	return errs
}

// validateImage checks a custom image reference is a syntactically valid [registry/]repo[:tag].
func validateImage(image string) error {
	pattern := `^([a-z0-9-]+(\.[a-z0-9-]+)*(:[0-9]+)?/)?[a-z0-9_.-]+(/[a-z0-9_.-]+)*(:[a-zA-Z0-9_.-]+)?$`
	matched, err := regexp.MatchString(pattern, image)
	if err != nil {
		return fmt.Errorf("failed to validate image: %w", err)
	}
	if !matched {
		return fmt.Errorf("invalid image format")
	}
	return nil
}
