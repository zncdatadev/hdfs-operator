package v1alpha1

import (
	"github.com/zncdatadev/operator-go/pkg/util"
	corev1 "k8s.io/api/core/v1"
)

const (
	DefaultRepository     = "quay.io/zncdatadev"
	DefaultProductVersion = "3.3.6"
	DefaultProductName    = "hadoop"
	DefaultStackVersion   = "0.0.0-dev"
)

type ImageSpec struct {
	// +kubebuilder:validation:Optional
	Custom string `json:"custom,omitempty"`

	// +kubebuilder:validation:Optional
	// +kubebuilder:default=quay.io/zncdatadev
	Repo string `json:"repository,omitempty"`

	// +kubebuilder:validation:Optional
	// +kubebuilder:default="0.0.0-dev"
	PlatformVersion string `json:"platformVersion,omitempty"`

	// +kubebuilder:validation:Optional
	// +kubebuilder:default="3.3.6"
	ProductVersion string `json:"productVersion,omitempty"`

	// +kubebuilder:validation:Optional
	// +kubebuilder:default:=IfNotPresent
	PullPolicy *corev1.PullPolicy `json:"pullPolicy,omitempty"`

	// +kubebuilder:validation:Optional
	PullSecretName string `json:"pullSecretName,omitempty"`
}

func TransformImage(imageSpec *ImageSpec) *util.Image {
	if imageSpec == nil {
		return util.NewImage(DefaultProductName, DefaultStackVersion, DefaultProductVersion)
	}
	return &util.Image{
		Custom:          imageSpec.Custom,
		Repo:            imageSpec.Repo,
		PlatformVersion: imageSpec.PlatformVersion,
		ProductVersion:  imageSpec.ProductVersion,
		PullPolicy:      imageSpec.PullPolicy,
		PullSecretName:  imageSpec.PullSecretName,
	}
}
