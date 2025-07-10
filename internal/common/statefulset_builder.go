package common

import (
	"context"

	hdfsv1alpha1 "github.com/zncdatadev/hdfs-operator/api/v1alpha1"
	"github.com/zncdatadev/hdfs-operator/internal/constant"
	commonsv1alpha1 "github.com/zncdatadev/operator-go/pkg/apis/commons/v1alpha1"
	"github.com/zncdatadev/operator-go/pkg/builder"
	"github.com/zncdatadev/operator-go/pkg/client"
	"github.com/zncdatadev/operator-go/pkg/reconciler"
	"github.com/zncdatadev/operator-go/pkg/util"
	appv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
)

// StatefulSetBuilder is the common builder for HDFS StatefulSets
type StatefulSetBuilder struct {
	*builder.StatefulSet
	instance      *hdfsv1alpha1.HdfsCluster
	roleGroupInfo *reconciler.RoleGroupInfo
	roleType      constant.Role
	ctx           context.Context
}

// NewStatefulSetBuilder creates a new StatefulSetBuilder with common configuration
func NewStatefulSetBuilder(
	ctx context.Context,
	client *client.Client,
	roleGroupInfo *reconciler.RoleGroupInfo,
	image *util.Image,
	replicas *int32,
	roleConfig *commonsv1alpha1.RoleGroupConfigSpec,
	overrides *commonsv1alpha1.OverridesSpec,
	instance *hdfsv1alpha1.HdfsCluster,
	roleType constant.Role,
) *StatefulSetBuilder {
	statefulSetBuilder := builder.NewStatefulSetBuilder(
		client,
		roleGroupInfo.GetFullName(),
		replicas,
		image,
		overrides,
		roleConfig,
		func(o *builder.Options) {
			o.ClusterName = roleGroupInfo.ClusterName
			o.Labels = roleGroupInfo.GetLabels()
			o.Annotations = roleGroupInfo.GetAnnotations()
			o.RoleName = roleGroupInfo.RoleName
			o.RoleGroupName = roleGroupInfo.GetGroupName()
		},
	)

	return &StatefulSetBuilder{
		StatefulSet:   statefulSetBuilder,
		instance:      instance,
		roleGroupInfo: roleGroupInfo,
		roleType:      roleType,
		ctx:           ctx,
	}
}

// StatefulSetComponentBuilder defines methods that should be implemented by role-specific builders
type StatefulSetComponentBuilder interface {
	// GetName returns the StatefulSet name
	GetName() string
	// GetMainContainers returns the main containers for the component
	GetMainContainers() []corev1.Container
	// GetInitContainers returns any init containers required by the component
	GetInitContainers() []corev1.Container
	// GetVolumes returns component-specific volumes
	GetVolumes() []corev1.Volume
	// GetVolumeClaimTemplates returns PVCs for the component
	GetVolumeClaimTemplates() []corev1.PersistentVolumeClaim
	// GetSecurityContext returns the security context for the pod
	GetSecurityContext() *corev1.PodSecurityContext
	// GetServiceAccountName returns the service account name
	GetServiceAccountName() string
}

// Build constructs the StatefulSet object combining common and component-specific configurations
func (b *StatefulSetBuilder) Build(ctx context.Context, component StatefulSetComponentBuilder) (ctrlclient.Object, error) {
	// Add component-specific containers
	for _, container := range component.GetMainContainers() {
		b.AddContainer(&container)
	}

	// Add init containers if any
	for _, initContainer := range component.GetInitContainers() {
		b.AddInitContainer(&initContainer)
	}

	// Get common volumes
	commonVolumes := GetCommonVolumes(b.instance.Spec.ClusterConfig, b.instance.GetName(), b.roleType)

	// Add common volumes
	b.AddVolumes(commonVolumes)

	// Add component-specific volumes
	b.AddVolumes(component.GetVolumes())

	// Add volume claim templates
	b.AddVolumeClaimTemplates(component.GetVolumeClaimTemplates())

	// Create the StatefulSet object using the embedded builder
	sts, err := b.GetObject()
	if err != nil {
		return nil, err
	}

	// Set parallel pod management for faster scaling
	sts.Spec.PodManagementPolicy = appv1.ParallelPodManagement

	// Set security context if provided
	if securityContext := component.GetSecurityContext(); securityContext != nil {
		sts.Spec.Template.Spec.SecurityContext = securityContext
	}

	// Set service account name if provided
	if serviceAccountName := component.GetServiceAccountName(); serviceAccountName != "" {
		sts.Spec.Template.Spec.ServiceAccountName = serviceAccountName
	}

	// Add vector container if enabled
	if err := b.addVectorContainer(sts); err != nil {
		return nil, err
	}

	// Add OIDC container if needed
	if err := b.addOIDCContainer(ctx, sts); err != nil {
		return nil, err
	}

	return sts, nil
}

// addVectorContainer adds vector container if logging is enabled
func (b *StatefulSetBuilder) addVectorContainer(sts *appv1.StatefulSet) error {
	// This method should be implemented by role-specific builders
	// For now, we'll leave it empty and let the specific implementations handle it
	return nil
}

// addOIDCContainer adds OIDC container if authentication is configured
func (b *StatefulSetBuilder) addOIDCContainer(ctx context.Context, sts *appv1.StatefulSet) error {
	if b.instance.Spec.ClusterConfig.Authentication != nil && b.instance.Spec.ClusterConfig.Authentication.AuthenticationClass != "" {
		// This method should be implemented by role-specific builders
		// For now, we'll leave it empty and let the specific implementations handle it
	}
	return nil
}

// GetInstance returns the instance
func (b *StatefulSetBuilder) GetInstance() *hdfsv1alpha1.HdfsCluster {
	return b.instance
}

// GetRoleGroupInfo returns the role group info
func (b *StatefulSetBuilder) GetRoleGroupInfo() *reconciler.RoleGroupInfo {
	return b.roleGroupInfo
}

// GetRoleType returns the role type
func (b *StatefulSetBuilder) GetRoleType() constant.Role {
	return b.roleType
}
