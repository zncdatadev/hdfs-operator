package common

import (
	"context"

	hdfsv1alpha1 "github.com/zncdatadev/hdfs-operator/api/v1alpha1"
	"github.com/zncdatadev/operator-go/pkg/util"
	appv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// RoleType represents different HDFS component types
type RoleType string

const (
	NameNode    RoleType = "namenode"
	DataNode    RoleType = "datanode"
	JournalNode RoleType = "journalnode"
)

// StatefulSetBuilder is the common builder for HDFS StatefulSets
type StatefulSetBuilder struct {
	scheme      *runtime.Scheme
	client      client.Client
	instance    *hdfsv1alpha1.HdfsCluster
	groupName   string
	labels      map[string]string
	roleType    RoleType
	replicas    *int32
	image       *util.Image
	serviceName string
	ctx         context.Context
}

// NewStatefulSetBuilder creates a new StatefulSetBuilder with common configuration
func NewStatefulSetBuilder(
	ctx context.Context,
	scheme *runtime.Scheme,
	instance *hdfsv1alpha1.HdfsCluster,
	client client.Client,
	groupName string,
	labels map[string]string,
	roleType RoleType,
	replicas *int32,
	image *util.Image,
	serviceName string,
) *StatefulSetBuilder {
	return &StatefulSetBuilder{
		scheme:      scheme,
		client:      client,
		instance:    instance,
		groupName:   groupName,
		labels:      labels,
		roleType:    roleType,
		replicas:    replicas,
		image:       image,
		serviceName: serviceName,
		ctx:         ctx,
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
func (b *StatefulSetBuilder) Build(ctx context.Context, component StatefulSetComponentBuilder) (client.Object, error) {
	// Get common volumes
	commonVolumes := GetCommonVolumes(b.instance.Spec.ClusterConfig, b.instance.GetName(), string(b.roleType))

	// Combine common and component-specific volumes
	allVolumes := append(commonVolumes, component.GetVolumes()...)

	// Create the StatefulSet object
	sts := &appv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      component.GetName(),
			Namespace: b.instance.GetNamespace(),
			Labels:    b.labels,
		},
		Spec: appv1.StatefulSetSpec{
			ServiceName:          b.serviceName,
			Replicas:             b.replicas,
			Selector:             &metav1.LabelSelector{MatchLabels: b.labels},
			PodManagementPolicy:  appv1.ParallelPodManagement,
			VolumeClaimTemplates: component.GetVolumeClaimTemplates(),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: b.labels,
				},
				Spec: corev1.PodSpec{
					ServiceAccountName: component.GetServiceAccountName(),
					SecurityContext:    component.GetSecurityContext(),
					Containers:         component.GetMainContainers(),
					InitContainers:     component.GetInitContainers(),
					Volumes:            allVolumes,
				},
			},
		},
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

// GetGroupName returns the group name
func (b *StatefulSetBuilder) GetGroupName() string {
	return b.groupName
}

// GetLabels returns the labels
func (b *StatefulSetBuilder) GetLabels() map[string]string {
	return b.labels
}

// GetRoleType returns the role type
func (b *StatefulSetBuilder) GetRoleType() RoleType {
	return b.roleType
}

// GetImage returns the image
func (b *StatefulSetBuilder) GetImage() *util.Image {
	return b.image
}

// GetClient returns the client
func (b *StatefulSetBuilder) GetClient() client.Client {
	return b.client
}

// GetScheme returns the scheme
func (b *StatefulSetBuilder) GetScheme() *runtime.Scheme {
	return b.scheme
}
