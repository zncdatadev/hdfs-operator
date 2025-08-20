package common

import (
	"context"
	"fmt"

	hdfsv1alpha1 "github.com/zncdatadev/hdfs-operator/api/v1alpha1"
	"github.com/zncdatadev/hdfs-operator/internal/constant"
	commonsv1alpha1 "github.com/zncdatadev/operator-go/pkg/apis/commons/v1alpha1"
	"github.com/zncdatadev/operator-go/pkg/client"
	"github.com/zncdatadev/operator-go/pkg/reconciler"
	opgoutil "github.com/zncdatadev/operator-go/pkg/util"
	ctrl "sigs.k8s.io/controller-runtime"
)

var logger = ctrl.Log.WithName("role-reconciler")

// HdfsComponentReconciler is the interface that all component reconcilers must implement
type HdfsComponentReconciler interface {
	// RegisterResourceWithRoleGroup registers resources for a specific role group
	RegisterResourceWithRoleGroup(
		ctx context.Context,
		replicas *int32,
		roleGroupInfo *reconciler.RoleGroupInfo,
		overrides *commonsv1alpha1.OverridesSpec,
		config *hdfsv1alpha1.ConfigSpec,
	) ([]reconciler.Reconciler, error)
}

// HdfsComponentResourceBuilder defines methods that component implementations should provide
type HdfsComponentResourceBuilder interface {
	// CreateServiceReconcilers returns internal and access service reconcilers
	CreateServiceReconcilers(
		client *client.Client,
		roleGroupInfo *reconciler.RoleGroupInfo,
	) []reconciler.Reconciler

	// CreateStatefulSetReconciler returns statefulset reconciler
	CreateStatefulSetReconciler(
		ctx context.Context,
		client *client.Client,
		image *opgoutil.Image,
		replicas *int32,
		hdfsCluster *hdfsv1alpha1.HdfsCluster,
		clusterOperation *commonsv1alpha1.ClusterOperationSpec,
		roleGroupInfo *reconciler.RoleGroupInfo,
		config *hdfsv1alpha1.ConfigSpec,
		overrides *commonsv1alpha1.OverridesSpec,
	) (reconciler.Reconciler, error)

	CreateConfigMapReconciler(
		ctx context.Context,
		client *client.Client,
		hdfsCluster *hdfsv1alpha1.HdfsCluster,
		roleGroupInfo *reconciler.RoleGroupInfo,
		replicas *int32,
		config *hdfsv1alpha1.ConfigSpec,
		overrides *commonsv1alpha1.OverridesSpec,
		clusterComponentInfo *ClusterComponentsInfo,
	) (reconciler.Reconciler, error)
}

// BaseHdfsRoleReconciler is the common base for NameNode, DataNode and JournalNode role reconcilers
type BaseHdfsRoleReconciler struct {
	reconciler.BaseRoleReconciler[hdfsv1alpha1.RoleSpec]
	HdfsCluster      *hdfsv1alpha1.HdfsCluster
	ClusterConfig    *hdfsv1alpha1.ClusterConfigSpec
	ClusterOperation *commonsv1alpha1.ClusterOperationSpec
	Image            *opgoutil.Image
	ComponentType    constant.Role
	ComponentRec     HdfsComponentReconciler
}

// NewBaseHdfsRoleReconciler creates a new base role reconciler for HDFS components
func NewBaseHdfsRoleReconciler(
	client *client.Client,
	roleInfo reconciler.RoleInfo,
	spec hdfsv1alpha1.RoleSpec,
	hdfsCluster *hdfsv1alpha1.HdfsCluster,
	image *opgoutil.Image,
	componentType constant.Role,
	componentRec HdfsComponentReconciler,
) *BaseHdfsRoleReconciler {
	stopped := false
	if hdfsCluster.Spec.ClusterOperationSpec != nil {
		stopped = hdfsCluster.Spec.ClusterOperationSpec.Stopped
	}

	return &BaseHdfsRoleReconciler{
		BaseRoleReconciler: *reconciler.NewBaseRoleReconciler(
			client,
			stopped,
			roleInfo,
			spec,
		),
		HdfsCluster:      hdfsCluster,
		ClusterConfig:    hdfsCluster.Spec.ClusterConfig,
		ClusterOperation: hdfsCluster.Spec.ClusterOperationSpec,
		Image:            image,
		ComponentType:    componentType,
		ComponentRec:     componentRec,
	}
}

// RegisterResources registers all resources for all role groups
func (r *BaseHdfsRoleReconciler) RegisterResources(ctx context.Context) error {
	for name, roleGroup := range r.Spec.RoleGroups {
		// Merge configurations
		mergedConfig, err := opgoutil.MergeObject(r.Spec.Config, roleGroup.Config)
		if err != nil {
			return err
		}

		// Merge override configurations
		overrides, err := opgoutil.MergeObject(r.Spec.OverridesSpec, roleGroup.OverridesSpec)
		if err != nil {
			return err
		}

		if overrides == nil {
			overrides = &commonsv1alpha1.OverridesSpec{}
		}

		// merge Default config
		defaultInstance := DefaultRoleConfig(r.GetClusterName(), r.ComponentType)
		if mergedConfig == nil {
			// must set here
			mergedConfig = &hdfsv1alpha1.ConfigSpec{}
		}
		defaultInstance.MergeDefaultConfig(mergedConfig)

		info := &reconciler.RoleGroupInfo{
			RoleInfo:      r.RoleInfo,
			RoleGroupName: name,
		}

		reconcilers, err := r.ComponentRec.RegisterResourceWithRoleGroup(
			ctx,
			roleGroup.Replicas,
			info,
			overrides,
			mergedConfig,
		)
		if err != nil {
			return err
		}

		for _, reconciler := range reconcilers {
			r.AddResource(reconciler)
			logger.Info("registered resource", "role", r.GetName(), "roleGroup", name, "reconciler", reconciler.GetName())
		}
	}
	return nil
}

// RegisterStandardResources registers common resources for an HDFS component
func RegisterStandardResources(
	ctx context.Context,
	client *client.Client,
	builder HdfsComponentResourceBuilder,
	replicas *int32,
	image *opgoutil.Image,
	hdfsCluster *hdfsv1alpha1.HdfsCluster,
	clusterOperation *commonsv1alpha1.ClusterOperationSpec,
	roleGroupInfo *reconciler.RoleGroupInfo,
	config *hdfsv1alpha1.ConfigSpec,
	overrides *commonsv1alpha1.OverridesSpec,
	clusterComponentInfo *ClusterComponentsInfo,
) ([]reconciler.Reconciler, error) {
	var reconcilers = make([]reconciler.Reconciler, 0)

	// Create services
	serviceReconcilers := builder.CreateServiceReconcilers(client, roleGroupInfo)
	reconcilers = append(reconcilers, serviceReconcilers...)

	// Create StatefulSet
	statefulSetReconciler, err := builder.CreateStatefulSetReconciler(
		ctx,
		client,
		image,
		replicas,
		hdfsCluster,
		clusterOperation,
		roleGroupInfo,
		config,
		overrides,
	)
	if err != nil {
		return nil, err
	}
	reconcilers = append(reconcilers, statefulSetReconciler)

	// create ConfigMap reconciler
	configMapReconciler, err := builder.CreateConfigMapReconciler(
		ctx,
		client,
		hdfsCluster,
		roleGroupInfo,
		replicas,
		config,
		overrides,
		clusterComponentInfo,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create ConfigMap reconciler: %w", err)
	}
	reconcilers = append(reconcilers, configMapReconciler)

	return reconcilers, nil
}
