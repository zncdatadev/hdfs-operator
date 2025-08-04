package name

import (
	"context"

	hdfsv1alpha1 "github.com/zncdatadev/hdfs-operator/api/v1alpha1"
	"github.com/zncdatadev/hdfs-operator/internal/common"
	"github.com/zncdatadev/hdfs-operator/internal/constant"
	commonsv1alpha1 "github.com/zncdatadev/operator-go/pkg/apis/commons/v1alpha1"
	"github.com/zncdatadev/operator-go/pkg/client"
	opconstants "github.com/zncdatadev/operator-go/pkg/constants"
	"github.com/zncdatadev/operator-go/pkg/reconciler"
	opgoutil "github.com/zncdatadev/operator-go/pkg/util"
)

// NameNodeReconciler is the unified reconciler for NameNode
// It implements both HdfsComponentReconciler and HdfsComponentResourceBuilder interfaces
type NameNodeReconciler struct {
	*common.BaseHdfsRoleReconciler
	client               *client.Client
	nameNodeSpec         hdfsv1alpha1.RoleSpec
	clusterComponentInfo *common.ClusterComponentsInfo
}

var _ common.HdfsComponentReconciler = &NameNodeReconciler{}
var _ common.HdfsComponentResourceBuilder = &NameNodeReconciler{}

// NewNameNodeRole creates a new NameNode role reconciler
func NewNameNodeRole(
	client *client.Client,
	roleInfo reconciler.RoleInfo,
	spec hdfsv1alpha1.RoleSpec,
	image *opgoutil.Image,
	instance *hdfsv1alpha1.HdfsCluster,
	clusterComponentInfo *common.ClusterComponentsInfo,
) *NameNodeReconciler {
	nameNodeReconciler := &NameNodeReconciler{
		client:               client,
		nameNodeSpec:         spec,
		clusterComponentInfo: clusterComponentInfo,
	}

	// Create base role reconciler with NameNode as component type
	baseReconciler := common.NewBaseHdfsRoleReconciler(
		client,
		roleInfo,
		spec,
		instance,
		image,
		constant.NameNode,
		nameNodeReconciler, // Pass itself as the componentRec
	)

	nameNodeReconciler.BaseHdfsRoleReconciler = baseReconciler
	return nameNodeReconciler
}

// RegisterResourceWithRoleGroup implements HdfsComponentReconciler interface
func (r *NameNodeReconciler) RegisterResourceWithRoleGroup(
	ctx context.Context,
	replicas *int32,
	roleGroupInfo *reconciler.RoleGroupInfo,
	overrides *commonsv1alpha1.OverridesSpec,
	config *hdfsv1alpha1.ConfigSpec,
) ([]reconciler.Reconciler, error) {
	// Use common resource registration logic
	reconcilers, err := common.RegisterStandardResources(
		ctx,
		r.client,
		r, // NameNodeReconciler implements HdfsComponentResourceBuilder interface
		replicas,
		r.Image,
		r.HdfsCluster,
		r.ClusterOperation,
		roleGroupInfo,
		config,
		overrides,
		r.clusterComponentInfo,
	)
	if err != nil {
		return nil, err
	}

	return reconcilers, nil
}

// CreateConfigMapReconciler implements common.HdfsComponentResourceBuilder.
func (r *NameNodeReconciler) CreateConfigMapReconciler(
	ctx context.Context,
	client *client.Client,
	hdfsCluster *hdfsv1alpha1.HdfsCluster,
	roleGroupInfo *reconciler.RoleGroupInfo,
	replicas *int32,
	config *hdfsv1alpha1.ConfigSpec,
	overrides *commonsv1alpha1.OverridesSpec,
	clusterComponentInfo *common.ClusterComponentsInfo,
) (reconciler.Reconciler, error) {
	cmBuilder := NewNamenodeConfigMapBuilder(
		ctx,
		client,
		roleGroupInfo,
		replicas,
		overrides,
		config,
		hdfsCluster,
		clusterComponentInfo,
	)

	return reconciler.NewGenericResourceReconciler(
		client,
		cmBuilder,
	), nil

}

// CreateServiceReconcilers implements HdfsComponentResourceBuilder interface
func (r *NameNodeReconciler) CreateServiceReconcilers(
	client *client.Client,
	roleGroupInfo *reconciler.RoleGroupInfo,
) []reconciler.Reconciler {
	svcBuilder := NewNameNodeServiceBuilder(
		client,
		roleGroupInfo,
		r.HdfsCluster.Spec.ClusterConfig,
	)

	// Since NameNodeServiceBuilder implements both ServicePortProvider and ServiceBuilder,
	// we can pass it directly as ServicePortProvider
	serviceReconciler := common.NewRoleGroupService(
		client,
		roleGroupInfo,
		opconstants.ClusterInternal,
		true,
		svcBuilder, // Type assertion to get the concrete type
	)

	return []reconciler.Reconciler{serviceReconciler}
}

// CreateStatefulSetReconciler implements HdfsComponentResourceBuilder interface
func (r *NameNodeReconciler) CreateStatefulSetReconciler(
	ctx context.Context,
	client *client.Client,
	image *opgoutil.Image,
	replicas *int32,
	hdfsCluster *hdfsv1alpha1.HdfsCluster,
	clusterOperation *commonsv1alpha1.ClusterOperationSpec,
	roleGroupInfo *reconciler.RoleGroupInfo,
	config *hdfsv1alpha1.ConfigSpec,
	overrides *commonsv1alpha1.OverridesSpec,
) (reconciler.Reconciler, error) {
	nnStsBuilder := NewNamenodeStatefulSetBuilder(
		ctx,
		client,
		roleGroupInfo,
		image,
		replicas,
		config.RoleGroupConfigSpec,
		overrides,
		hdfsCluster,
		config,
	)
	nnStsReconciler := reconciler.NewStatefulSet(
		client,
		nnStsBuilder,
		r.ClusterStopped(),
	)
	return nnStsReconciler, nil
}
