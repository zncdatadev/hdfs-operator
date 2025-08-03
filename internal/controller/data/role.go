package data

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

// DataNodeReconciler is the unified reconciler for DataNode
// It implements both HdfsComponentReconciler and HdfsComponentResourceBuilder interfaces
type DataNodeReconciler struct {
	*common.BaseHdfsRoleReconciler
	client               *client.Client
	dataNodeSpec         *hdfsv1alpha1.RoleSpec
	configSpec           hdfsv1alpha1.ConfigSpec
	mergedConfig         *hdfsv1alpha1.RoleGroupSpec
	clusterComponentInfo *common.ClusterComponentsInfo
}

var _ common.HdfsComponentReconciler = &DataNodeReconciler{}
var _ common.HdfsComponentResourceBuilder = &DataNodeReconciler{}

// NewDataNodeRole creates a new DataNode role reconciler
func NewDataNodeRole(
	client *client.Client,
	roleInfo reconciler.RoleInfo,
	spec *hdfsv1alpha1.RoleSpec,
	image *opgoutil.Image,
	instance *hdfsv1alpha1.HdfsCluster,
	clusterComponentInfo *common.ClusterComponentsInfo,
) *DataNodeReconciler {
	dataNodeReconciler := &DataNodeReconciler{
		client:               client,
		dataNodeSpec:         spec,
		clusterComponentInfo: clusterComponentInfo,
	}

	// Create base role reconciler with DataNode as component type
	baseReconciler := common.NewBaseHdfsRoleReconciler(
		client,
		roleInfo,
		*spec,
		instance,
		image,
		string(constant.DataNode),
		dataNodeReconciler, // Pass itself as the componentRec
	)

	dataNodeReconciler.BaseHdfsRoleReconciler = baseReconciler
	return dataNodeReconciler
}

// RegisterResourceWithRoleGroup implements HdfsComponentReconciler interface
func (r *DataNodeReconciler) RegisterResourceWithRoleGroup(
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
		r, // DataNodeReconciler implements HdfsComponentResourceBuilder interface
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
func (r *DataNodeReconciler) CreateConfigMapReconciler(
	ctx context.Context,
	client *client.Client,
	hdfsCluster *hdfsv1alpha1.HdfsCluster,
	roleGroupInfo *reconciler.RoleGroupInfo,
	config *hdfsv1alpha1.ConfigSpec,
	overrides *commonsv1alpha1.OverridesSpec,
	clusterComponentInfo *common.ClusterComponentsInfo,
) (reconciler.Reconciler, error) {

	cmBuilder := NewDataNodeConfigMapBuilder(
		hdfsCluster,
		roleGroupInfo,
		r.configSpec.RoleGroupConfigSpec,
		clusterComponentInfo,
	)

	// DataNodeConfigMapBuilder implements ConfigMapComponentBuilder
	cmReconciler := common.NewConfigMapReconciler(
		ctx,
		client,
		constant.DataNode,
		roleGroupInfo,
		overrides,
		r.configSpec.RoleGroupConfigSpec,
		hdfsCluster,
		cmBuilder, // DataNodeConfigMapBuilder implements ConfigMapComponentBuilder
		common.GetVectorConfigMapName(hdfsCluster),
	)

	return cmReconciler, nil
}

// CreateServiceReconcilers implements HdfsComponentResourceBuilder interface
func (r *DataNodeReconciler) CreateServiceReconcilers(
	client *client.Client,
	roleGroupInfo *reconciler.RoleGroupInfo,
) []reconciler.Reconciler {
	svcBuilder := NewDataNodeServiceBuilder(
		client,
		r.HdfsCluster,
		roleGroupInfo,
		r.configSpec.RoleGroupConfigSpec,
	)

	// Since DataNodeServiceBuilder implements both ServicePortProvider and ServiceBuilder,
	// we can pass it directly as ServicePortProvider
	serviceReconciler := common.NewRoleGroupService(
		client,
		roleGroupInfo,
		opconstants.ClusterInternal,
		true,
		svcBuilder, // DataNodeServiceBuilder implements ServicePortProvider
	)

	return []reconciler.Reconciler{serviceReconciler}
}

// CreateStatefulSetReconciler implements HdfsComponentResourceBuilder interface
func (r *DataNodeReconciler) CreateStatefulSetReconciler(
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
	dnStsBuilder := NewDataNodeStatefulSetBuilder(
		ctx,
		client,
		roleGroupInfo,
		image,
		replicas,
		r.configSpec.RoleGroupConfigSpec,
		overrides,
		hdfsCluster,
		r.mergedConfig,
	)
	dnStsReconciler := reconciler.NewStatefulSet(client, dnStsBuilder, r.ClusterStopped())
	return dnStsReconciler, nil
}
