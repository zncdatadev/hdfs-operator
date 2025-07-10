package journal

import (
	"context"
	"errors"

	hdfsv1alpha1 "github.com/zncdatadev/hdfs-operator/api/v1alpha1"
	"github.com/zncdatadev/hdfs-operator/internal/common"
	"github.com/zncdatadev/hdfs-operator/internal/constant"
	commonsv1alpha1 "github.com/zncdatadev/operator-go/pkg/apis/commons/v1alpha1"
	"github.com/zncdatadev/operator-go/pkg/client"
	opconstants "github.com/zncdatadev/operator-go/pkg/constants"
	"github.com/zncdatadev/operator-go/pkg/reconciler"
	opgoutil "github.com/zncdatadev/operator-go/pkg/util"
)

// JournalNodeReconciler is the unified reconciler for JournalNode
// It implements both HdfsComponentReconciler and HdfsComponentResourceBuilder interfaces
type JournalNodeReconciler struct {
	*common.BaseHdfsRoleReconciler
	client               *client.Client
	journalNodeSpec      *hdfsv1alpha1.RoleSpec
	clusterComponentInfo *common.ClusterComponentsInfo
	configSpec           hdfsv1alpha1.ConfigSpec
	mergedConfig         *hdfsv1alpha1.RoleGroupSpec
}

var _ common.HdfsComponentReconciler = &JournalNodeReconciler{}
var _ common.HdfsComponentResourceBuilder = &JournalNodeReconciler{}

// NewJournalNodeRole creates a new JournalNode role reconciler
func NewJournalNodeRole(
	client *client.Client,
	roleInfo reconciler.RoleInfo,
	spec *hdfsv1alpha1.RoleSpec,
	image *opgoutil.Image,
	instance *hdfsv1alpha1.HdfsCluster,
) *JournalNodeReconciler {
	journalNodeReconciler := &JournalNodeReconciler{
		client:          client,
		journalNodeSpec: spec,
	}

	// Create base role reconciler with JournalNode as component type
	baseReconciler := common.NewBaseHdfsRoleReconciler(
		client,
		roleInfo,
		spec,
		instance,
		image,
		"journalnode",
		journalNodeReconciler, // Pass itself as the componentRec
	)

	journalNodeReconciler.BaseHdfsRoleReconciler = baseReconciler
	return journalNodeReconciler
}

// RegisterResourceWithRoleGroup implements HdfsComponentReconciler interface
func (r *JournalNodeReconciler) RegisterResourceWithRoleGroup(
	ctx context.Context,
	replicas *int32,
	roleGroupInfo *reconciler.RoleGroupInfo,
	overrides *commonsv1alpha1.OverridesSpec,
	config hdfsv1alpha1.ConfigSpec,
) ([]reconciler.Reconciler, error) {
	// Use common resource registration logic
	reconcilers, err := common.RegisterStandardResources(
		ctx,
		r.client,
		r, // JournalNodeReconciler implements HdfsComponentResourceBuilder interface
		replicas,
		r.Image,
		r.HdfsCluster,
		r.ClusterOperation,
		roleGroupInfo,
		config,
		overrides,
	)
	if err != nil {
		return nil, err
	}

	return reconcilers, nil
}

// CreateConfigMapReconciler implements common.HdfsComponentResourceBuilder.
func (r *JournalNodeReconciler) CreateConfigMapReconciler(
	ctx context.Context,
	client *client.Client,
	hdfsCluster *hdfsv1alpha1.HdfsCluster,
	roleGroupInfo *reconciler.RoleGroupInfo,
	config hdfsv1alpha1.ConfigSpec,
	overrides *commonsv1alpha1.OverridesSpec,
) (reconciler.Reconciler, error) {

	cmBuilder := NewJournalnodeConfigMapBuilder(
		ctx,
		client,
		roleGroupInfo,
		overrides,
		r.configSpec.RoleGroupConfigSpec,
		hdfsCluster,
		r.mergedConfig,
	)

	if a, ok := cmBuilder.(common.ConfigMapComponentBuilder); ok {
		// Ensure the builder implements ConfigMapComponentBuilder
		cmReconciler := common.NewConfigMapReconciler(
			ctx,
			client,
			constant.JournalNode,
			roleGroupInfo,
			overrides,
			r.configSpec.RoleGroupConfigSpec,
			hdfsCluster,
			a, // JournalNodeReconciler implements ConfigMapComponentBuilder
			common.GetVectorConfigMapName(hdfsCluster),
		)

		return cmReconciler, nil
	}

	return nil, errors.New("JournalnodeConfigMapBuilder does not implement ConfigMapComponentBuilder")
}

// CreateServiceReconcilers implements HdfsComponentResourceBuilder interface
func (r *JournalNodeReconciler) CreateServiceReconcilers(
	client *client.Client,
	roleGroupInfo *reconciler.RoleGroupInfo,
) []reconciler.Reconciler {
	svcBuilder := NewJournalNodeServiceBuilder(
		client,
		roleGroupInfo,
		r.HdfsCluster.Spec.ClusterConfig,
	)

	// Since JournalNodeServiceBuilder implements both ServicePortProvider and ServiceBuilder,
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
func (r *JournalNodeReconciler) CreateStatefulSetReconciler(
	ctx context.Context,
	client *client.Client,
	image *opgoutil.Image,
	replicas *int32,
	hdfsCluster *hdfsv1alpha1.HdfsCluster,
	clusterOperation *commonsv1alpha1.ClusterOperationSpec,
	roleGroupInfo *reconciler.RoleGroupInfo,
	config hdfsv1alpha1.ConfigSpec,
	overrides *commonsv1alpha1.OverridesSpec,
) (reconciler.Reconciler, error) {

	return nil, nil
}
