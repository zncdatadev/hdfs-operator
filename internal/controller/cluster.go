package controller

import (
	"context"

	hdfsv1alpha1 "github.com/zncdatadev/hdfs-operator/api/v1alpha1"
	"github.com/zncdatadev/hdfs-operator/internal/common"
	"github.com/zncdatadev/hdfs-operator/internal/constant"
	"github.com/zncdatadev/hdfs-operator/internal/controller/data"
	"github.com/zncdatadev/hdfs-operator/internal/controller/journal"
	"github.com/zncdatadev/hdfs-operator/internal/controller/name"
	"github.com/zncdatadev/hdfs-operator/internal/util/version"
	commonsv1alpha1 "github.com/zncdatadev/operator-go/pkg/apis/commons/v1alpha1"
	"github.com/zncdatadev/operator-go/pkg/builder"
	resourceClient "github.com/zncdatadev/operator-go/pkg/client"
	"github.com/zncdatadev/operator-go/pkg/reconciler"
	"github.com/zncdatadev/operator-go/pkg/util"
	ctrl "sigs.k8s.io/controller-runtime"
)

var clusterLogger = ctrl.Log.WithName("cluster-reconciler")
var _ reconciler.Reconciler = &Reconciler{}

// Reconciler is the main reconciler for HdfsCluster resources
type Reconciler struct {
	reconciler.BaseCluster[*hdfsv1alpha1.HdfsClusterSpec]
	ClusterConfig    *hdfsv1alpha1.ClusterConfigSpec
	ClusterOperation *commonsv1alpha1.ClusterOperationSpec

	instance *hdfsv1alpha1.HdfsCluster
}

// NewClusterReconciler creates a new cluster reconciler for HdfsCluster resources
func NewClusterReconciler(
	client *resourceClient.Client,
	clusterInfo reconciler.ClusterInfo,
	instance *hdfsv1alpha1.HdfsClusterSpec,
) *Reconciler {
	return &Reconciler{
		BaseCluster: *reconciler.NewBaseCluster(
			client,
			clusterInfo,
			instance.ClusterOperationSpec,
			instance,
		),
		ClusterConfig:    instance.ClusterConfig,
		ClusterOperation: instance.ClusterOperationSpec,
	}
}

// GetImage returns the image configuration for HDFS components
func (r *Reconciler) GetImage(roleType constant.Role) *util.Image {
	image := util.NewImage(
		hdfsv1alpha1.DefaultProductName,
		version.BuildVersion,
		hdfsv1alpha1.DefaultProductVersion,
		func(options *util.ImageOptions) {
			options.Custom = r.Spec.Image.Custom
			options.Repo = r.Spec.Image.Repo
			options.PullPolicy = r.Spec.Image.PullPolicy
		},
	)

	if r.Spec.Image.KubedoopVersion != "" {
		image.KubedoopVersion = r.Spec.Image.KubedoopVersion
	}

	return image
}

// RegisterResources registers all resources for the HdfsCluster
func (r *Reconciler) RegisterResources(
	ctx context.Context) error {
	// Optional: Create service account for the cluster if needed
	sa := NewServiceAccountReconciler(r.Client, r.instance, func(o *builder.Options) {
		o.ClusterName = r.ClusterInfo.ClusterName
		o.Labels = r.ClusterInfo.GetLabels()
		o.Annotations = r.ClusterInfo.GetAnnotations()
	})
	if sa != nil {
		r.AddResource(sa)
	}

	clusterComponent := &common.ClusterComponentsInfo{}
	common.PopulateClusterComponents(r.instance, clusterComponent, &r.ClusterInfo)

	// NameNode role
	if r.instance.Spec.NameNode != nil {
		nameNodeRoleInfo := reconciler.RoleInfo{
			ClusterInfo: r.ClusterInfo,
			RoleName:    string(constant.NameNode),
		}

		// Create NameNode reconciler with base image
		nameNodeImage := r.GetImage(constant.NameNode)
		nameNodeReconciler := name.NewNameNodeRole(
			r.Client,
			nameNodeRoleInfo,
			*r.Spec.NameNode,
			nameNodeImage,
			r.instance,
		)

		if err := nameNodeReconciler.RegisterResources(ctx); err != nil {
			return err
		}
		r.AddResource(nameNodeReconciler)
		clusterLogger.Info("Registered NameNode role")
	}

	// JournalNode role
	if r.instance.Spec.JournalNode != nil {
		journalNodeRoleInfo := reconciler.RoleInfo{
			ClusterInfo: r.ClusterInfo,
			RoleName:    string(constant.JournalNode),
		}
		// Create JournalNode reconciler with base image
		journalNodeImage := r.GetImage(constant.JournalNode)
		journalNodeReconciler := journal.NewJournalNodeRole(
			r.Client,

			journalNodeRoleInfo,
			r.Spec.JournalNode,
			journalNodeImage,
			r.instance,
			clusterComponent,
		)
		if err := journalNodeReconciler.RegisterResources(ctx); err != nil {
			return err
		}
		r.AddResource(journalNodeReconciler)
		clusterLogger.Info("Registered JournalNode role")
	}

	// DataNode role
	if r.instance.Spec.DataNode != nil {
		dataNodeRoleInfo := reconciler.RoleInfo{
			ClusterInfo: r.ClusterInfo,
			RoleName:    string(constant.DataNode),
		}
		// Create DataNode reconciler with base image
		dataNodeImage := r.GetImage(constant.DataNode)
		dataNodeReconciler := data.NewDataNodeRole(
			r.Client,
			dataNodeRoleInfo,
			r.Spec.DataNode,
			dataNodeImage,
			r.instance,
			clusterComponent,
		)
		if err := dataNodeReconciler.RegisterResources(ctx); err != nil {
			return err
		}
		r.AddResource(dataNodeReconciler)
		clusterLogger.Info("Registered DataNode role")

	}
	// TODO: discovery

	return nil
}
