package controller

import (
	"context"

	hdfsv1alpha1 "github.com/zncdatadev/hdfs-operator/api/v1alpha1"
	"github.com/zncdatadev/hdfs-operator/internal/constant"
	"github.com/zncdatadev/hdfs-operator/internal/controller/name"
	"github.com/zncdatadev/hdfs-operator/internal/util/version"
	commonsv1alpha1 "github.com/zncdatadev/operator-go/pkg/apis/commons/v1alpha1"
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
}

// NewClusterReconciler creates a new cluster reconciler for HdfsCluster resources
func NewClusterReconciler(
	client *resourceClient.Client,
	clusterInfo reconciler.ClusterInfo,
	spec *hdfsv1alpha1.HdfsClusterSpec,
) *Reconciler {
	return &Reconciler{
		BaseCluster: *reconciler.NewBaseCluster(
			client,
			clusterInfo,
			spec.ClusterOperationSpec,
			spec,
		),
		ClusterConfig:    spec.ClusterConfig,
		ClusterOperation: spec.ClusterOperationSpec,
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
func (r *Reconciler) RegisterResources(ctx context.Context) error {
	// Optional: Create service account for the cluster if needed
	// sa := createServiceAccount(r.Client, r.GetName())
	// if sa != nil {
	//     r.AddResource(sa)
	// }

	// NameNode role
	if r.Spec.NameNode != nil {
		nameNodeRoleInfo := reconciler.RoleInfo{
			ClusterInfo: r.ClusterInfo,
			RoleName:    "namenode",
		}

		// Create NameNode reconciler with base image
		nameNodeImage := r.GetImage(constant.NameNode)
		nameNodeReconciler := name.NewNameNodeRole(
			r.Client,
			nameNodeRoleInfo,
			r.Spec.NameNode,
			nameNodeImage,
			&hdfsv1alpha1.HdfsCluster{
				Spec: *r.Spec,
			},
		)

		if err := nameNodeReconciler.RegisterResources(ctx); err != nil {
			return err
		}
		r.AddResource(nameNodeReconciler)
		clusterLogger.Info("Registered NameNode role")
	}

	// Note: DataNode and JournalNode are using old API and need to be refactored
	// For now, we'll skip them until they are updated to the new architecture

	// TODO: Refactor DataNode and JournalNode to use new API like NameNode
	// DataNode role
	// if r.Spec.DataNode != nil {
	//     ...
	// }

	// JournalNode role
	// if r.Spec.JournalNode != nil {
	//     ...
	// }

	return nil
}
