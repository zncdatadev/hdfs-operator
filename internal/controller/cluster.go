package controller

import (
	"context"
	"github.com/go-logr/logr"
	hdfsv1alpha1 "github.com/zncdata-labs/hdfs-operator/api/v1alpha1"
	"github.com/zncdata-labs/hdfs-operator/internal/common"
	"github.com/zncdata-labs/hdfs-operator/internal/controller/data"
	"github.com/zncdata-labs/hdfs-operator/internal/controller/journal"
	"github.com/zncdata-labs/hdfs-operator/internal/controller/name"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type ClusterReconciler struct {
	client client.Client
	scheme *runtime.Scheme
	cr     *hdfsv1alpha1.HdfsCluster
	Log    logr.Logger

	roleReconcilers     []common.RoleReconciler
	resourceReconcilers []common.ResourceReconciler
}

func NewClusterReconciler(client client.Client, scheme *runtime.Scheme, cr *hdfsv1alpha1.HdfsCluster) *ClusterReconciler {
	c := &ClusterReconciler{
		client: client,
		scheme: scheme,
		cr:     cr,
	}
	c.RegisterRole()
	c.RegisterResource()
	return c
}

// RegisterRole register role reconciler
func (c *ClusterReconciler) RegisterRole() {
	nameNodeRole := name.NewRoleNameNode(c.scheme, c.cr, c.client, c.Log)
	jounalNodeRole := journal.NewRoleJournalNode(c.scheme, c.cr, c.client, c.Log)
	dataNodeRole := data.NewRoleDataNode(c.scheme, c.cr, c.client, c.Log)
	c.roleReconcilers = []common.RoleReconciler{
		jounalNodeRole,
		nameNodeRole,
		dataNodeRole,
	}
}

func (c *ClusterReconciler) RegisterResource() {
	// registry sa resource
	labels := common.RoleLabels{
		InstanceName: c.cr.Name,
	}
	sa := NewServiceAccount(c.scheme, c.cr, c.client, "", labels.GetLabels(), nil)
	c.resourceReconcilers = []common.ResourceReconciler{sa}
}

func (c *ClusterReconciler) ReconcileCluster(ctx context.Context) (ctrl.Result, error) {
	c.preReconcile()

	// reconcile resource of cluster level
	if len(c.resourceReconcilers) > 0 {
		res, err := common.ReconcilerDoHandler(ctx, c.resourceReconcilers)
		if err != nil {
			return ctrl.Result{}, err
		}
		if res.RequeueAfter > 0 {
			return res, nil
		}
	}

	//reconcile role
	for _, r := range c.roleReconcilers {
		res, err := r.ReconcileRole(ctx)
		if err != nil {
			return ctrl.Result{}, err
		}
		if res.RequeueAfter > 0 {
			return res, nil
		}
	}

	// reconcile discovery
	res, err := c.ReconcileDiscovery(ctx)
	if err != nil {
		return ctrl.Result{}, err
	}
	if res.RequeueAfter > 0 {
		return res, nil
	}

	return ctrl.Result{}, nil
}

func (c *ClusterReconciler) preReconcile() {
	// pre-reconcile
	// merge all the role-group cfg of roles, and cache it
	// because of existing role group config circle reference
	// we need to cache it before reconcile
	for _, r := range c.roleReconcilers {
		r.CacheRoleGroupConfig()
	}
}

func (c *ClusterReconciler) ReconcileDiscovery(ctx context.Context) (ctrl.Result, error) {
	discovery := NewDiscovery(c.scheme, c.cr, c.client)
	return discovery.ReconcileResource(ctx, common.NewSingleResourceBuilder(discovery))
}

type HdfsClusterInstance struct {
	Instance *hdfsv1alpha1.HdfsCluster
}

func (h *HdfsClusterInstance) GetRoleConfigSpec(role common.Role) (any, error) {
	return nil, nil
}

func (h *HdfsClusterInstance) GetClusterConfig() any {
	return h.Instance.Spec.ClusterConfigSpec
}

func (h *HdfsClusterInstance) GetNamespace() string {
	return h.Instance.GetNamespace()
}

func (h *HdfsClusterInstance) GetInstanceName() string {
	return h.Instance.GetName()
}
