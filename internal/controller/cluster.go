package controller

import (
	"context"
	"github.com/go-logr/logr"
	hdfsv1alpha1 "github.com/zncdata-labs/hdfs-operator/api/v1alpha1"
	"github.com/zncdata-labs/hdfs-operator/internal/common"
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
	//serverRole := NewRoleServer(c.scheme, c.cr, c.client, c.Log)
	//c.roleReconcilers = []common.RoleReconciler{serverRole}
}

func (c *ClusterReconciler) RegisterResource() {
	//label := common.RoleLabels[*stackv1alpha1.ZookeeperCluster]{
	//	Cr:   c.cr,
	//	Name: string(common.Server),
	//}
	//lables := label.GetLabels()
	//svc := NewClusterService(c.scheme, c.cr, c.client, "", lables, nil)
	//c.resourceReconcilers = []common.ResourceReconciler{svc}
}

func (c *ClusterReconciler) ReconcileCluster(ctx context.Context) (ctrl.Result, error) {
	if len(c.resourceReconcilers) > 0 {
		res, err := common.ReconcilerDoHandler(ctx, c.resourceReconcilers)
		if err != nil {
			return ctrl.Result{}, err
		}
		if res.RequeueAfter > 0 {
			return res, nil
		}
	}

	for _, r := range c.roleReconcilers {
		res, err := r.ReconcileRole(ctx)
		if err != nil {
			return ctrl.Result{}, err
		}
		if res.RequeueAfter > 0 {
			return res, nil
		}
	}
	return ctrl.Result{}, nil
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
