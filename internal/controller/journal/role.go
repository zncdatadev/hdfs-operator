package journal

import (
	"context"
	"fmt"
	"github.com/go-logr/logr"
	hdfsv1alpha1 "github.com/zncdata-labs/hdfs-operator/api/v1alpha1"
	"github.com/zncdata-labs/hdfs-operator/internal/common"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// role server reconciler

type Role struct {
	common.BaseRoleReconciler[*hdfsv1alpha1.HdfsCluster]
}

func NewRoleJournalNode(
	scheme *runtime.Scheme,
	instance *hdfsv1alpha1.HdfsCluster,
	client client.Client,
	log logr.Logger) *Role {
	r := &Role{
		BaseRoleReconciler: common.BaseRoleReconciler[*hdfsv1alpha1.HdfsCluster]{
			Scheme:   scheme,
			Instance: instance,
			Client:   client,
			Log:      log,
		},
	}
	r.Labels = r.GetLabels()
	r.Role = r.RoleName()
	return r
}

func (r *Role) RoleName() common.Role {
	return common.JournalNode
}

func (r *Role) CacheRoleGroupConfig() {
	roleSpec := r.Instance.Spec.JournalNode
	groups := roleSpec.RoleGroups
	// merge all the role-group cfg
	// and cache it
	for groupName, groupSpec := range groups {
		mergedCfg := MergeConfig(roleSpec, groupSpec)
		cacheKey := common.CreateRoleCfgCacheKey(r.Instance.GetName(), r.Role, groupName)
		common.MergedCache.Set(cacheKey, mergedCfg)
	}
}

func (r *Role) ReconcileRole(ctx context.Context) (ctrl.Result, error) {
	roleCfg := r.Instance.Spec.JournalNode
	// role pdb
	if roleCfg.Config != nil && roleCfg.Config.PodDisruptionBudget != nil {
		pdb := common.NewReconcilePDB(r.Client, r.Scheme, r.Instance, r.GetLabels(), string(r.RoleName()),
			roleCfg.PodDisruptionBudget)
		res, err := pdb.ReconcileResource(ctx, common.NewSingleResourceBuilder(pdb))
		if err != nil {
			return ctrl.Result{}, err
		}
		if res.RequeueAfter > 0 {
			return res, nil
		}
	}
	// reconciler groups
	for name := range roleCfg.RoleGroups {
		groupReconciler := NewRoleGroupReconciler(r.Scheme, r.Instance, r.Client, name, r.GetLabels(), r.Log)
		res, err := groupReconciler.ReconcileGroup(ctx)
		if err != nil {
			return ctrl.Result{}, err
		}
		if res.RequeueAfter > 0 {
			return res, nil
		}
	}
	return ctrl.Result{}, nil
}

// RoleGroup master role group reconcile
type RoleGroup struct {
	common.BaseRoleGroupReconciler[*hdfsv1alpha1.HdfsCluster]
}

func NewRoleGroupReconciler(
	scheme *runtime.Scheme,
	instance *hdfsv1alpha1.HdfsCluster,
	client client.Client,
	groupName string,
	roleLabels map[string]string,
	log logr.Logger) *RoleGroup {
	r := &RoleGroup{
		BaseRoleGroupReconciler: common.BaseRoleGroupReconciler[*hdfsv1alpha1.HdfsCluster]{
			Scheme:     scheme,
			Instance:   instance,
			Client:     client,
			GroupName:  groupName,
			RoleLabels: roleLabels,
			Log:        log,
		},
	}
	r.RegisterResource()
	return r
}

func (m *RoleGroup) RegisterResource() {
	cfg := m.MergeGroupConfigSpec()
	lables := m.MergeLabels(cfg)
	mergedCfg := cfg.(*hdfsv1alpha1.JournalNodeRoleGroupSpec)
	pdbSpec := mergedCfg.Config.PodDisruptionBudget
	//logDataBuilder := &LogDataBuilder{cfg: mergedCfg}

	pdb := common.NewReconcilePDB(m.Client, m.Scheme, m.Instance, lables, m.GroupName, pdbSpec)
	cm := NewConfigMap(m.Scheme, m.Instance, m.Client, m.GroupName, lables, mergedCfg)
	//logging := NewJournalNodeLogging(m.Scheme, m.Instance, m.Client, m.GroupName, lables, mergedCfg, common.JournalNode)
	statefulSet := NewStatefulSet(m.Scheme, m.Instance, m.Client, m.GroupName, lables, mergedCfg, mergedCfg.Replicas)
	svc := NewServiceHeadless(m.Scheme, m.Instance, m.Client, m.GroupName, lables, mergedCfg)
	m.Reconcilers = []common.ResourceReconciler{cm /* logging,*/, pdb, svc, statefulSet}
}

func (m *RoleGroup) MergeGroupConfigSpec() any {
	cacheKey := common.CreateRoleCfgCacheKey(m.Instance.GetName(), common.JournalNode, m.GroupName)
	if cfg, ok := common.MergedCache.Get(cacheKey); ok {
		return cfg
	}
	panic(fmt.Sprintf("role group config not found: %s, key: %s", m.GroupName, cacheKey))
}

func (m *RoleGroup) MergeLabels(mergedCfg any) map[string]string {
	mergedMasterCfg := mergedCfg.(*hdfsv1alpha1.JournalNodeRoleGroupSpec)
	return m.AppendLabels(mergedMasterCfg.Config.NodeSelector)
}

// MergeConfig merge the role's config into the role group's config
func MergeConfig(masterRole *hdfsv1alpha1.JournalNodeSpec,
	group *hdfsv1alpha1.JournalNodeRoleGroupSpec) *hdfsv1alpha1.JournalNodeRoleGroupSpec {
	copiedRoleGroup := group.DeepCopy()
	// Merge the role into the role group.
	// if the role group has a config, and role group not has a config, will
	// merge the role's config into the role group's config.
	common.MergeObjects(copiedRoleGroup, masterRole, []string{"RoleGroups"})

	// merge the role's config into the role group's config
	if masterRole.Config != nil && copiedRoleGroup.Config != nil {
		common.MergeObjects(copiedRoleGroup.Config, masterRole.Config, []string{})
	}
	return copiedRoleGroup
}
