package name

import (
	"context"
	hdfsv1alpha1 "github.com/zncdata-labs/hdfs-operator/api/v1alpha1"
	"github.com/zncdata-labs/hdfs-operator/internal/common"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type StatefulSetReconciler struct {
	common.WorkloadStyleReconciler[*hdfsv1alpha1.HdfsCluster, *hdfsv1alpha1.RoleGroupSpec]
}

func (s *StatefulSetReconciler) CommandOverride(resource client.Object) {
	//TODO implement me
	panic("implement me")
}

func (s *StatefulSetReconciler) EnvOverride(resource client.Object) {
	//TODO implement me
	panic("implement me")
}

func (s *StatefulSetReconciler) LogOverride(resource client.Object) {
	//TODO implement me
	panic("implement me")
}

// NewStatefulSetController new a StatefulSetReconciler

func NewStatefulSet(
	scheme *runtime.Scheme,
	instance *hdfsv1alpha1.HdfsCluster,
	client client.Client,
	groupName string,
	labels map[string]string,
	mergedCfg *hdfsv1alpha1.RoleGroupSpec,
	replicate int32,
) *StatefulSetReconciler {
	return &StatefulSetReconciler{
		WorkloadStyleReconciler: *common.NewWorkloadStyleReconciler(
			scheme,
			instance,
			client,
			groupName,
			labels,
			mergedCfg,
			replicate,
		),
	}
}

func (s *StatefulSetReconciler) Build(ctx context.Context) (client.Object, error) {
	panic("implement me")
}
