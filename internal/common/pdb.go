package common

import (
	"context"

	hdfsv1alpha1 "github.com/zncdatadev/hdfs-operator/api/v1alpha1"

	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type PDBReconciler[T client.Object] struct {
	GeneralResourceStyleReconciler[T, any]
	name   string
	labels map[string]string
	pdb    *hdfsv1alpha1.PodDisruptionBudgetSpec
}

func NewReconcilePDB[T client.Object](
	client client.Client,
	schema *runtime.Scheme,
	cr T,
	labels map[string]string,
	name string,
	pdb *hdfsv1alpha1.PodDisruptionBudgetSpec,
) *PDBReconciler[T] {
	return &PDBReconciler[T]{
		GeneralResourceStyleReconciler: *NewGeneraResourceStyleReconciler[T, any](
			schema,
			cr,
			client,
			"",
			labels,
			nil,
		),
		name:   name,
		labels: labels,
		pdb:    pdb,
	}
}

func (r *PDBReconciler[T]) Build(_ context.Context) (client.Object, error) {
	if r.pdb == nil {
		return nil, nil
	}
	obj := &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{
			Name:      r.name,
			Namespace: r.Instance.GetNamespace(),
			Labels:    r.labels,
		},
		Spec: policyv1.PodDisruptionBudgetSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: r.labels,
			},
		},
	}

	if r.pdb.MinAvailable > 0 {
		obj.Spec.MinAvailable = &intstr.IntOrString{
			Type:   intstr.Int,
			IntVal: r.pdb.MinAvailable,
		}
	}

	if r.pdb.MaxUnavailable > 0 {
		obj.Spec.MaxUnavailable = &intstr.IntOrString{
			Type:   intstr.Int,
			IntVal: r.pdb.MaxUnavailable,
		}
	}
	return obj, nil
}
