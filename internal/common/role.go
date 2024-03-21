package common

import (
	"context"
	"github.com/go-logr/logr"
	stackv1alpha1 "github.com/zncdata-labs/alluxio-operator/api/v1alpha1"
	"k8s.io/apimachinery/pkg/runtime"
	"reflect"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type Role string

const (
	Master Role = "master"
	Worker Role = "worker"
)

type RoleReconciler interface {
	RoleName() Role
	MergeLabels() map[string]string
	ReconcileRole(ctx context.Context) (ctrl.Result, error)
}

// RoleGroupRecociler RoleReconcile role reconciler interface
// all role reconciler should implement this interface
type RoleGroupRecociler interface {
	ReconcileGroup(ctx context.Context) (ctrl.Result, error)
	MergeLabels(mergedGroupCfg any) map[string]string
	MergeGroupConfigSpec() any
}

type BaseRoleReconciler[R any] struct {
	Scheme   *runtime.Scheme
	Instance *stackv1alpha1.Alluxio
	Client   client.Client
	Log      logr.Logger
	Labels   map[string]string
	Role     R
}

func (r *BaseRoleReconciler[R]) GetLabels(role Role) map[string]string {
	roleLables := RoleLabels{Cr: r.Instance, Name: string(role)}
	mergeLabels := roleLables.GetLabels()
	return mergeLabels
}

func (r *BaseRoleReconciler[R]) EnabledClusterConfig() bool {
	return r.Instance.Spec.ClusterConfig != nil
}

// MergeObjects merge right to left, if field not in left, it will be added from right,
// else skip.
// Node: If variable is a pointer, it will be modified directly.
func MergeObjects(left interface{}, right interface{}, exclude []string) {

	leftValues := reflect.ValueOf(left)
	rightValues := reflect.ValueOf(right)

	if leftValues.Kind() == reflect.Ptr {
		leftValues = leftValues.Elem()
	}

	if rightValues.Kind() == reflect.Ptr {
		rightValues = rightValues.Elem()
	}

	for i := 0; i < rightValues.NumField(); i++ {
		rightField := rightValues.Field(i)
		rightFieldName := rightValues.Type().Field(i).Name
		if !contains(exclude, rightFieldName) {
			// if right field is zero value, skip
			if reflect.DeepEqual(rightField.Interface(), reflect.Zero(rightField.Type()).Interface()) {
				continue
			}
			leftField := leftValues.FieldByName(rightFieldName)

			// if left field is zero value, set it use right field, else skip
			if !reflect.DeepEqual(leftField.Interface(), reflect.Zero(leftField.Type()).Interface()) {
				continue
			}

			leftField.Set(rightField)
		}
	}
}

func contains(slice []string, str string) bool {
	for _, v := range slice {
		if v == str {
			return true
		}
	}
	return false
}
