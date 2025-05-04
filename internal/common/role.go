package common

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/go-logr/logr"
	operatorutil "github.com/zncdatadev/operator-go/pkg/util"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/zncdatadev/hdfs-operator/internal/util"
)

type Role string

const (
	NameNode    Role = "namenode"
	DataNode    Role = "datanode"
	JournalNode Role = "journalnode"
)

type RoleReconciler interface {
	RoleName() Role
	ReconcileRole(ctx context.Context) (ctrl.Result, error)
	CacheRoleGroupConfig()
}

// RoleGroupRecociler RoleReconcile role reconciler interface
// all role reconciler should implement this interface
type RoleGroupRecociler interface {
	ReconcileGroup(ctx context.Context) (ctrl.Result, error)
	MergeLabels(mergedGroupCfg any) map[string]string
	RegisterResource()
}

type RoleConfigSpec interface {
	GetRoleConfigSpec(role Role) (any, error)
}

type BaseRoleReconciler[T client.Object] struct {
	Scheme   *runtime.Scheme
	Instance T
	Client   client.Client
	Log      logr.Logger
	Labels   map[string]string
	Image    *operatorutil.Image

	Role Role
}

func (r *BaseRoleReconciler[T]) GetLabels() map[string]string {
	roleLables := RoleLabels{InstanceName: r.Instance.GetName(), Name: string(r.Role)}
	mergeLabels := roleLables.GetLabels()
	return mergeLabels
}

type BaseRoleGroupReconciler[T client.Object] struct {
	Scheme     *runtime.Scheme
	Instance   T
	Client     client.Client
	GroupName  string
	RoleLabels map[string]string
	Log        logr.Logger
	Image      *operatorutil.Image

	Reconcilers []ResourceReconciler
}

func ReconcilerDoHandler(ctx context.Context, reconcilers []ResourceReconciler) (ctrl.Result, error) {
	for _, r := range reconcilers {
		if single, ok := r.(ResourceBuilder); ok {
			res, err := r.ReconcileResource(ctx, NewSingleResourceBuilder(single))
			if err != nil {
				return ctrl.Result{}, err
			}
			if res.RequeueAfter > 0 {
				return res, nil
			}
		} else if multi, ok := r.(MultiResourceReconcilerBuilder); ok {
			res, err := r.ReconcileResource(ctx, NewMultiResourceBuilder(multi))
			if err != nil {
				return ctrl.Result{}, err
			}
			if res.RequeueAfter > 0 {
				return res, nil
			}
		} else {
			panic(fmt.Sprintf("unknown resource reconciler builder, actual type: %T", r))
		}
	}
	return ctrl.Result{}, nil
}

// ReconcileGroup ReconcileRole implements the Role interface
func (m *BaseRoleGroupReconciler[T]) ReconcileGroup(ctx context.Context) (ctrl.Result, error) {
	return ReconcilerDoHandler(ctx, m.Reconcilers)
}

// AppendLabels  merge role labels and additional labels
func (m *BaseRoleGroupReconciler[T]) AppendLabels(additionalLabels map[string]string) map[string]string {
	roleLabels := m.RoleLabels
	mergeLabels := make(util.Map)
	mergeLabels.MapMerge(roleLabels, true)
	mergeLabels.MapMerge(additionalLabels, true)
	mergeLabels["app.kubernetes.io/instance"] = strings.ToLower(m.GroupName)
	return mergeLabels
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
