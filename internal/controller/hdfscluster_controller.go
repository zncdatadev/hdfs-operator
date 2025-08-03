/*
Copyright 2024 zncdatadev.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"

	"github.com/go-logr/logr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	hdfsv1alpha1 "github.com/zncdatadev/hdfs-operator/api/v1alpha1"
	"github.com/zncdatadev/operator-go/pkg/client"
	"github.com/zncdatadev/operator-go/pkg/reconciler"
)

var logger = ctrl.Log.WithName("hdfscluster-controller")

// HdfsClusterReconciler reconciles a HdfsCluster object
type HdfsClusterReconciler struct {
	ctrlclient.Client
	Scheme *runtime.Scheme
	Log    logr.Logger
}

// +kubebuilder:rbac:groups=listeners.kubedoop.dev,resources=listeners,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=hdfs.kubedoop.dev,resources=hdfsclusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=hdfs.kubedoop.dev,resources=hdfsclusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=hdfs.kubedoop.dev,resources=hdfsclusters/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=configmaps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=secrets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=serviceaccounts,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups=authentication.kubedoop.dev,resources=authenticationclasses,verbs=get;list;watch

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the HdfsCluster object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.15.0/pkg/reconcile
func (r *HdfsClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger.V(1).Info("Reconciling HdfsCluster")

	instance := &hdfsv1alpha1.HdfsCluster{}
	err := r.Get(ctx, req.NamespacedName, instance)
	if err != nil {
		if ctrlclient.IgnoreNotFound(err) == nil {
			logger.V(1).Info("HdfsCluster not found, may have been deleted")
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	logger.V(1).Info("HdfsCluster found", "namespace", instance.Namespace, "name", instance.Name)

	resourceClient := &client.Client{
		Client:         r.Client,
		OwnerReference: instance,
	}

	gvk := instance.GetObjectKind().GroupVersionKind()

	clusterReconciler := NewClusterReconciler(
		resourceClient,
		reconciler.ClusterInfo{
			GVK: &metav1.GroupVersionKind{
				Group:   gvk.Group,
				Version: gvk.Version,
				Kind:    gvk.Kind,
			},
			ClusterName: instance.Name,
		},
		&instance.Spec,
	)
	if err := clusterReconciler.RegisterResources(ctx); err != nil {
		return ctrl.Result{}, err
	}

	if result, err := clusterReconciler.Reconcile(ctx); err != nil {
		return ctrl.Result{}, err
	} else if !result.IsZero() {
		return result, nil
	}

	logger.Info("Cluster resource reconciled, checking if ready.", "cluster", instance.Name, "namespace", instance.Namespace)

	if result, err := clusterReconciler.Ready(ctx); err != nil {
		return ctrl.Result{}, err
	} else if !result.IsZero() {
		return result, nil
	}

	logger.V(1).Info("Reconcile finished.", "cluster", instance.Name, "namespace", instance.Namespace)

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *HdfsClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&hdfsv1alpha1.HdfsCluster{}).
		Complete(r)
}
