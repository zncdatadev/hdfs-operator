package controller

import (
	hdfsv1alpha1 "github.com/zncdatadev/hdfs-operator/api/v1alpha1"
	"github.com/zncdatadev/hdfs-operator/internal/common"
	"github.com/zncdatadev/operator-go/pkg/builder"
	"github.com/zncdatadev/operator-go/pkg/client"
	"github.com/zncdatadev/operator-go/pkg/reconciler"
)

// NewServiceAccountReconciler creates a new ServiceAccountReconciler
func NewServiceAccountReconciler(
	client *client.Client,
	instance *hdfsv1alpha1.HdfsCluster,
	mergedLabels map[string]string,
	options ...builder.Option,
) reconciler.ResourceReconciler[*builder.GenericServiceAccountBuilder] {

	saBuilder := builder.NewGenericServiceAccountBuilder(
		client,
		common.CreateServiceAccountName(instance.GetName()),
		options...,
	)

	return reconciler.NewGenericResourceReconciler(
		client,
		saBuilder,
	)
}
