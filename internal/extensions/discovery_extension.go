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

// Package extensions holds HDFS ClusterExtensions — cluster-level hooks that run around the
// SDK's role-group reconciliation.
package extensions

import (
	"context"
	"fmt"

	"github.com/zncdatadev/operator-go/pkg/common"
	"github.com/zncdatadev/operator-go/pkg/config"
	"github.com/zncdatadev/operator-go/pkg/reconciler"
	"sigs.k8s.io/controller-runtime/pkg/client"

	hdfsv1alpha1 "github.com/zncdatadev/hdfs-operator/api/v1alpha1"
	"github.com/zncdatadev/hdfs-operator/internal/constants"
	"github.com/zncdatadev/hdfs-operator/internal/product"
)

// DiscoveryExtension publishes a cluster-level discovery ConfigMap (named after the cluster)
// containing the client-facing core-site.xml / hdfs-site.xml, so external clients can reach the
// HA NameNodes. It is a ClusterExtension so it runs once per cluster (not per role group), after
// the role groups — and their Services — have been reconciled.
type DiscoveryExtension struct {
	common.BaseExtension
}

// NewDiscoveryExtension creates the discovery ClusterExtension.
func NewDiscoveryExtension() *DiscoveryExtension {
	return &DiscoveryExtension{BaseExtension: common.NewBaseExtension("hdfs-discovery")}
}

// PreReconcile is a no-op for discovery.
func (e *DiscoveryExtension) PreReconcile(_ context.Context, _ client.Client, _ common.ClusterInterface) error {
	return nil
}

// PostReconcile renders the discovery config and applies the ConfigMap via the SDK's shared
// ensure-helper (idempotent CreateOrUpdate + owner reference + canonical labels).
func (e *DiscoveryExtension) PostReconcile(ctx context.Context, k8sClient client.Client, cr common.ClusterInterface) error {
	hdfs, ok := cr.(*hdfsv1alpha1.HdfsCluster)
	if !ok {
		return fmt.Errorf("expected *HdfsCluster, got %T", cr)
	}

	generator := config.NewMultiFormatConfigGenerator()
	generator.RegisterDefaultFormats()
	data, err := generator.GenerateFiles(product.DiscoveryConfig(hdfs))
	if err != nil {
		return fmt.Errorf("render discovery config: %w", err)
	}

	return reconciler.EnsureDiscoveryConfigMap(ctx, k8sClient, k8sClient.Scheme(), hdfs, hdfs.Name, data,
		reconciler.WithDiscoveryProductName(constants.ProductName),
	)
}

// OnReconcileError is a no-op for discovery.
func (e *DiscoveryExtension) OnReconcileError(_ context.Context, _ client.Client, _ common.ClusterInterface, _ error) error {
	return nil
}

// Ensure DiscoveryExtension satisfies the SDK ClusterExtension contract.
var _ common.ClusterExtension[common.ClusterInterface] = &DiscoveryExtension{}
