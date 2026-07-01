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

// Package product holds HDFS product-intrinsic configuration logic — expressed as data that
// flows through the SDK merge pipeline (product < role < role group), not imperative resource
// construction.
package product

import (
	"fmt"
	"slices"

	commonsv1alpha1 "github.com/zncdatadev/operator-go/pkg/apis/commons/v1alpha1"
	"github.com/zncdatadev/operator-go/pkg/reconciler"

	hdfsv1alpha1 "github.com/zncdatadev/hdfs-operator/api/v1alpha1"
	"github.com/zncdatadev/hdfs-operator/internal/constants"
)

// ComputeConfig is the HDFS ProductConfig hook. It computes the product's core-site.xml /
// hdfs-site.xml for a given role group and returns them as an *OverridesSpec — the same shape
// users write in the CRD. The SDK merges it as the LOWEST layer, so any value a user sets via
// configOverrides always wins.
//
// NOTE (skeleton): this currently emits a minimal, single-NameNode config. HA (nameservices,
// JournalNode quorum), Kerberos, and TLS settings are reintroduced in later refactor phases.
func ComputeConfig(cr *hdfsv1alpha1.HdfsCluster, roleName, _ string) *commonsv1alpha1.OverridesSpec {
	coreSite := map[string]string{
		"fs.defaultFS": defaultFS(cr),
	}

	hdfsSite := map[string]string{
		"dfs.replication": fmt.Sprintf("%d", dfsReplication(cr)),
	}

	switch roleName {
	case hdfsv1alpha1.NameNodeRoleName:
		hdfsSite["dfs.namenode.name.dir"] = hdfsv1alpha1.NameNodeRootDataDir
	case hdfsv1alpha1.DataNodeRoleName:
		hdfsSite["dfs.datanode.data.dir"] = hdfsv1alpha1.DataNodeRootDataDirPrefix + "0" + hdfsv1alpha1.DataNodeRootDataDirSuffix
	case hdfsv1alpha1.JournalNodeRoleName:
		hdfsSite["dfs.journalnode.edits.dir"] = hdfsv1alpha1.JournalNodeRootDataDir
	}

	return &commonsv1alpha1.OverridesSpec{
		ConfigOverrides: map[string]map[string]string{
			constants.CoreSiteXML: coreSite,
			constants.HdfsSiteXML: hdfsSite,
		},
	}
}

// dfsReplication returns the configured replication factor, defaulting to 1.
func dfsReplication(cr *hdfsv1alpha1.HdfsCluster) int32 {
	if cr.Spec.ClusterConfig != nil && cr.Spec.ClusterConfig.DfsReplication > 0 {
		return cr.Spec.ClusterConfig.DfsReplication
	}
	return 1
}

// defaultFS builds the HDFS default filesystem URI from the NameNode RPC Service the framework
// creates. The role group is chosen deterministically (sorted) so the URI is stable across
// reconciles regardless of map iteration order.
func defaultFS(cr *hdfsv1alpha1.HdfsCluster) string {
	return fmt.Sprintf("hdfs://%s:%d", nameNodeServiceName(cr), hdfsv1alpha1.NameNodeRpcPort)
}

// nameNodeServiceName derives the NameNode Service name from the first (sorted) NameNode role
// group, matching the {cluster}-{role}-{group} name the SDK produces.
func nameNodeServiceName(cr *hdfsv1alpha1.HdfsCluster) string {
	groupName := "default"
	if cr.Spec.NameNodes != nil && len(cr.Spec.NameNodes.RoleGroups) > 0 {
		names := make([]string, 0, len(cr.Spec.NameNodes.RoleGroups))
		for g := range cr.Spec.NameNodes.RoleGroups {
			names = append(names, g)
		}
		slices.Sort(names)
		groupName = names[0]
	}
	return reconciler.RoleGroupResourceName(cr.Name, hdfsv1alpha1.NameNodeRoleName, groupName)
}
