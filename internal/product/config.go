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
	"strconv"
	"strings"

	commonsv1alpha1 "github.com/zncdatadev/operator-go/pkg/apis/commons/v1alpha1"
	"github.com/zncdatadev/operator-go/pkg/reconciler"

	hdfsv1alpha1 "github.com/zncdatadev/hdfs-operator/api/v1alpha1"
	"github.com/zncdatadev/hdfs-operator/internal/constants"
)

const (
	defaultClusterDomain = "cluster.local"
	// failoverProxyProvider is the HDFS client-side HA failover proxy provider.
	failoverProxyProvider = "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
	// xmlTrue is the string value used for boolean HDFS config properties.
	xmlTrue = "true"
)

// ComputeConfig is the HDFS ProductConfig hook. It computes core-site.xml / hdfs-site.xml for a
// role group and returns them as an *OverridesSpec — the same shape users write in the CRD. The
// SDK merges it as the LOWEST layer, so any value a user sets via configOverrides always wins.
//
// The NameNode HA block (nameservices, per-NameNode rpc/http addresses, the JournalNode quorum
// shared-edits URI) is emitted for every role, because DataNodes and JournalNodes also need to
// resolve the NameNodes.
//
// Some values are Hadoop ${env.VAR} references (ZOOKEEPER, POD_NAME, POD_ADDRESS, IPC_PORT,
// DATA_PORT); the corresponding container env vars are wired by the handler's BuildResources
// (later phase). Kerberos/TLS keys are added in the security phase.
func ComputeConfig(cr *hdfsv1alpha1.HdfsCluster, roleName, _ string) *commonsv1alpha1.OverridesSpec {
	return &commonsv1alpha1.OverridesSpec{
		ConfigOverrides: map[string]map[string]string{
			constants.CoreSiteXML: coreSiteConfig(cr),
			constants.HdfsSiteXML: hdfsSiteConfig(cr, roleName),
		},
	}
}

// coreSiteConfig builds core-site.xml. fs.defaultFS points at the logical HA nameservice (the
// cluster name), and the ZooKeeper quorum is resolved at runtime from an env var injected from
// the user's zookeeperConfigMap.
func coreSiteConfig(cr *hdfsv1alpha1.HdfsCluster) map[string]string {
	return map[string]string{
		"fs.defaultFS":        fmt.Sprintf("hdfs://%s/", cr.Name),
		"ha.zookeeper.quorum": "${env.ZOOKEEPER}",
	}
}

// hdfsSiteConfig builds hdfs-site.xml: the shared NameNode HA block plus role-specific data dirs.
func hdfsSiteConfig(cr *hdfsv1alpha1.HdfsCluster, roleName string) map[string]string {
	nameservice := cr.Name
	props := map[string]string{
		"dfs.nameservices": nameservice,
		"dfs.client.failover.proxy.provider." + nameservice: failoverProxyProvider,
		"dfs.replication": strconv.Itoa(int(dfsReplication(cr))),

		// Automatic failover + fencing (ZKFC drives failover; fencing is a no-op because
		// StatefulSet guarantees at-most-one pod per ordinal).
		"dfs.ha.automatic-failover.enabled": xmlTrue,
		"dfs.ha.fencing.methods":            "shell(/bin/true)",
		"dfs.ha.namenode.id":                "${env.POD_NAME}",
		"dfs.namenode.datanode.registration.unsafe.allow-address-override": xmlTrue,

		// DataNode registers with its externally reachable address/ports (set via env by the
		// pod), so clients reach it through the listener rather than the pod IP.
		"dfs.datanode.registered.hostname": "${env.POD_ADDRESS}",
		"dfs.datanode.registered.ipc.port": "${env.IPC_PORT}",
		"dfs.datanode.registered.port":     "${env.DATA_PORT}",

		// JournalNode quorum shared edits + on-disk dirs.
		"dfs.namenode.shared.edits.dir": sharedEditsURI(cr, nameservice),
		"dfs.journalnode.edits.dir":     hdfsv1alpha1.JournalNodeRootDataDir,
		"dfs.namenode.name.dir":         hdfsv1alpha1.NameNodeRootDataDir,
	}

	// Enumerate every NameNode pod as an HA namenode id, with its rpc/http/name.dir entries.
	nnPods := nameNodePods(cr)
	ids := make([]string, 0, len(nnPods))
	for _, nn := range nnPods {
		ids = append(ids, nn.id)
		props["dfs.namenode.rpc-address."+nameservice+"."+nn.id] = fmt.Sprintf("%s:%d", nn.fqdn, hdfsv1alpha1.NameNodeRpcPort)
		props["dfs.namenode.http-address."+nameservice+"."+nn.id] = fmt.Sprintf("%s:%d", nn.fqdn, hdfsv1alpha1.NameNodeHttpPort)
		props["dfs.namenode.name.dir."+nameservice+"."+nn.id] = hdfsv1alpha1.NameNodeRootDataDir
	}
	props["dfs.ha.namenodes."+nameservice] = strings.Join(ids, ",")

	if roleName == hdfsv1alpha1.DataNodeRoleName {
		props["dfs.datanode.data.dir"] = hdfsv1alpha1.DataNodeRootDataDirPrefix + "0" + hdfsv1alpha1.DataNodeRootDataDirSuffix
	}

	return props
}

// dfsReplication returns the configured replication factor, defaulting to 1.
func dfsReplication(cr *hdfsv1alpha1.HdfsCluster) int32 {
	if cr.Spec.ClusterConfig != nil && cr.Spec.ClusterConfig.DfsReplication > 0 {
		return cr.Spec.ClusterConfig.DfsReplication
	}
	return 1
}

// clusterDomain returns the cluster DNS domain, defaulting to cluster.local.
func clusterDomain(cr *hdfsv1alpha1.HdfsCluster) string {
	if cr.Spec.ClusterConfig != nil && cr.Spec.ClusterConfig.ClusterDomain != "" {
		return cr.Spec.ClusterConfig.ClusterDomain
	}
	return defaultClusterDomain
}

// nnPod is a single NameNode pod: its HA id (the pod name) and its stable DNS FQDN.
type nnPod struct {
	id   string
	fqdn string
}

// nameNodePods enumerates every NameNode pod across all NameNode role groups (sorted for
// deterministic output). The pod FQDN follows the SDK naming: the StatefulSet and its headless
// Service are {cluster}-{role}-{group} and {cluster}-{role}-{group}-headless respectively, so a
// pod is reachable at {sts}-{ordinal}.{sts}-headless.{ns}.svc.{clusterDomain}.
func nameNodePods(cr *hdfsv1alpha1.HdfsCluster) []nnPod {
	var pods []nnPod
	if cr.Spec.NameNodes == nil {
		return pods
	}
	domain := clusterDomain(cr)
	for _, group := range sortedGroups(cr.Spec.NameNodes.RoleGroups) {
		sts := reconciler.RoleGroupResourceName(cr.Name, hdfsv1alpha1.NameNodeRoleName, group)
		rg := cr.Spec.NameNodes.RoleGroups[group]
		for i := range rg.GetReplicas() {
			podName := fmt.Sprintf("%s-%d", sts, i)
			pods = append(pods, nnPod{
				id:   podName,
				fqdn: fmt.Sprintf("%s.%s-headless.%s.svc.%s", podName, sts, cr.Namespace, domain),
			})
		}
	}
	return pods
}

// sharedEditsURI builds the qjournal:// URI listing every JournalNode pod, terminated by the
// nameservice: qjournal://jn0:8485;jn1:8485;.../nameservice.
func sharedEditsURI(cr *hdfsv1alpha1.HdfsCluster, nameservice string) string {
	if cr.Spec.JournalNodes == nil {
		return ""
	}
	domain := clusterDomain(cr)
	var endpoints []string
	for _, group := range sortedGroups(cr.Spec.JournalNodes.RoleGroups) {
		sts := reconciler.RoleGroupResourceName(cr.Name, hdfsv1alpha1.JournalNodeRoleName, group)
		rg := cr.Spec.JournalNodes.RoleGroups[group]
		for i := range rg.GetReplicas() {
			fqdn := fmt.Sprintf("%s-%d.%s-headless.%s.svc.%s", sts, i, sts, cr.Namespace, domain)
			endpoints = append(endpoints, fmt.Sprintf("%s:%d", fqdn, hdfsv1alpha1.JournalNodeRpcPort))
		}
	}
	return fmt.Sprintf("qjournal://%s/%s", strings.Join(endpoints, ";"), nameservice)
}

// sortedGroups returns the role group names in deterministic (sorted) order.
func sortedGroups(groups map[string]commonsv1alpha1.RoleGroupSpec) []string {
	names := make([]string, 0, len(groups))
	for g := range groups {
		names = append(names, g)
	}
	slices.Sort(names)
	return names
}
