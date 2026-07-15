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
	"path"
	"slices"
	"strconv"
	"strings"

	commonsv1alpha1 "github.com/zncdatadev/operator-go/pkg/apis/commons/v1alpha1"
	"github.com/zncdatadev/operator-go/pkg/constant"
	"github.com/zncdatadev/operator-go/pkg/reconciler"

	hdfsv1alpha1 "github.com/zncdatadev/hdfs-operator/api/v1alpha1"
	"github.com/zncdatadev/hdfs-operator/internal/constants"
)

const (
	defaultJksPassword = "changeit"
	// kerberosRealm is resolved at runtime from krb5.conf and exported as KERBEROS_REALM; the
	// principals below reference it so the config is realm-agnostic.
	kerberosRealm          = "${env.KERBEROS_REALM}"
	dataTransferProtection = "privacy"
	keytabFile             = "keytab"
	authKerberos           = "kerberos"
	keyFsDefaultFS         = "fs.defaultFS"
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
	overrides := map[string]map[string]string{
		constants.CoreSiteXML: coreSiteConfig(cr),
		constants.HdfsSiteXML: hdfsSiteConfig(cr, roleName),
	}
	// TLS: emit the keystore/truststore config files referencing the SecretProvisioner mount.
	if tlsEnabled(cr) {
		overrides[constants.SslServerXML] = sslServerConfig(cr)
		overrides[constants.SslClientXML] = sslClientConfig(cr)
	}
	return &commonsv1alpha1.OverridesSpec{ConfigOverrides: overrides}
}

// tlsEnabled reports whether the CR requests TLS.
func tlsEnabled(cr *hdfsv1alpha1.HdfsCluster) bool {
	return cr.Spec.ClusterConfig != nil &&
		cr.Spec.ClusterConfig.Authentication != nil &&
		cr.Spec.ClusterConfig.Authentication.Tls != nil
}

// jksPassword returns the configured PKCS12 store password, defaulting to "changeit".
func jksPassword(cr *hdfsv1alpha1.HdfsCluster) string {
	if tlsEnabled(cr) && cr.Spec.ClusterConfig.Authentication.Tls.JksPassword != "" {
		return cr.Spec.ClusterConfig.Authentication.Tls.JksPassword
	}
	return defaultJksPassword
}

// tlsStorePath returns the absolute path of a store file inside the TLS secret mount.
func tlsStorePath(file string) string {
	return path.Join(constant.KubedoopMountDir, constants.TlsSecretVolumeName, file)
}

// sslServerConfig / sslClientConfig render ssl-server.xml / ssl-client.xml pointing at the
// PKCS12 keystore/truststore materialized by the TLS secret volume.
func sslServerConfig(cr *hdfsv1alpha1.HdfsCluster) map[string]string {
	pw := jksPassword(cr)
	return map[string]string{
		"ssl.server.truststore.location": tlsStorePath(constants.TruststoreP12),
		"ssl.server.truststore.type":     constants.Pkcs12StoreType,
		"ssl.server.truststore.password": pw,
		"ssl.server.keystore.location":   tlsStorePath(constants.KeystoreP12),
		"ssl.server.keystore.type":       constants.Pkcs12StoreType,
		"ssl.server.keystore.password":   pw,
	}
}

func sslClientConfig(cr *hdfsv1alpha1.HdfsCluster) map[string]string {
	return map[string]string{
		"ssl.client.truststore.location": tlsStorePath(constants.TruststoreP12),
		"ssl.client.truststore.type":     constants.Pkcs12StoreType,
		"ssl.client.truststore.password": jksPassword(cr),
	}
}

// coreSiteConfig builds core-site.xml. fs.defaultFS points at the logical HA nameservice (the
// cluster name), and the ZooKeeper quorum is resolved at runtime from an env var injected from
// the user's zookeeperConfigMap.
func coreSiteConfig(cr *hdfsv1alpha1.HdfsCluster) map[string]string {
	props := map[string]string{
		keyFsDefaultFS:        fmt.Sprintf("hdfs://%s/", cr.Name),
		"ha.zookeeper.quorum": "${env.ZOOKEEPER}",
	}
	if kerberosEnabled(cr) {
		for k, v := range kerberosCoreSite(cr) {
			props[k] = v
		}
	}
	return props
}

// kerberosEnabled reports whether the CR requests Kerberos authentication.
func kerberosEnabled(cr *hdfsv1alpha1.HdfsCluster) bool {
	return cr.Spec.ClusterConfig != nil &&
		cr.Spec.ClusterConfig.Authentication != nil &&
		cr.Spec.ClusterConfig.Authentication.Kerberos != nil
}

// principalHostPart is the shared host/realm suffix of every service principal:
// {cluster}.{namespace}.svc.cluster.local@${env.KERBEROS_REALM}.
func principalHostPart(cr *hdfsv1alpha1.HdfsCluster) string {
	return fmt.Sprintf("%s.%s.svc.cluster.local@%s", cr.Name, cr.Namespace, kerberosRealm)
}

// kerberosCoreSite returns the core-site.xml Kerberos keys: authentication mode, the per-role
// service principals (+ SPNEGO/HTTP), the keytab locations and principal patterns.
func kerberosCoreSite(cr *hdfsv1alpha1.HdfsCluster) map[string]string {
	host := principalHostPart(cr)
	keytab := path.Join(constant.KubedoopKerberosDir, keytabFile)
	return map[string]string{
		"hadoop.security.authentication":                     authKerberos,
		"hadoop.rpc.protection":                              dataTransferProtection,
		"dfs.journalnode.kerberos.principal":                 "jn/" + host,
		"dfs.namenode.kerberos.principal":                    "nn/" + host,
		"dfs.datanode.kerberos.principal":                    "dn/" + host,
		"dfs.web.authentication.kerberos.principal":          "HTTP/" + host,
		"dfs.journalnode.kerberos.internal.spnego.principal": "jn/" + host,
		"dfs.journalnode.keytab.file":                        keytab,
		"dfs.namenode.keytab.file":                           keytab,
		"dfs.datanode.keytab.file":                           keytab,
		"dfs.journalnode.kerberos.principal.pattern":         "jn/" + host,
		"dfs.namenode.kerberos.principal.pattern":            "nn/" + host,
	}
}

// kerberosHdfsSite returns the hdfs-site.xml Kerberos keys: block access tokens, keytab login
// autorenewal, and on-the-wire encryption (privacy).
func kerberosHdfsSite() map[string]string {
	return map[string]string{
		"dfs.block.access.token.enable":                    xmlTrue,
		"hadoop.kerberos.keytab.login.autorenewal.enabled": xmlTrue,
		"dfs.encrypt.data.transfer":                        xmlTrue,
		"dfs.data.transfer.protection":                     dataTransferProtection,
	}
}

// hdfsSiteConfig builds hdfs-site.xml: the shared NameNode HA block plus role-specific data dirs.
// nameNodeHAConfig returns the client-facing NameNode HA block shared by the pod hdfs-site.xml
// and the discovery ConfigMap: the nameservice, the failover proxy provider, the HA namenode id
// list and each NameNode's rpc/http address.
func nameNodeHAConfig(cr *hdfsv1alpha1.HdfsCluster) map[string]string {
	nameservice := cr.Name
	props := map[string]string{
		"dfs.nameservices": nameservice,
		"dfs.client.failover.proxy.provider." + nameservice: failoverProxyProvider,
	}
	tls := tlsEnabled(cr)
	nnPods := nameNodePods(cr)
	ids := make([]string, 0, len(nnPods))
	for _, nn := range nnPods {
		ids = append(ids, nn.id)
		props["dfs.namenode.rpc-address."+nameservice+"."+nn.id] = fmt.Sprintf("%s:%d", nn.fqdn, hdfsv1alpha1.NameNodeRpcPort)
		props["dfs.namenode.http-address."+nameservice+"."+nn.id] = fmt.Sprintf("%s:%d", nn.fqdn, hdfsv1alpha1.NameNodeHttpPort)
		// With dfs.http.policy=HTTPS_ONLY (set when TLS is enabled) the NameNode web UI/API binds
		// to the https-address, so clients need it to reach the NameNodes over TLS.
		if tls {
			props["dfs.namenode.https-address."+nameservice+"."+nn.id] = fmt.Sprintf("%s:%d", nn.fqdn, hdfsv1alpha1.NameNodeHttpsPort)
		}
	}
	props["dfs.ha.namenodes."+nameservice] = strings.Join(ids, ",")
	return props
}

func hdfsSiteConfig(cr *hdfsv1alpha1.HdfsCluster, roleName string) map[string]string {
	nameservice := cr.Name
	props := nameNodeHAConfig(cr)

	props["dfs.replication"] = strconv.Itoa(int(dfsReplication(cr)))
	// Automatic failover + fencing (ZKFC drives failover; fencing is a no-op because
	// StatefulSet guarantees at-most-one pod per ordinal).
	props["dfs.ha.automatic-failover.enabled"] = xmlTrue
	props["dfs.ha.fencing.methods"] = "shell(/bin/true)"
	props["dfs.ha.namenode.id"] = "${env.POD_NAME}"
	props["dfs.namenode.datanode.registration.unsafe.allow-address-override"] = xmlTrue
	// DataNode registers with its externally reachable address/ports (set via env by the pod),
	// so clients reach it through the listener rather than the pod IP.
	props["dfs.datanode.registered.hostname"] = "${env.POD_ADDRESS}"
	props["dfs.datanode.registered.ipc.port"] = "${env.IPC_PORT}"
	props["dfs.datanode.registered.port"] = "${env.DATA_PORT}"
	// JournalNode quorum shared edits + on-disk dirs.
	props["dfs.namenode.shared.edits.dir"] = sharedEditsURI(cr, nameservice)
	props["dfs.journalnode.edits.dir"] = hdfsv1alpha1.JournalNodeRootDataDir
	props["dfs.namenode.name.dir"] = hdfsv1alpha1.NameNodeRootDataDir

	// Per-NameNode name.dir (pod-local, not part of the discovery block).
	for _, nn := range nameNodePods(cr) {
		props["dfs.namenode.name.dir."+nameservice+"."+nn.id] = hdfsv1alpha1.NameNodeRootDataDir
	}

	if roleName == hdfsv1alpha1.DataNodeRoleName {
		props["dfs.datanode.data.dir"] = hdfsv1alpha1.DataNodeRootDataDirPrefix + "0" + hdfsv1alpha1.DataNodeRootDataDirSuffix
	}

	if tlsEnabled(cr) {
		props["dfs.http.policy"] = "HTTPS_ONLY"
		props["dfs.https.server.keystore.resource"] = constants.SslServerXML
		props["dfs.https.client.keystore.resource"] = constants.SslClientXML
	}

	if kerberosEnabled(cr) {
		for k, v := range kerberosHdfsSite() {
			props[k] = v
		}
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

// DiscoveryConfig returns the client-facing core-site.xml / hdfs-site.xml for the cluster-level
// discovery ConfigMap: enough for an external client to reach the HA NameNodes (nameservice,
// failover proxy, per-NameNode rpc/http addresses) plus the Kerberos client keys when enabled.
func DiscoveryConfig(cr *hdfsv1alpha1.HdfsCluster) map[string]map[string]string {
	core := map[string]string{
		keyFsDefaultFS: fmt.Sprintf("hdfs://%s/", cr.Name),
	}
	hdfs := nameNodeHAConfig(cr)

	if kerberosEnabled(cr) {
		for k, v := range kerberosCoreSite(cr) {
			core[k] = v
		}
		for k, v := range kerberosHdfsSite() {
			hdfs[k] = v
		}
	}

	return map[string]map[string]string{
		constants.CoreSiteXML: core,
		constants.HdfsSiteXML: hdfs,
	}
}

// NameNodePodNames returns every NameNode pod name (across all NameNode role groups, sorted).
// These double as the HA namenode ids used by `hdfs haadmin -getServiceState` in the
// format-namenode and wait-for-namenodes containers.
func NameNodePodNames(cr *hdfsv1alpha1.HdfsCluster) []string {
	pods := nameNodePods(cr)
	names := make([]string, 0, len(pods))
	for _, p := range pods {
		names = append(names, p.id)
	}
	return names
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
