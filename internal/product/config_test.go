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

package product

import (
	"context"
	"strings"
	"testing"

	commonsv1alpha1 "github.com/zncdatadev/operator-go/pkg/apis/commons/v1alpha1"
	"github.com/zncdatadev/operator-go/pkg/listener"
	"github.com/zncdatadev/operator-go/pkg/reconciler"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	hdfsv1alpha1 "github.com/zncdatadev/hdfs-operator/api/v1alpha1"
	"github.com/zncdatadev/hdfs-operator/internal/constants"
)

func TestJvmOptsEnvVars(t *testing.T) {
	cfg := func(mem string) *commonsv1alpha1.RoleGroupConfigSpec {
		q := resource.MustParse(mem)
		return &commonsv1alpha1.RoleGroupConfigSpec{
			Resources: &commonsv1alpha1.ResourcesSpec{Memory: &commonsv1alpha1.MemoryResource{Limit: &q}},
		}
	}

	// NameNode with a 2Gi limit: -Xmx (2Gi*0.8/1Mi = 1638) + the jmx javaagent on port 8183.
	nn := jvmOptsEnvVars(hdfsv1alpha1.NameNodeRoleName, cfg("2Gi"))
	if got := nn["HDFS_NAMENODE_OPTS"]; !strings.Contains(got, "-Xmx1638m") ||
		!strings.Contains(got, "=8183:/kubedoop/jmx/namenode.yaml") {
		t.Errorf("namenode opts = %q, want -Xmx1638m + jmx javaagent on 8183", got)
	}

	// No memory limit: no -Xmx, but the javaagent is still added on the datanode port.
	dn := jvmOptsEnvVars(hdfsv1alpha1.DataNodeRoleName, nil)
	if got := dn["HDFS_DATANODE_OPTS"]; strings.Contains(got, "-Xmx") || !strings.Contains(got, "=8082:/kubedoop/jmx/datanode.yaml") {
		t.Errorf("no-limit datanode opts = %q, want jmx on 8082 and no -Xmx", got)
	}

	// Unknown role -> empty.
	if got := jvmOptsEnvVars("unknown", cfg("2Gi")); len(got) != 0 {
		t.Errorf("unknown role should yield no env, got %+v", got)
	}
}

// mustCompute runs ComputeConfig for the default role group and returns the contribution, failing
// the calling test on the never-expected error. A minimal build context supplies the role name; the
// effective config is empty, which is all these config-content assertions need.
func mustCompute(cr *hdfsv1alpha1.HdfsCluster, roleName string) *reconciler.Contribution {
	rg := &reconciler.RoleGroupBuildContext{RoleName: roleName, RoleGroupName: defaultGroup}
	out, err := ComputeConfig(context.Background(), nil, cr, rg)
	if err != nil {
		panic(err)
	}
	return out
}

// defaultGroup / clusterName are fixtures used throughout these tests.
const (
	defaultGroup       = "default"
	clusterName        = "simple-hdfs"
	defaultNameNodeIDs = "simple-hdfs-namenode-default-0,simple-hdfs-namenode-default-1"
)

func testCluster() *hdfsv1alpha1.HdfsCluster {
	rg := func(replicas int32) hdfsv1alpha1.RoleGroupSpec {
		return hdfsv1alpha1.RoleGroupSpec{Replicas: ptr.To(replicas)}
	}
	return &hdfsv1alpha1.HdfsCluster{
		ObjectMeta: metav1.ObjectMeta{Name: clusterName, Namespace: "default"},
		Spec: hdfsv1alpha1.HdfsClusterSpec{
			ClusterConfig: &hdfsv1alpha1.ClusterConfigSpec{DfsReplication: 3},
			NameNodes: &hdfsv1alpha1.NameNodeSpec{RoleSpec: hdfsv1alpha1.RoleSpec{
				RoleGroups: map[string]hdfsv1alpha1.RoleGroupSpec{defaultGroup: rg(2)},
			}},
			JournalNodes: &hdfsv1alpha1.JournalNodeSpec{RoleSpec: hdfsv1alpha1.RoleSpec{
				RoleGroups: map[string]hdfsv1alpha1.RoleGroupSpec{defaultGroup: rg(3)},
			}},
			DataNodes: &hdfsv1alpha1.DataNodeSpec{RoleSpec: hdfsv1alpha1.RoleSpec{
				RoleGroups: map[string]hdfsv1alpha1.RoleGroupSpec{defaultGroup: rg(3)},
			}},
		},
	}
}

func TestComputeConfig_CoreSite(t *testing.T) {
	got := mustCompute(testCluster(), hdfsv1alpha1.NameNodeRoleName).ConfigOverrides["core-site.xml"]
	want := map[string]string{
		"fs.defaultFS":        "hdfs://simple-hdfs/",
		"ha.zookeeper.quorum": "${env.ZOOKEEPER}",
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("core-site.xml[%q] = %q, want %q", k, got[k], v)
		}
	}
}

func TestComputeConfig_ListenerClassPrecedence(t *testing.T) {
	cr := testCluster()
	compute := func(group string) listener.ListenerClass {
		out, err := ComputeConfig(context.Background(), nil, cr, &reconciler.RoleGroupBuildContext{
			RoleName:      hdfsv1alpha1.NameNodeRoleName,
			RoleGroupName: group,
		})
		if err != nil {
			t.Fatalf("ComputeConfig(%s): %v", group, err)
		}
		return out.ListenerClass
	}

	if got := compute(defaultGroup); got != listener.ListenerClassClusterInternal {
		t.Errorf("default listener class = %q, want %q", got, listener.ListenerClassClusterInternal)
	}

	roleClass := listener.ListenerClassExternalStable
	groupClass := listener.ListenerClassExternalUnstable
	cr.Spec.NameNodes.Config = &hdfsv1alpha1.ConfigSpec{ListenerClass: &roleClass}
	cr.Spec.NameNodes.RoleGroups[defaultGroup] = hdfsv1alpha1.RoleGroupSpec{
		Replicas: ptr.To(int32(2)),
		Config:   &hdfsv1alpha1.ConfigSpec{ListenerClass: &groupClass},
	}
	cr.Spec.NameNodes.RoleGroups["inherited"] = hdfsv1alpha1.RoleGroupSpec{Replicas: ptr.To(int32(1))}

	if got := compute("inherited"); got != roleClass {
		t.Errorf("role-inherited listener class = %q, want %q", got, roleClass)
	}
	if got := compute(defaultGroup); got != groupClass {
		t.Errorf("role-group listener class = %q, want %q", got, groupClass)
	}
}

func TestComputeConfig_TLS(t *testing.T) {
	cr := testCluster()
	cr.Spec.ClusterConfig.Authentication = &hdfsv1alpha1.AuthenticationSpec{
		Tls: &hdfsv1alpha1.TlsSpec{SecretClass: "tls", JksPassword: "secret123"},
	}
	out := mustCompute(cr, hdfsv1alpha1.NameNodeRoleName).ConfigOverrides

	if got := out["hdfs-site.xml"]["dfs.http.policy"]; got != "HTTPS_ONLY" {
		t.Errorf("dfs.http.policy = %q, want HTTPS_ONLY", got)
	}
	ssl := out["ssl-server.xml"]
	if ssl == nil {
		t.Fatal("ssl-server.xml missing when TLS enabled")
	}
	if got := ssl["ssl.server.keystore.location"]; got != "/kubedoop/mount/tls/keystore.p12" {
		t.Errorf("keystore.location = %q, want /kubedoop/mount/tls/keystore.p12", got)
	}
	if got := ssl["ssl.server.keystore.password"]; got != "secret123" {
		t.Errorf("keystore.password = %q, want secret123", got)
	}
	if out["ssl-client.xml"]["ssl.client.truststore.type"] != "pkcs12" {
		t.Errorf("ssl-client truststore.type should be pkcs12")
	}
	// HTTPS_ONLY means clients reach the NameNodes via the https-address.
	wantHTTPS := "simple-hdfs-namenode-default-0.simple-hdfs-namenode-default-headless.default.svc.cluster.local:9871"
	if got := out["hdfs-site.xml"]["dfs.namenode.https-address.simple-hdfs.simple-hdfs-namenode-default-0"]; got != wantHTTPS {
		t.Errorf("https-address = %q, want %q", got, wantHTTPS)
	}
	hs := out["hdfs-site.xml"]
	if hs["dfs.datanode.registered.https.port"] != "${env.HTTPS_PORT}" {
		t.Errorf("registered.https.port = %q, want ${env.HTTPS_PORT}", hs["dfs.datanode.registered.https.port"])
	}
	if _, ok := hs["dfs.datanode.registered.http.port"]; ok {
		t.Error("registered.http.port must be absent under TLS")
	}
}

func TestComputeConfig_NoTLS_NoHTTPSAddress(t *testing.T) {
	out := mustCompute(testCluster(), hdfsv1alpha1.NameNodeRoleName).ConfigOverrides
	for k := range out["hdfs-site.xml"] {
		if len(k) >= 24 && k[:24] == "dfs.namenode.https-addre" {
			t.Errorf("https-address keys should be absent without TLS, found %q", k)
		}
	}
}

func TestComputeConfig_NoTLS(t *testing.T) {
	out := mustCompute(testCluster(), hdfsv1alpha1.NameNodeRoleName).ConfigOverrides
	if _, ok := out["ssl-server.xml"]; ok {
		t.Error("ssl-server.xml should be absent when TLS disabled")
	}
	if _, ok := out["hdfs-site.xml"]["dfs.http.policy"]; ok {
		t.Error("dfs.http.policy should be absent when TLS disabled")
	}
}

func TestDiscoveryConfig(t *testing.T) {
	out := DiscoveryConfig(testCluster(), nil)

	core := out["core-site.xml"]
	if core["fs.defaultFS"] != "hdfs://simple-hdfs/" {
		t.Errorf("discovery fs.defaultFS = %q, want hdfs://simple-hdfs/", core["fs.defaultFS"])
	}
	hdfs := out["hdfs-site.xml"]
	if hdfs[keyDfsNameservices] != clusterName {
		t.Errorf("discovery nameservices = %q", hdfs[keyDfsNameservices])
	}
	if hdfs["dfs.ha.namenodes.simple-hdfs"] != defaultNameNodeIDs {
		t.Errorf("discovery ha.namenodes = %q", hdfs["dfs.ha.namenodes.simple-hdfs"])
	}
	want := "simple-hdfs-namenode-default-0.simple-hdfs-namenode-default-headless.default.svc.cluster.local:8020"
	if hdfs["dfs.namenode.rpc-address.simple-hdfs.simple-hdfs-namenode-default-0"] != want {
		t.Errorf("discovery rpc-address = %q, want %q", hdfs["dfs.namenode.rpc-address.simple-hdfs.simple-hdfs-namenode-default-0"], want)
	}
	// pod-local keys must NOT leak into the client discovery config.
	for _, k := range []string{"dfs.namenode.name.dir", "dfs.datanode.registered.hostname", "dfs.ha.namenode.id"} {
		if _, ok := hdfs[k]; ok {
			t.Errorf("discovery hdfs-site should not contain pod-local key %q", k)
		}
	}
}

func TestDiscoveryConfigUsesResolvedEndpointsInStablePodOrder(t *testing.T) {
	cr := testCluster()
	endpoints := InternalNameNodeDiscoveryEndpoints(cr)
	endpoints["simple-hdfs-namenode-default-0"] = DiscoveryEndpoint{
		Address: "nn-0.example.test",
		Ports: map[string]int32{
			hdfsv1alpha1.RpcName:  31020,
			hdfsv1alpha1.HttpName: 31070,
		},
	}
	endpoints["simple-hdfs-namenode-default-1"] = DiscoveryEndpoint{
		Address: "nn-1.example.test",
		Ports: map[string]int32{
			hdfsv1alpha1.RpcName:  32020,
			hdfsv1alpha1.HttpName: 32070,
		},
	}

	hdfs := DiscoveryConfig(cr, endpoints)["hdfs-site.xml"]
	if got := hdfs["dfs.ha.namenodes.simple-hdfs"]; got != defaultNameNodeIDs {
		t.Fatalf("discovery HA ids = %q", got)
	}
	if got := hdfs["dfs.namenode.rpc-address.simple-hdfs.simple-hdfs-namenode-default-0"]; got != "nn-0.example.test:31020" {
		t.Errorf("resolved rpc endpoint = %q", got)
	}
	if got := hdfs["dfs.namenode.http-address.simple-hdfs.simple-hdfs-namenode-default-1"]; got != "nn-1.example.test:32070" {
		t.Errorf("resolved http endpoint = %q", got)
	}
}

func TestDiscoveryConfigBracketsIPv6ListenerAddress(t *testing.T) {
	cr := testCluster()
	endpoints := InternalNameNodeDiscoveryEndpoints(cr)
	endpoints["simple-hdfs-namenode-default-0"] = DiscoveryEndpoint{
		Address: "2001:db8::10",
		Ports: map[string]int32{
			hdfsv1alpha1.RpcName:  hdfsv1alpha1.NameNodeRpcPort,
			hdfsv1alpha1.HttpName: hdfsv1alpha1.NameNodeHttpPort,
		},
	}

	hdfs := DiscoveryConfig(cr, endpoints)[constants.HdfsSiteXML]
	key := "dfs.namenode.rpc-address.simple-hdfs.simple-hdfs-namenode-default-0"
	if got, want := hdfs[key], "[2001:db8::10]:8020"; got != want {
		t.Errorf("IPv6 discovery endpoint = %q, want %q", got, want)
	}
}

func TestComputeConfig_Kerberos(t *testing.T) {
	cr := testCluster()
	cr.Spec.ClusterConfig.Authentication = &hdfsv1alpha1.AuthenticationSpec{
		Kerberos: &hdfsv1alpha1.KerberosSpec{SecretClass: "kerberos"},
	}
	out := mustCompute(cr, hdfsv1alpha1.NameNodeRoleName).ConfigOverrides

	core := out["core-site.xml"]
	if core["hadoop.security.authentication"] != "kerberos" {
		t.Errorf("hadoop.security.authentication = %q, want kerberos", core["hadoop.security.authentication"])
	}
	wantNN := "nn/simple-hdfs.default.svc.cluster.local@${env.KERBEROS_REALM}"
	if core["dfs.namenode.kerberos.principal"] != wantNN {
		t.Errorf("namenode principal = %q, want %q", core["dfs.namenode.kerberos.principal"], wantNN)
	}
	if core["dfs.namenode.keytab.file"] != "/kubedoop/mount/kerberos/keytab" {
		t.Errorf("namenode keytab = %q, want /kubedoop/mount/kerberos/keytab", core["dfs.namenode.keytab.file"])
	}
	if out["hdfs-site.xml"]["dfs.data.transfer.protection"] != "privacy" {
		t.Errorf("data.transfer.protection should be privacy")
	}
}

func TestComputeConfig_HdfsSiteHA(t *testing.T) {
	got := mustCompute(testCluster(), hdfsv1alpha1.DataNodeRoleName).ConfigOverrides["hdfs-site.xml"]

	cases := map[string]string{
		keyDfsNameservices:                  clusterName,
		"dfs.replication":                   "3",
		"dfs.ha.namenodes.simple-hdfs":      defaultNameNodeIDs,
		"dfs.ha.automatic-failover.enabled": "true",
		// NameNode pod FQDN must use the "-headless" service suffix produced by the SDK.
		"dfs.namenode.rpc-address.simple-hdfs.simple-hdfs-namenode-default-0":  "simple-hdfs-namenode-default-0.simple-hdfs-namenode-default-headless.default.svc.cluster.local:8020",
		"dfs.namenode.http-address.simple-hdfs.simple-hdfs-namenode-default-1": "simple-hdfs-namenode-default-1.simple-hdfs-namenode-default-headless.default.svc.cluster.local:9870",
		// JournalNode quorum: all 3 JN pods, terminated by the nameservice.
		"dfs.namenode.shared.edits.dir": "qjournal://" +
			"simple-hdfs-journalnode-default-0.simple-hdfs-journalnode-default-headless.default.svc.cluster.local:8485;" +
			"simple-hdfs-journalnode-default-1.simple-hdfs-journalnode-default-headless.default.svc.cluster.local:8485;" +
			"simple-hdfs-journalnode-default-2.simple-hdfs-journalnode-default-headless.default.svc.cluster.local:8485/simple-hdfs",
		// DataNode-specific data dir.
		"dfs.datanode.data.dir": "/kubedoop/data/0/datanode",
	}
	for k, want := range cases {
		if got[k] != want {
			t.Errorf("hdfs-site.xml[%q]\n  got  = %q\n  want = %q", k, got[k], want)
		}
	}
}
