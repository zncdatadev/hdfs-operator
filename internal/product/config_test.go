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
	"testing"

	commonsv1alpha1 "github.com/zncdatadev/operator-go/pkg/apis/commons/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	hdfsv1alpha1 "github.com/zncdatadev/hdfs-operator/api/v1alpha1"
)

// defaultGroup / clusterName are fixtures used throughout these tests.
const (
	defaultGroup = "default"
	clusterName  = "simple-hdfs"
)

func testCluster() *hdfsv1alpha1.HdfsCluster {
	rg := func(replicas int32) commonsv1alpha1.RoleGroupSpec {
		return commonsv1alpha1.RoleGroupSpec{Replicas: ptr.To(replicas)}
	}
	return &hdfsv1alpha1.HdfsCluster{
		ObjectMeta: metav1.ObjectMeta{Name: clusterName, Namespace: "default"},
		Spec: hdfsv1alpha1.HdfsClusterSpec{
			ClusterConfig: &hdfsv1alpha1.ClusterConfigSpec{DfsReplication: 3},
			NameNodes: &hdfsv1alpha1.NameNodeSpec{RoleSpec: commonsv1alpha1.RoleSpec{
				RoleGroups: map[string]commonsv1alpha1.RoleGroupSpec{defaultGroup: rg(2)},
			}},
			JournalNodes: &hdfsv1alpha1.JournalNodeSpec{RoleSpec: commonsv1alpha1.RoleSpec{
				RoleGroups: map[string]commonsv1alpha1.RoleGroupSpec{defaultGroup: rg(3)},
			}},
			DataNodes: &hdfsv1alpha1.DataNodeSpec{RoleSpec: commonsv1alpha1.RoleSpec{
				RoleGroups: map[string]commonsv1alpha1.RoleGroupSpec{defaultGroup: rg(3)},
			}},
		},
	}
}

func TestComputeConfig_CoreSite(t *testing.T) {
	got := ComputeConfig(testCluster(), hdfsv1alpha1.NameNodeRoleName, defaultGroup).ConfigOverrides["core-site.xml"]
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

func TestComputeConfig_TLS(t *testing.T) {
	cr := testCluster()
	cr.Spec.ClusterConfig.Authentication = &hdfsv1alpha1.AuthenticationSpec{
		Tls: &hdfsv1alpha1.TlsSpec{SecretClass: "tls", JksPassword: "secret123"},
	}
	out := ComputeConfig(cr, hdfsv1alpha1.NameNodeRoleName, defaultGroup).ConfigOverrides

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
}

func TestComputeConfig_NoTLS_NoHTTPSAddress(t *testing.T) {
	out := ComputeConfig(testCluster(), hdfsv1alpha1.NameNodeRoleName, defaultGroup).ConfigOverrides
	for k := range out["hdfs-site.xml"] {
		if len(k) >= 24 && k[:24] == "dfs.namenode.https-addre" {
			t.Errorf("https-address keys should be absent without TLS, found %q", k)
		}
	}
}

func TestComputeConfig_NoTLS(t *testing.T) {
	out := ComputeConfig(testCluster(), hdfsv1alpha1.NameNodeRoleName, defaultGroup).ConfigOverrides
	if _, ok := out["ssl-server.xml"]; ok {
		t.Error("ssl-server.xml should be absent when TLS disabled")
	}
	if _, ok := out["hdfs-site.xml"]["dfs.http.policy"]; ok {
		t.Error("dfs.http.policy should be absent when TLS disabled")
	}
}

func TestDiscoveryConfig(t *testing.T) {
	out := DiscoveryConfig(testCluster())

	core := out["core-site.xml"]
	if core["fs.defaultFS"] != "hdfs://simple-hdfs/" {
		t.Errorf("discovery fs.defaultFS = %q, want hdfs://simple-hdfs/", core["fs.defaultFS"])
	}
	hdfs := out["hdfs-site.xml"]
	if hdfs["dfs.nameservices"] != clusterName {
		t.Errorf("discovery nameservices = %q", hdfs["dfs.nameservices"])
	}
	if hdfs["dfs.ha.namenodes.simple-hdfs"] != "simple-hdfs-namenode-default-0,simple-hdfs-namenode-default-1" {
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

func TestComputeConfig_Kerberos(t *testing.T) {
	cr := testCluster()
	cr.Spec.ClusterConfig.Authentication = &hdfsv1alpha1.AuthenticationSpec{
		Kerberos: &hdfsv1alpha1.KerberosSpec{SecretClass: "kerberos"},
	}
	out := ComputeConfig(cr, hdfsv1alpha1.NameNodeRoleName, defaultGroup).ConfigOverrides

	core := out["core-site.xml"]
	if core["hadoop.security.authentication"] != "kerberos" {
		t.Errorf("hadoop.security.authentication = %q, want kerberos", core["hadoop.security.authentication"])
	}
	wantNN := "nn/simple-hdfs.default.svc.cluster.local@${env.KERBEROS_REALM}"
	if core["dfs.namenode.kerberos.principal"] != wantNN {
		t.Errorf("namenode principal = %q, want %q", core["dfs.namenode.kerberos.principal"], wantNN)
	}
	if core["dfs.namenode.keytab.file"] != "/kubedoop/kerberos/keytab" {
		t.Errorf("namenode keytab = %q, want /kubedoop/kerberos/keytab", core["dfs.namenode.keytab.file"])
	}
	if out["hdfs-site.xml"]["dfs.data.transfer.protection"] != "privacy" {
		t.Errorf("data.transfer.protection should be privacy")
	}
}

func TestComputeConfig_HdfsSiteHA(t *testing.T) {
	got := ComputeConfig(testCluster(), hdfsv1alpha1.DataNodeRoleName, defaultGroup).ConfigOverrides["hdfs-site.xml"]

	cases := map[string]string{
		"dfs.nameservices":                  clusterName,
		"dfs.replication":                   "3",
		"dfs.ha.namenodes.simple-hdfs":      "simple-hdfs-namenode-default-0,simple-hdfs-namenode-default-1",
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
