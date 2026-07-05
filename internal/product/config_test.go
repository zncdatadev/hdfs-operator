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

// defaultGroup is the role group name used throughout these tests.
const defaultGroup = "default"

func testCluster() *hdfsv1alpha1.HdfsCluster {
	rg := func(replicas int32) commonsv1alpha1.RoleGroupSpec {
		return commonsv1alpha1.RoleGroupSpec{Replicas: ptr.To(replicas)}
	}
	return &hdfsv1alpha1.HdfsCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "simple-hdfs", Namespace: "default"},
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

func TestComputeConfig_HdfsSiteHA(t *testing.T) {
	got := ComputeConfig(testCluster(), hdfsv1alpha1.DataNodeRoleName, defaultGroup).ConfigOverrides["hdfs-site.xml"]

	cases := map[string]string{
		"dfs.nameservices":                  "simple-hdfs",
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
