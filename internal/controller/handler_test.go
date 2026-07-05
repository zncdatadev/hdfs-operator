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
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"

	hdfsv1alpha1 "github.com/zncdatadev/hdfs-operator/api/v1alpha1"
	"github.com/zncdatadev/hdfs-operator/internal/constants"
)

func envByName(env []corev1.EnvVar, name string) *corev1.EnvVar {
	for i := range env {
		if env[i].Name == name {
			return &env[i]
		}
	}
	return nil
}

func TestCommonEnv(t *testing.T) {
	cr := &hdfsv1alpha1.HdfsCluster{
		Spec: hdfsv1alpha1.HdfsClusterSpec{
			ClusterConfig: &hdfsv1alpha1.ClusterConfigSpec{ZookeeperConfigMapName: "zk-cm"},
		},
	}
	env := commonEnv(cr, "/kubedoop/hadoop/etc/hadoop")

	if got := envByName(env, "HADOOP_CONF_DIR"); got == nil || got.Value != "/kubedoop/hadoop/etc/hadoop" {
		t.Errorf("HADOOP_CONF_DIR = %+v, want value /kubedoop/hadoop/etc/hadoop", got)
	}
	if got := envByName(env, "HADOOP_HOME"); got == nil || got.Value != hdfsv1alpha1.HadoopHome {
		t.Errorf("HADOOP_HOME = %+v, want %q", got, hdfsv1alpha1.HadoopHome)
	}
	if got := envByName(env, "POD_NAME"); got == nil || got.ValueFrom == nil ||
		got.ValueFrom.FieldRef == nil || got.ValueFrom.FieldRef.FieldPath != "metadata.name" {
		t.Errorf("POD_NAME should be a fieldRef to metadata.name, got %+v", got)
	}
	zk := envByName(env, "ZOOKEEPER")
	if zk == nil || zk.ValueFrom == nil || zk.ValueFrom.ConfigMapKeyRef == nil {
		t.Fatalf("ZOOKEEPER should be a configMapKeyRef, got %+v", zk)
	}
	if zk.ValueFrom.ConfigMapKeyRef.Name != "zk-cm" || zk.ValueFrom.ConfigMapKeyRef.Key != "ZOOKEEPER" {
		t.Errorf("ZOOKEEPER ref = %+v, want {zk-cm, ZOOKEEPER}", zk.ValueFrom.ConfigMapKeyRef)
	}
}

func TestCommonEnv_NoZookeeper(t *testing.T) {
	cr := &hdfsv1alpha1.HdfsCluster{Spec: hdfsv1alpha1.HdfsClusterSpec{ClusterConfig: &hdfsv1alpha1.ClusterConfigSpec{}}}
	if zk := envByName(commonEnv(cr, "/x"), "ZOOKEEPER"); zk != nil {
		t.Errorf("ZOOKEEPER should be absent when zookeeperConfigMapName is unset, got %+v", zk)
	}
}

func TestListenerProvisioner(t *testing.T) {
	p := newListenerProvisioner()

	volumes := p.Volumes()
	if len(volumes) != 1 || volumes[0].Name != listenerVolumeName {
		t.Fatalf("Volumes() = %+v, want a single %q volume", volumes, listenerVolumeName)
	}
	mounts := p.VolumeMounts()
	if len(mounts) != 1 || mounts[0].Name != listenerVolumeName {
		t.Fatalf("VolumeMounts() = %+v, want a single %q mount", mounts, listenerVolumeName)
	}
	if want := "/listener"; !strings.HasSuffix(mounts[0].MountPath, want) {
		t.Errorf("listener mount path = %q, want suffix %q", mounts[0].MountPath, want)
	}
}

func TestMainContainerScript(t *testing.T) {
	cases := map[string]string{
		hdfsv1alpha1.NameNodeRoleName:    "exec /kubedoop/hadoop/bin/hdfs namenode",
		hdfsv1alpha1.DataNodeRoleName:    "exec /kubedoop/hadoop/bin/hdfs datanode",
		hdfsv1alpha1.JournalNodeRoleName: "exec /kubedoop/hadoop/bin/hdfs journalnode",
	}
	for role, wantExec := range cases {
		script := mainContainerScript(role)
		if !strings.Contains(script, wantExec) {
			t.Errorf("%s script missing %q:\n%s", role, wantExec, script)
		}
		if !strings.Contains(script, "POD_ADDRESS") {
			t.Errorf("%s script should export POD_ADDRESS from the listener mount", role)
		}
	}
}

func crWithNameNodes() *hdfsv1alpha1.HdfsCluster {
	cr := &hdfsv1alpha1.HdfsCluster{Spec: hdfsv1alpha1.HdfsClusterSpec{ClusterConfig: &hdfsv1alpha1.ClusterConfigSpec{}}}
	cr.Name = "simple-hdfs"
	cr.Spec.NameNodes = &hdfsv1alpha1.NameNodeSpec{}
	return cr
}

func TestInitAndSidecarContainers(t *testing.T) {
	cr := crWithNameNodes()
	const confDir = "/kubedoop/hadoop/etc/hadoop"

	mountNames := func(ms []corev1.VolumeMount) map[string]bool {
		m := map[string]bool{}
		for _, x := range ms {
			m[x.Name] = true
		}
		return m
	}

	fmtNN := formatNameNodeContainer(cr, confDir)
	if fmtNN.Name != formatNameNodeContainerName || !strings.Contains(fmtNN.Args[0], "namenode -format") {
		t.Errorf("format-namenode: name=%q args missing format: %v", fmtNN.Name, fmtNN.Args)
	}
	if m := mountNames(fmtNN.VolumeMounts); !m[configVolumeName] || !m[dataVolumeName] {
		t.Errorf("format-namenode should mount config+data, got %v", fmtNN.VolumeMounts)
	}
	if fmtNN.RestartPolicy != nil {
		t.Errorf("format-namenode is an init container, RestartPolicy should be nil")
	}

	zkfc := zkfcContainer(cr, confDir)
	if zkfc.RestartPolicy == nil || *zkfc.RestartPolicy != corev1.ContainerRestartPolicyAlways {
		t.Errorf("zkfc should be a native sidecar (RestartPolicy=Always), got %v", zkfc.RestartPolicy)
	}
	if !strings.Contains(zkfc.Args[0], "hdfs zkfc") {
		t.Errorf("zkfc args should run 'hdfs zkfc', got %v", zkfc.Args)
	}

	wait := waitForNameNodesContainer(cr, confDir)
	if !strings.Contains(wait.Args[0], "haadmin -getServiceState") {
		t.Errorf("wait-for-namenodes should poll haadmin, got %v", wait.Args)
	}
}

func TestTlsSecretProvisioner(t *testing.T) {
	// disabled: no TLS in cluster config
	if p := tlsSecretProvisioner(crWithNameNodes()); p != nil {
		t.Errorf("expected nil provisioner when TLS disabled, got %v", p)
	}
	// enabled
	cr := crWithNameNodes()
	cr.Spec.ClusterConfig.Authentication = &hdfsv1alpha1.AuthenticationSpec{
		Tls: &hdfsv1alpha1.TlsSpec{SecretClass: constants.DefaultTlsSecretClass, JksPassword: "pw"},
	}
	p := tlsSecretProvisioner(cr)
	if p == nil {
		t.Fatal("expected a provisioner when TLS enabled")
	}
	vols := p.Volumes()
	if len(vols) != 1 || vols[0].Name != constants.TlsSecretVolumeName {
		t.Errorf("expected a single %q volume, got %+v", constants.TlsSecretVolumeName, vols)
	}
}

func TestRoleSidecarManager(t *testing.T) {
	cr := crWithNameNodes()
	if roleSidecarManager(cr, hdfsv1alpha1.NameNodeRoleName, "/x") == nil {
		t.Error("NameNode should have a sidecar manager (format + zkfc)")
	}
	if roleSidecarManager(cr, hdfsv1alpha1.DataNodeRoleName, "/x") == nil {
		t.Error("DataNode should have a sidecar manager (wait-for-namenodes)")
	}
	if roleSidecarManager(cr, hdfsv1alpha1.JournalNodeRoleName, "/x") != nil {
		t.Error("JournalNode should have no sidecar manager")
	}
}
