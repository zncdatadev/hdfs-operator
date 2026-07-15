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

	"github.com/zncdatadev/operator-go/pkg/productlogging"
	"github.com/zncdatadev/operator-go/pkg/reconciler"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/runtime"

	hdfsv1alpha1 "github.com/zncdatadev/hdfs-operator/api/v1alpha1"
	"github.com/zncdatadev/hdfs-operator/internal/constants"
)

func TestJvmHeapEnv(t *testing.T) {
	withMem := func(q string) *corev1.Container {
		return &corev1.Container{Resources: corev1.ResourceRequirements{
			Limits: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse(q)},
		}}
	}

	// 2Gi * 0.8 / 1Mi = 1638
	e := jvmHeapEnv(hdfsv1alpha1.NameNodeRoleName, withMem("2Gi"))
	if e == nil || e.Name != "HDFS_NAMENODE_OPTS" || e.Value != "-Xmx1638m" {
		t.Errorf("namenode heap env = %+v, want HDFS_NAMENODE_OPTS=-Xmx1638m", e)
	}
	if e := jvmHeapEnv(hdfsv1alpha1.DataNodeRoleName, withMem("1Gi")); e == nil || e.Value != "-Xmx819m" {
		t.Errorf("datanode heap env = %+v, want -Xmx819m", e)
	}
	// no memory limit -> nil (leave image JVM defaults)
	if e := jvmHeapEnv(hdfsv1alpha1.NameNodeRoleName, &corev1.Container{}); e != nil {
		t.Errorf("no memory limit should yield nil, got %+v", e)
	}
	// unknown role -> nil
	if e := jvmHeapEnv("unknown", withMem("2Gi")); e != nil {
		t.Errorf("unknown role should yield nil, got %+v", e)
	}
}

func TestRoleLogging(t *testing.T) {
	h := NewHdfsRoleGroupHandler(runtime.NewScheme())
	for role, cname := range roleContainerNames {
		if got := h.RoleMainContainerName[role]; got != cname {
			t.Errorf("role %q main container = %q, want %q", role, got, cname)
		}
		lc := h.LoggingProducers(role)
		if len(lc) != 1 || lc[0].Container != cname || lc[0].Framework != productlogging.LoggingFrameworkLog4j {
			t.Errorf("role %q logging = %+v, want single {%s, log4j}", role, lc, cname)
		}
	}
	// A role group with no per-role entry falls back to the (empty) global list.
	if lc := h.LoggingProducers("unknown"); len(lc) != 0 {
		t.Errorf("unknown role should have no logging producers, got %+v", lc)
	}
}

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
	cr := &hdfsv1alpha1.HdfsCluster{Spec: hdfsv1alpha1.HdfsClusterSpec{ClusterConfig: &hdfsv1alpha1.ClusterConfigSpec{}}}
	for role, wantExec := range cases {
		script := mainContainerScript(cr, role)
		if !strings.Contains(script, wantExec) {
			t.Errorf("%s script missing %q:\n%s", role, wantExec, script)
		}
		if !strings.Contains(script, "POD_ADDRESS") {
			t.Errorf("%s script should export POD_ADDRESS from the listener mount", role)
		}
		if strings.Contains(script, "KERBEROS_REALM") {
			t.Errorf("%s script should not export KERBEROS_REALM when kerberos is off", role)
		}
	}
}

func TestKerberos(t *testing.T) {
	cr := crWithNameNodes()
	cr.Spec.ClusterConfig.Authentication = &hdfsv1alpha1.AuthenticationSpec{
		Kerberos: &hdfsv1alpha1.KerberosSpec{SecretClass: "kerberos"},
	}

	// secret volume registered per role, named "kerberos"
	p := kerberosSecretProvisioner(cr, hdfsv1alpha1.NameNodeRoleName)
	if p == nil {
		t.Fatal("expected a kerberos provisioner when enabled")
	}
	if vols := p.Volumes(); len(vols) != 1 || vols[0].Name != constants.KerberosSecretVolumeName {
		t.Errorf("expected a single %q volume, got %+v", constants.KerberosSecretVolumeName, vols)
	}

	// KRB5 env on the container
	env := commonEnv(cr, "/x")
	if e := envByName(env, "KRB5_CONFIG"); e == nil || e.Value != "/kubedoop/kerberos/krb5.conf" {
		t.Errorf("KRB5_CONFIG = %+v, want /kubedoop/kerberos/krb5.conf", e)
	}

	// realm export in the startup script
	if !strings.Contains(mainContainerScript(cr, hdfsv1alpha1.NameNodeRoleName), "KERBEROS_REALM") {
		t.Error("main script should export KERBEROS_REALM when kerberos enabled")
	}

	// disabled → nil
	if kerberosSecretProvisioner(crWithNameNodes(), hdfsv1alpha1.NameNodeRoleName) != nil {
		t.Error("expected nil kerberos provisioner when disabled")
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

func hasMount(ms []corev1.VolumeMount, name string) bool {
	for _, m := range ms {
		if m.Name == name {
			return true
		}
	}
	return false
}

func TestKinitInInitContainers(t *testing.T) {
	cr := crWithNameNodes()
	cr.Namespace = "default"

	// Without Kerberos: no kinit, no kerberos mount.
	noKrb := formatNameNodeContainer(cr, "/x")
	if strings.Contains(noKrb.Args[0], "kinit") {
		t.Error("no kinit expected without kerberos")
	}
	if hasMount(noKrb.VolumeMounts, constants.KerberosSecretVolumeName) {
		t.Error("no kerberos mount expected without kerberos")
	}

	// With Kerberos: format-namenode kinits as nn, wait-for-namenodes as dn, both mount the keytab.
	cr.Spec.ClusterConfig.Authentication = &hdfsv1alpha1.AuthenticationSpec{
		Kerberos: &hdfsv1alpha1.KerberosSpec{SecretClass: "kerberos"},
	}
	fmtNN := formatNameNodeContainer(cr, "/x")
	if !strings.Contains(fmtNN.Args[0], "kinit -kt") || !strings.Contains(fmtNN.Args[0], "nn/simple-hdfs.default.svc.cluster.local") {
		t.Errorf("format-namenode should kinit as nn principal:\n%s", fmtNN.Args[0])
	}
	if !hasMount(fmtNN.VolumeMounts, constants.KerberosSecretVolumeName) {
		t.Error("format-namenode should mount the kerberos volume under kerberos")
	}
	wait := waitForNameNodesContainer(cr, "/x")
	if !strings.Contains(wait.Args[0], "dn/simple-hdfs.default.svc.cluster.local") {
		t.Errorf("wait-for-namenodes should kinit as dn principal:\n%s", wait.Args[0])
	}
}

func TestMetricsService(t *testing.T) {
	buildCtx := &reconciler.RoleGroupBuildContext{
		ClusterName:      "simple-hdfs",
		ClusterNamespace: "default",
		RoleName:         hdfsv1alpha1.NameNodeRoleName,
		ResourceName:     "simple-hdfs-namenode-default",
	}
	svc := metricsService(buildCtx)
	if svc == nil {
		t.Fatal("expected a metrics service for namenode")
	}
	if svc.Name != "simple-hdfs-namenode-default-metrics" {
		t.Errorf("name = %q, want simple-hdfs-namenode-default-metrics", svc.Name)
	}
	if svc.Spec.ClusterIP != corev1.ClusterIPNone {
		t.Error("metrics service should be headless")
	}
	if len(svc.Spec.Ports) != 1 || svc.Spec.Ports[0].Name != hdfsv1alpha1.MetricName ||
		svc.Spec.Ports[0].Port != hdfsv1alpha1.NameNodeNativeMetricsHttpPort ||
		svc.Spec.Ports[0].TargetPort.StrVal != hdfsv1alpha1.MetricName {
		t.Errorf("port = %+v, want metric/%d targetPort metric", svc.Spec.Ports, hdfsv1alpha1.NameNodeNativeMetricsHttpPort)
	}
	if svc.Spec.Selector["app.kubernetes.io/component"] != hdfsv1alpha1.NameNodeRoleName {
		t.Errorf("selector component = %q, want namenode", svc.Spec.Selector["app.kubernetes.io/component"])
	}
}

func TestHTTPSContainerPort(t *testing.T) {
	cases := map[string]int32{
		hdfsv1alpha1.NameNodeRoleName:    hdfsv1alpha1.NameNodeHttpsPort,
		hdfsv1alpha1.DataNodeRoleName:    hdfsv1alpha1.DataNodeHttpsPort,
		hdfsv1alpha1.JournalNodeRoleName: hdfsv1alpha1.JournalNodeHttpsPort,
	}
	for role, want := range cases {
		p := httpsContainerPort(role)
		if p == nil || p.ContainerPort != want || p.Name != hdfsv1alpha1.HttpsName {
			t.Errorf("%s https port = %+v, want %d named %q", role, p, want, hdfsv1alpha1.HttpsName)
		}
	}
	if httpsContainerPort("unknown") != nil {
		t.Error("unknown role should have no https port")
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
