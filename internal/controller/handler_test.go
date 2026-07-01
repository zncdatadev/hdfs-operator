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
	"testing"

	corev1 "k8s.io/api/core/v1"

	hdfsv1alpha1 "github.com/zncdatadev/hdfs-operator/api/v1alpha1"
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

func TestRoleStartupCommand(t *testing.T) {
	cases := map[string]string{
		hdfsv1alpha1.NameNodeRoleName:    "exec /kubedoop/hadoop/bin/hdfs namenode",
		hdfsv1alpha1.DataNodeRoleName:    "exec /kubedoop/hadoop/bin/hdfs datanode",
		hdfsv1alpha1.JournalNodeRoleName: "exec /kubedoop/hadoop/bin/hdfs journalnode",
	}
	for role, wantScript := range cases {
		command, args := roleStartupCommand(role)
		if len(command) != 2 || command[0] != "/bin/bash" || command[1] != "-c" {
			t.Errorf("%s command = %v, want [/bin/bash -c]", role, command)
		}
		if len(args) != 1 || args[0] != wantScript {
			t.Errorf("%s args = %v, want [%q]", role, args, wantScript)
		}
	}
}
