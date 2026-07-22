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

package v1alpha1

import (
	"context"
	"testing"

	commonsv1alpha1 "github.com/zncdatadev/operator-go/pkg/apis/commons/v1alpha1"

	hdfsv1alpha1 "github.com/zncdatadev/hdfs-operator/api/v1alpha1"
)

func TestDefault_FillsImageDefaults(t *testing.T) {
	cr := &hdfsv1alpha1.HdfsCluster{}
	if err := (&HdfsClusterCustomDefaulter{}).Default(context.Background(), cr); err != nil {
		t.Fatalf("Default() error: %v", err)
	}
	img := cr.Spec.Image
	if img == nil {
		t.Fatal("image should be initialized")
	}
	if img.Repo != hdfsv1alpha1.DefaultRepository {
		t.Errorf("repo = %q, want %q", img.Repo, hdfsv1alpha1.DefaultRepository)
	}
	if img.ProductVersion != hdfsv1alpha1.DefaultProductVersion {
		t.Errorf("productVersion = %q, want %q", img.ProductVersion, hdfsv1alpha1.DefaultProductVersion)
	}
	if img.KubedoopVersion != hdfsv1alpha1.DefaultKubedoopVersion {
		t.Errorf("kubedoopVersion = %q, want %q", img.KubedoopVersion, hdfsv1alpha1.DefaultKubedoopVersion)
	}
}

func TestDefault_KeepsUserValues(t *testing.T) {
	cr := &hdfsv1alpha1.HdfsCluster{
		Spec: hdfsv1alpha1.HdfsClusterSpec{
			Image: &commonsv1alpha1.ImageSpec{Repo: "my-repo", ProductVersion: "3.3.6"},
		},
	}
	if err := (&HdfsClusterCustomDefaulter{}).Default(context.Background(), cr); err != nil {
		t.Fatalf("Default() error: %v", err)
	}
	if cr.Spec.Image.Repo != "my-repo" || cr.Spec.Image.ProductVersion != "3.3.6" {
		t.Errorf("user-set repo/version must be preserved, got %+v", cr.Spec.Image)
	}
	// only the missing field is filled
	if cr.Spec.Image.KubedoopVersion != hdfsv1alpha1.DefaultKubedoopVersion {
		t.Errorf("missing kubedoopVersion should be defaulted, got %q", cr.Spec.Image.KubedoopVersion)
	}
}

func TestDefault_CustomImageUntouched(t *testing.T) {
	cr := &hdfsv1alpha1.HdfsCluster{
		Spec: hdfsv1alpha1.HdfsClusterSpec{
			Image: &commonsv1alpha1.ImageSpec{Custom: "example.com/hdfs:custom"},
		},
	}
	if err := (&HdfsClusterCustomDefaulter{}).Default(context.Background(), cr); err != nil {
		t.Fatalf("Default() error: %v", err)
	}
	if cr.Spec.Image.Repo != "" || cr.Spec.Image.ProductVersion != "" {
		t.Errorf("a fully custom image must not get repo/version defaults, got %+v", cr.Spec.Image)
	}
}
