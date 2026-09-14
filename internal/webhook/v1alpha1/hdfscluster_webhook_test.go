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

func TestValidateCreate_ResolvableImages(t *testing.T) {
	const productVersion = "3.4.1"
	cases := []struct {
		name  string
		image *commonsv1alpha1.ImageSpec
	}{
		{"nil image (handler defaults it)", nil},
		{"productVersion only (handler fills the rest)", &commonsv1alpha1.ImageSpec{ProductVersion: productVersion}},
		{"full structured image", &commonsv1alpha1.ImageSpec{Repo: "quay.io/zncdatadev", ProductVersion: productVersion, KubedoopVersion: "0.0.0-dev"}},
		{"valid custom image", &commonsv1alpha1.ImageSpec{Custom: "example.com/hadoop:custom"}},
	}
	v := &HdfsClusterCustomValidator{}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cr := &hdfsv1alpha1.HdfsCluster{Spec: hdfsv1alpha1.HdfsClusterSpec{Image: tc.image}}
			if _, err := v.ValidateCreate(context.Background(), cr); err != nil {
				t.Errorf("ValidateCreate() unexpected error: %v", err)
			}
		})
	}
}

func TestValidateCreate_InvalidCustomImage(t *testing.T) {
	cr := &hdfsv1alpha1.HdfsCluster{
		Spec: hdfsv1alpha1.HdfsClusterSpec{
			Image: &commonsv1alpha1.ImageSpec{Custom: "INVALID IMAGE!"},
		},
	}
	if _, err := (&HdfsClusterCustomValidator{}).ValidateCreate(context.Background(), cr); err == nil {
		t.Error("ValidateCreate() should reject a malformed custom image")
	}
}

func TestValidateUpdate_ImageImmutable(t *testing.T) {
	oldCR := &hdfsv1alpha1.HdfsCluster{
		Spec: hdfsv1alpha1.HdfsClusterSpec{Image: &commonsv1alpha1.ImageSpec{ProductVersion: "3.4.1"}},
	}
	newCR := &hdfsv1alpha1.HdfsCluster{
		Spec: hdfsv1alpha1.HdfsClusterSpec{Image: &commonsv1alpha1.ImageSpec{ProductVersion: "3.3.6"}},
	}
	if _, err := (&HdfsClusterCustomValidator{}).ValidateUpdate(context.Background(), oldCR, newCR); err == nil {
		t.Error("ValidateUpdate() should reject a change to spec.image")
	}

	// Same image on both sides is allowed.
	if _, err := (&HdfsClusterCustomValidator{}).ValidateUpdate(context.Background(), oldCR, oldCR.DeepCopy()); err != nil {
		t.Errorf("ValidateUpdate() with unchanged image: unexpected error: %v", err)
	}
}
