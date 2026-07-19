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
	"testing"

	commonsv1alpha1 "github.com/zncdatadev/operator-go/pkg/apis/commons/v1alpha1"
	"k8s.io/apimachinery/pkg/api/resource"
)

// A role group that omits storage must receive the default data capacity, so the framework builds
// the data VolumeClaimTemplate the init containers mount. Without it the StatefulSet pod template
// is rejected ("volumeMounts...name: Not found: \"data\"").
func TestGetSpecDefaultsStorage(t *testing.T) {
	cr := &HdfsCluster{
		Spec: HdfsClusterSpec{
			NameNodes: &NameNodeSpec{RoleSpec: commonsv1alpha1.RoleSpec{
				RoleGroups: map[string]commonsv1alpha1.RoleGroupSpec{
					"default": {}, // no config.resources.storage
				},
			}},
		},
	}

	spec := cr.GetSpec()
	got := spec.Roles[NameNodeRoleName].RoleGroups["default"].Config
	if got == nil || got.Resources == nil || got.Resources.Storage == nil {
		t.Fatalf("storage was not defaulted: %+v", got)
	}
	want := resource.MustParse(DefaultDataStorageCapacity)
	if got.Resources.Storage.Capacity.Cmp(want) != 0 {
		t.Errorf("default capacity = %s, want %s", got.Resources.Storage.Capacity.String(), want.String())
	}

	// The CR itself must stay untouched (getters must not mutate).
	if rg := cr.Spec.NameNodes.RoleGroups["default"]; rg.Config != nil {
		t.Errorf("GetSpec mutated the source CR: %+v", rg.Config)
	}
}

// An explicit storage request must be preserved, not overwritten by the default.
func TestGetSpecKeepsExplicitStorage(t *testing.T) {
	cr := &HdfsCluster{
		Spec: HdfsClusterSpec{
			DataNodes: &DataNodeSpec{RoleSpec: commonsv1alpha1.RoleSpec{
				RoleGroups: map[string]commonsv1alpha1.RoleGroupSpec{
					"default": {Config: &commonsv1alpha1.RoleGroupConfigSpec{
						Resources: &commonsv1alpha1.ResourcesSpec{
							Storage: &commonsv1alpha1.StorageResource{Capacity: resource.MustParse("5Gi")},
						},
					}},
				},
			}},
		},
	}

	got := cr.GetSpec().Roles[DataNodeRoleName].RoleGroups["default"].Config.Resources.Storage.Capacity
	if want := resource.MustParse("5Gi"); got.Cmp(want) != 0 {
		t.Errorf("capacity = %s, want %s (explicit request must win)", got.String(), want.String())
	}
}
