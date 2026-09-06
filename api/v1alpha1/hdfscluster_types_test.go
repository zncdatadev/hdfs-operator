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
	"encoding/json"
	"testing"

	commonsv1alpha1 "github.com/zncdatadev/operator-go/pkg/apis/commons/v1alpha1"
	"github.com/zncdatadev/operator-go/pkg/listener"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/utils/ptr"
)

// GetSpec passes each role spec through verbatim — it no longer defaults storage. A role opts into
// a data PVC via its RoleDeclaration.DataVolume, and the framework builds the VolumeClaimTemplate
// from the effective config.resources.storage (defaulting the capacity to DefaultStorageCapacity).
func TestGetSpecPassesRolesThrough(t *testing.T) {
	cr := &HdfsCluster{
		Spec: HdfsClusterSpec{
			NameNodes: &NameNodeSpec{RoleSpec: RoleSpec{
				RoleGroups: map[string]RoleGroupSpec{
					"default": {}, // no config.resources.storage
				},
			}},
		},
	}

	spec := cr.GetSpec()
	if _, ok := spec.Roles[NameNodeRoleName]; !ok {
		t.Fatal("namenode role missing from GetSpec()")
	}
	// No storage default is stamped in — the group is passed through unchanged.
	if got := spec.Roles[NameNodeRoleName].RoleGroups["default"].Config; got != nil {
		t.Errorf("GetSpec should not default the role group config, got %+v", got)
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
			DataNodes: &DataNodeSpec{RoleSpec: RoleSpec{
				RoleGroups: map[string]RoleGroupSpec{
					"default": {Config: &ConfigSpec{
						RoleGroupConfigSpec: &commonsv1alpha1.RoleGroupConfigSpec{
							Resources: &commonsv1alpha1.ResourcesSpec{
								Storage: &commonsv1alpha1.StorageResource{Capacity: ptr.To(resource.MustParse("5Gi"))},
							},
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

func TestProductConfigUnmarshalAndGenericProjection(t *testing.T) {
	raw := []byte(`{
		"spec": {
			"nameNodes": {
				"config": {"listenerClass": "external-stable"},
				"roleGroups": {
					"default": {
						"config": {
							"listenerClass": "external-unstable",
							"resources": {"storage": {"capacity": "7Gi"}}
						}
					}
				}
			}
		}
	}`)
	var cr HdfsCluster
	if err := json.Unmarshal(raw, &cr); err != nil {
		t.Fatalf("unmarshal HdfsCluster: %v", err)
	}

	roleClass := cr.Spec.NameNodes.Config.ListenerClass
	if roleClass == nil || *roleClass != listener.ListenerClassExternalStable {
		t.Fatalf("role listenerClass = %v, want %q", roleClass, listener.ListenerClassExternalStable)
	}
	group := cr.Spec.NameNodes.RoleGroups["default"]
	if group.Config == nil || group.Config.ListenerClass == nil ||
		*group.Config.ListenerClass != listener.ListenerClassExternalUnstable {
		t.Fatalf("role-group listenerClass = %+v, want %q", group.Config, listener.ListenerClassExternalUnstable)
	}

	genericGroup := cr.GetSpec().Roles[NameNodeRoleName].RoleGroups["default"]
	if genericGroup.Config == nil || genericGroup.Config.Resources == nil ||
		genericGroup.Config.Resources.Storage == nil || genericGroup.Config.Resources.Storage.Capacity == nil {
		t.Fatalf("generic projection lost storage config: %+v", genericGroup.Config)
	}
	if got, want := genericGroup.Config.Resources.Storage.Capacity, resource.MustParse("7Gi"); got.Cmp(want) != 0 {
		t.Errorf("generic storage = %s, want %s", got.String(), want.String())
	}
}
