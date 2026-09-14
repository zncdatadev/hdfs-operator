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

	"github.com/zncdatadev/operator-go/pkg/testutil"
)

// Structural CRD defaults inside a role or role-group config block destroy the distinction between
// "unset" and "set to the default" before operator-go folds role -> role group. Keep the generated
// schema aligned with the v0.13 contract by applying defaults in declarations or at consumption.
func TestGeneratedCRDHasNoInheritedConfigDefaults(t *testing.T) {
	const crdGlob = "../../config/crd/bases/*.yaml"
	matcher := testutil.HaveNoInheritedConfigDefaults()
	ok, err := matcher.Match(crdGlob)
	if err != nil {
		t.Fatalf("inspect generated CRDs: %v", err)
	}
	if !ok {
		t.Fatal(matcher.FailureMessage(crdGlob))
	}
}
