package common

import (
	stackv1alpha1 "github.com/zncdata-labs/alluxio-operator/api/v1alpha1"
	"strings"
)

type RoleLabels struct {
	Cr   *stackv1alpha1.Alluxio
	Name string
}

func (r *RoleLabels) GetLabels() map[string]string {
	return map[string]string{
		"app.kubernetes.io/Name":       strings.ToLower(r.Cr.Name),
		"app.kubernetes.io/component":  r.Name,
		"app.kubernetes.io/managed-by": "alluxio-operator",
	}
}
