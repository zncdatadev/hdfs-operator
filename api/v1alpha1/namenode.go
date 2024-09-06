package v1alpha1

import (
	commonsv1alpha1 "github.com/zncdatadev/operator-go/pkg/apis/commons/v1alpha1"
	corev1 "k8s.io/api/core/v1"
)

type NameNodeRoleGroupSpec struct {
	// +kubebuilder:validation:Optional
	// +kubebuilder:default:=1
	Replicas int32 `json:"replicas,omitempty"`

	// +kubebuilder:validation:Required
	Config *NameNodeConfigSpec `json:"config,omitempty"`

	// +kubebuilder:validation:Optional
	CommandArgsOverrides []string `json:"commandArgsOverrides,omitempty"`

	// +kubebuilder:validation:Optional
	ConfigOverrides *ConfigOverridesSpec `json:"configOverrides,omitempty"`

	// +kubebuilder:validation:Optional
	EnvOverrides map[string]string `json:"envOverrides,omitempty"`
}

type NameNodeConfigSpec struct {
	// +kubebuilder:validation:Optional
	Resources *commonsv1alpha1.ResourcesSpec `json:"resources,omitempty"`

	// +kubebuilder:validation:Optional
	// +kubebuilder:default="external-stable"
	ListenerClass string `json:"listenerClass,omitempty"`

	// +kubebuilder:validation:Optional
	SecurityContext *corev1.PodSecurityContext `json:"securityContext"`

	// +kubebuilder:validation:Optional
	Affinity *corev1.Affinity `json:"affinity"`

	// +kubebuilder:validation:Optional
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`

	// +kubebuilder:validation:Optional
	Tolerations []corev1.Toleration `json:"tolerations"`

	// +kubebuilder:validation:Optional
	PodDisruptionBudget *PodDisruptionBudgetSpec `json:"podDisruptionBudget,omitempty"`

	// +kubebuilder:validation:Optional
	StorageClass string `json:"storageClass,omitempty"`

	// +kubebuilder:validation:Optional
	// +kubebuilder:default="8Gi"
	StorageSize string `json:"storageSize,omitempty"`

	// +kubebuilder:validation:Optional
	ExtraEnv map[string]string `json:"extraEnv,omitempty"`

	// +kubebuilder:validation:Optional
	ExtraSecret map[string]string `json:"extraSecret,omitempty"`

	// +kubebuilder:validation:Optional
	Logging *NameNodeContainerLoggingSpec `json:"logging,omitempty"`
}
type NameNodeContainerLoggingSpec struct {
	// +kubebuilder:validation:Optional
	// +kubebuilder:default=false
	EnableVectorAgent bool `json:"enableVectorAgent,omitempty"`
	// +kubebuilder:validation:Optional
	NameNode *LoggingConfigSpec `json:"namenode,omitempty"`
	// +kubebuilder:validation:Optional
	Zkfc *LoggingConfigSpec `json:"zkfc,omitempty"`

	// +kubebuilder:validation:Optional
	FormatZookeeper *LoggingConfigSpec `json:"formatZookeeper,omitempty"`
	// +kubebuilder:validation:Optional
	FormatNameNode *LoggingConfigSpec `json:"formatNameNode,omitempty"`
}
