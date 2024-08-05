package common

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
)

var (
	affinityLogger = ctrl.Log.WithName("reconciler").WithName("affinity")
)

type PodAffinity struct {
	affinityRequired bool
	anti             bool
	weight           int32
	labels           map[string]string
}

func NewPodAffinity(labels map[string]string, affinityRequired, anti bool) *PodAffinity {
	return &PodAffinity{
		affinityRequired: affinityRequired,
		anti:             anti,
		labels:           labels,
	}
}

func (p *PodAffinity) Weight(weight int32) *PodAffinity {
	p.weight = weight
	return p
}

type NodeAffinity struct {
	weight int32
}

func (n *NodeAffinity) Weight(weight int32) *NodeAffinity {
	n.weight = weight
	return n
}

type AffinityBuilder struct {
	PodAffinity []PodAffinity
	// NodePreferredAffinity []NodeAffinity
}

func NewAffinityBuilder(
	podAffinity ...PodAffinity,
) *AffinityBuilder {
	return &AffinityBuilder{PodAffinity: podAffinity}
}

func (a *AffinityBuilder) AddPodAffinity(podAffinity PodAffinity) *AffinityBuilder {
	a.PodAffinity = append(a.PodAffinity, podAffinity)
	return a
}

func (a *AffinityBuilder) buildPodAffinity() (*corev1.PodAffinity, *corev1.PodAntiAffinity) {
	var preferTerms []corev1.WeightedPodAffinityTerm
	var requireTerms []corev1.PodAffinityTerm
	var antiPreferTerms []corev1.WeightedPodAffinityTerm
	var antiRequireTerms []corev1.PodAffinityTerm

	for _, pa := range a.PodAffinity {
		if pa.affinityRequired {
			// return required
			term := corev1.PodAffinityTerm{
				LabelSelector: &metav1.LabelSelector{
					MatchLabels: pa.labels,
				},
				TopologyKey: corev1.LabelHostname,
			}
			if pa.anti {
				antiRequireTerms = append(antiRequireTerms, term)
			} else {
				requireTerms = append(requireTerms, term)
			}
		} else {
			if pa.weight == 0 {
				pa.weight = corev1.DefaultHardPodAffinitySymmetricWeight
				affinityLogger.Info("Weight not set for preferred pod affinity, setting to %d", pa.weight)
			}
			// return preferred
			weightTerm := corev1.WeightedPodAffinityTerm{
				Weight: pa.weight,
				PodAffinityTerm: corev1.PodAffinityTerm{
					LabelSelector: &metav1.LabelSelector{
						MatchLabels: pa.labels,
					},
					TopologyKey: corev1.LabelHostname,
				},
			}
			if pa.anti {
				antiPreferTerms = append(antiPreferTerms, weightTerm)
			} else {
				preferTerms = append(preferTerms, weightTerm)
			}
		}
	}

	podAffinity := &corev1.PodAffinity{
		RequiredDuringSchedulingIgnoredDuringExecution:  requireTerms,
		PreferredDuringSchedulingIgnoredDuringExecution: preferTerms,
	}

	podAntiAffinity := &corev1.PodAntiAffinity{
		RequiredDuringSchedulingIgnoredDuringExecution:  antiRequireTerms,
		PreferredDuringSchedulingIgnoredDuringExecution: antiPreferTerms,
	}

	return podAffinity, podAntiAffinity

}

func (a *AffinityBuilder) Build() *corev1.Affinity {

	podAffinity, podAntiAffinity := a.buildPodAffinity()

	return &corev1.Affinity{
		PodAffinity:     podAffinity,
		PodAntiAffinity: podAntiAffinity,
	}
}
