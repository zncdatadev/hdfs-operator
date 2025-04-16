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

func (a *AffinityBuilder) handleTerms(pa PodAffinity) (corev1.PodAffinityTerm, corev1.WeightedPodAffinityTerm) {
	term := corev1.PodAffinityTerm{
		LabelSelector: &metav1.LabelSelector{
			MatchLabels: pa.labels,
		},
		TopologyKey: corev1.LabelHostname,
	}

	if pa.weight == 0 {
		pa.weight = corev1.DefaultHardPodAffinitySymmetricWeight
		affinityLogger.Info("Weight not set for preferred pod affinity, setting to %d", pa.weight)
	}

	weightTerm := corev1.WeightedPodAffinityTerm{
		Weight:          pa.weight,
		PodAffinityTerm: term,
	}

	return term, weightTerm
}

func (a *AffinityBuilder) assignTerm(pa PodAffinity, term corev1.PodAffinityTerm, weightTerm corev1.WeightedPodAffinityTerm,
	terms *affinityTerms) {

	if !pa.affinityRequired {
		if pa.anti {
			terms.antiPreferTerms = append(terms.antiPreferTerms, weightTerm)
		} else {
			terms.preferTerms = append(terms.preferTerms, weightTerm)
		}
		return
	}

	if pa.anti {
		terms.antiRequireTerms = append(terms.antiRequireTerms, term)
	} else {
		terms.requireTerms = append(terms.requireTerms, term)
	}
}

type affinityTerms struct {
	preferTerms      []corev1.WeightedPodAffinityTerm
	requireTerms     []corev1.PodAffinityTerm
	antiPreferTerms  []corev1.WeightedPodAffinityTerm
	antiRequireTerms []corev1.PodAffinityTerm
}

func (a *AffinityBuilder) buildPodAffinity() (*corev1.PodAffinity, *corev1.PodAntiAffinity) {
	terms := &affinityTerms{}

	for _, pa := range a.PodAffinity {
		term, weightTerm := a.handleTerms(pa)
		a.assignTerm(pa, term, weightTerm, terms)
	}

	return &corev1.PodAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution:  terms.requireTerms,
			PreferredDuringSchedulingIgnoredDuringExecution: terms.preferTerms,
		}, &corev1.PodAntiAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution:  terms.antiRequireTerms,
			PreferredDuringSchedulingIgnoredDuringExecution: terms.antiPreferTerms,
		}
}

func (a *AffinityBuilder) Build() *corev1.Affinity {

	podAffinity, podAntiAffinity := a.buildPodAffinity()

	return &corev1.Affinity{
		PodAffinity:     podAffinity,
		PodAntiAffinity: podAntiAffinity,
	}
}
