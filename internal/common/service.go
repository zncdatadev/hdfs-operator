package common

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ServiceBuilder service builder
// contains: name, namespace, labels, ports, these should be required
// optional: clusterIP, serviceType should be optional,
type ServiceBuilder struct {
	Name      string
	Namespace string
	Labels    map[string]string
	Ports     []corev1.ServicePort

	ClusterIP *HeadlessServiceType
	Type      *corev1.ServiceType
}

func NewServiceBuilder(
	name string,
	namespace string,
	labels map[string]string,
	ports []corev1.ServicePort,
) *ServiceBuilder {
	return &ServiceBuilder{
		Name:      name,
		Namespace: namespace,
		Labels:    labels,
		Ports:     ports,
	}
}

func (s *ServiceBuilder) SetClusterIP(ip *HeadlessServiceType) *ServiceBuilder {
	s.ClusterIP = ip
	return s
}

func (s *ServiceBuilder) SetType(t *corev1.ServiceType) *ServiceBuilder {
	s.Type = t
	return s
}

func (s *ServiceBuilder) Build() *corev1.Service {
	svc := corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      s.Name,
			Namespace: s.Namespace,
			Labels:    s.Labels,
		},
		Spec: corev1.ServiceSpec{
			Ports:    s.Ports,
			Selector: s.Labels,
		},
	}

	if s.ClusterIP != nil {
		svc.Spec.ClusterIP = string(*s.ClusterIP)
	}
	if s.Type != nil {
		svc.Spec.Type = *s.Type
	}
	return &svc
}

type HeadlessServiceType string

const (
	Service         HeadlessServiceType = ""
	HeadlessService HeadlessServiceType = "None"
)
