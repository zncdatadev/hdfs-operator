package common

import corev1 "k8s.io/api/core/v1"

// ContainerBuilder container builder
// contains: image, imagePullPolicy, resource, ports should be required
// optional: name, command, commandArgs, containerEnv, volumeMount, livenessProbe, readinessProbe should be optional,
// optional fields should be implemented by the struct that embeds ContainerBuilder
// optional fields name usually should not be set, because container name can generate by deployment, statefulSet, daemonSet..
type ContainerBuilder struct {
	Image           string
	ImagePullPolicy corev1.PullPolicy
	Resources       corev1.ResourceRequirements
	Ports           []corev1.ContainerPort
}

func NewContainerBuilder(
	Image string,
	ImagePullPolicy corev1.PullPolicy,
	Ports []corev1.ContainerPort,
	Resource corev1.ResourceRequirements,
) *ContainerBuilder {
	return &ContainerBuilder{
		Image:           Image,
		ImagePullPolicy: ImagePullPolicy,
		Ports:           Ports,
		Resources:       Resource,
	}
}

func (b *ContainerBuilder) Build(handler interface{}) corev1.Container {
	container := corev1.Container{
		Image:           b.Image,
		Resources:       b.Resources,
		ImagePullPolicy: b.ImagePullPolicy,
		Ports:           b.Ports,
	}
	if containerName, ok := handler.(ContainerName); ok {
		container.Name = containerName.ContainerName()
	}
	if command, ok := handler.(Command); ok {
		container.Command = command.Command()
	}
	if commandArgs, ok := handler.(CommandArgs); ok {
		container.Args = commandArgs.CommandArgs()
	}
	if containerEnv, ok := handler.(ContainerEnv); ok {
		container.Env = containerEnv.ContainerEnv()
	}
	if volumeMount, ok := handler.(VolumeMount); ok {
		container.VolumeMounts = volumeMount.VolumeMount()
	}
	if livenessProbe, ok := handler.(LivenessProbe); ok {
		container.LivenessProbe = livenessProbe.LivenessProbe()
	}
	if readinessProbe, ok := handler.(ReadinessProbe); ok {
		container.ReadinessProbe = readinessProbe.ReadinessProbe()
	}
	return container
}

type ContainerName interface {
	ContainerName() string
}

type Command interface {
	Command() []string
}

type CommandArgs interface {
	CommandArgs() []string
}

type ContainerEnv interface {
	ContainerEnv() []corev1.EnvVar
}

type VolumeMount interface {
	VolumeMount() []corev1.VolumeMount
}

type LivenessProbe interface {
	LivenessProbe() *corev1.Probe
}

type ReadinessProbe interface {
	ReadinessProbe() *corev1.Probe
}
