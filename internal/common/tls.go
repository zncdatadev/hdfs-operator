package common

import (
	"fmt"
	hdfsv1alpha1 "github.com/zncdatadev/hdfs-operator/api/v1alpha1"
	"github.com/zncdatadev/hdfs-operator/internal/util"
	"github.com/zncdatadev/operator-go/pkg/constants"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

const TlsVolumeName = "tls"

func IsTlsEnabled(clusterSpec *hdfsv1alpha1.ClusterConfigSpec) bool {
	return clusterSpec.Authentication != nil && clusterSpec.Authentication.Tls != nil
}

func HttpPort(clusterSpec *hdfsv1alpha1.ClusterConfigSpec, httpsPort int32, httpPort int32) corev1.ContainerPort {
	if IsTlsEnabled(clusterSpec) {
		return corev1.ContainerPort{
			Name:          hdfsv1alpha1.HttpsName,
			ContainerPort: httpsPort,
			Protocol:      corev1.ProtocolTCP,
		}
	} else {
		return corev1.ContainerPort{
			Name:          hdfsv1alpha1.HttpName,
			ContainerPort: httpPort,
			Protocol:      corev1.ProtocolTCP,
		}
	}
}

func ServiceHttpPort(clusterSpec *hdfsv1alpha1.ClusterConfigSpec, svcHttpsPort int32, svcHttpPort int32) corev1.ServicePort {
	if IsTlsEnabled(clusterSpec) {
		return corev1.ServicePort{
			Name:       hdfsv1alpha1.HttpsName,
			Port:       svcHttpsPort,
			Protocol:   corev1.ProtocolTCP,
			TargetPort: intstr.FromString(hdfsv1alpha1.HttpsName),
		}
	} else {
		return corev1.ServicePort{
			Name:       hdfsv1alpha1.HttpName,
			Port:       svcHttpPort,
			Protocol:   corev1.ProtocolTCP,
			TargetPort: intstr.FromString(hdfsv1alpha1.HttpName),
		}
	}
}

func DfsNameNodeHttpAddressHa(
	clusterSpec *hdfsv1alpha1.ClusterConfigSpec,
	instanceName string,
	statefulsetName string,
	svcName string,
	namespace string) (dnsDomain string, keyTemplate string) {
	if IsTlsEnabled(clusterSpec) {
		dnsDomain = CreateDnsDomain(svcName, namespace, clusterSpec.ClusterDomain, hdfsv1alpha1.NameNodeHttpsPort)
		keyTemplate = fmt.Sprintf("dfs.namenode.https-address.%s.%s-%%d", instanceName, statefulsetName)
	} else {
		dnsDomain = CreateDnsDomain(svcName, namespace, clusterSpec.ClusterDomain, hdfsv1alpha1.NameNodeHttpPort)
		keyTemplate = fmt.Sprintf("dfs.namenode.http-address.%s.%s-%%d", instanceName, statefulsetName)
	}
	return dnsDomain, keyTemplate
}

func WebUiPortProbe(clusterSpec *hdfsv1alpha1.ClusterConfigSpec) corev1.URIScheme {
	if IsTlsEnabled(clusterSpec) {
		return corev1.URISchemeHTTPS
	} else {
		return corev1.URISchemeHTTP
	}
}

func TlsHttpGetAction(clusterSpec *hdfsv1alpha1.ClusterConfigSpec, probePath string) *corev1.HTTPGetAction {
	if IsTlsEnabled(clusterSpec) {
		return &corev1.HTTPGetAction{
			Path:   probePath,
			Port:   intstr.FromString(hdfsv1alpha1.HttpsName),
			Scheme: corev1.URISchemeHTTPS,
		}
	} else {
		return &corev1.HTTPGetAction{
			Path:   probePath,
			Port:   intstr.FromString(hdfsv1alpha1.HttpName),
			Scheme: corev1.URISchemeHTTP,
		}
	}
}

func TlsVolumeMounts() []corev1.VolumeMount {
	return []corev1.VolumeMount{
		{
			Name:      TlsVolumeName,
			MountPath: hdfsv1alpha1.TlsMountPath,
		},
	}
}

func TlsHdfsSiteXml(clusterSpec *hdfsv1alpha1.ClusterConfigSpec) []util.XmlNameValuePair {
	if IsTlsEnabled(clusterSpec) {
		return []util.XmlNameValuePair{
			{
				Name:  "dfs.datanode.registered.https.port",
				Value: "${env.HTTPS_PORT}",
			},
		}
	} else {
		return []util.XmlNameValuePair{
			{
				Name:  "dfs.datanode.registered.http.port",
				Value: "${env.HTTP_PORT}",
			},
		}
	}
}

func CreateTlsSecretPvc(secretClass string, jksPassword string) corev1.Volume {
	return corev1.Volume{
		Name: TlsVolumeName,
		VolumeSource: corev1.VolumeSource{
			Ephemeral: &corev1.EphemeralVolumeSource{
				VolumeClaimTemplate: &corev1.PersistentVolumeClaimTemplate{
					ObjectMeta: metav1.ObjectMeta{
						Annotations: map[string]string{
							constants.AnnotationSecretsClass:          secretClass,
							constants.AnnotationSecretsScope:          "pod,node",
							constants.AnnotationSecretsFormat:         "tls-p12",
							constants.AnnotationSecretsPKCS12Password: jksPassword,
						},
					},
					Spec: corev1.PersistentVolumeClaimSpec{
						AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
						StorageClassName: func() *string {
							cs := "secrets.zncdata.dev"
							return &cs
						}(),
						VolumeMode: func() *corev1.PersistentVolumeMode { v := corev1.PersistentVolumeFilesystem; return &v }(),
						Resources: corev1.VolumeResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceStorage: resource.MustParse("1Gi"),
							},
						},
					},
				},
			},
		},
	}
}
