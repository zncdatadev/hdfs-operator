package common

import (
	"fmt"
	hdfsv1alpha1 "github.com/zncdatadev/hdfs-operator/api/v1alpha1"
	"github.com/zncdatadev/hdfs-operator/internal/util"
	"github.com/zncdatadev/operator-go/pkg/config"
	"github.com/zncdatadev/secret-operator/pkg/volume"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const KrbVolumeName = "kerberos"

func IsKerberosEnabled(clusterSpec *hdfsv1alpha1.ClusterConfigSpec) bool {
	return clusterSpec.Authentication != nil && clusterSpec.Authentication.Kerberos != nil
}

func ExportKrbRealmFromConfig(krb5ConfPath string) string {
	return "export KERBEROS_REALM=$(grep -oP 'default_realm = \\K.*' " + krb5ConfPath + ")\n"
}

// SecurityHdfsSiteXml make kerberos config for hdfs-site.xml
func SecurityHdfsSiteXml() []util.XmlNameValuePair {
	return []util.XmlNameValuePair{
		{
			Name:  "dfs.block.access.token.enable",
			Value: "true",
		},
		{
			Name:  "dfs.http.policy",
			Value: "HTTPS_ONLY",
		},
		{
			Name:  "hadoop.kerberos.keytab.login.autorenewal.enabled",
			Value: "true",
		},
		{
			Name:  "dfs.https.server.keystore.resource",
			Value: "ssl-server.xml",
		},
		{
			Name:  "dfs.https.client.keystore.resource",
			Value: "ssl-client.xml",
		},
		{
			Name:  "dfs.encrypt.data.transfer",
			Value: "true",
		},
		{
			Name:  "dfs.data.transfer.protection",
			Value: "privacy",
		},
	}
}

func SecurityDiscoveryHdfsSiteXml() []util.XmlNameValuePair {
	return []util.XmlNameValuePair{
		{
			Name:  "hadoop.kerberos.keytab.login.autorenewal.enabled",
			Value: "true",
		},
		{
			Name:  "dfs.data.transfer.protection",
			Value: "privacy",
		},
		{
			Name:  "dfs.encrypt.data.transfer",
			Value: "true",
		},
	}
}

func SecurityDiscoveryCoreSiteXml(instanceName string, ns string) []util.XmlNameValuePair {
	principalHostPart := PrincipalHostPart(instanceName, ns)
	return []util.XmlNameValuePair{
		{
			Name:  "hadoop.security.authentication",
			Value: "kerberos",
		},
		{
			Name:  "dfs.journalnode.kerberos.principal",
			Value: fmt.Sprintf("jn/%s", principalHostPart),
		},
		{
			Name:  "dfs.namenode.kerberos.principal",
			Value: fmt.Sprintf("nn/%s", principalHostPart),
		},
		{
			Name:  "dfs.datanode.kerberos.principal",
			Value: fmt.Sprintf("dn/%s", principalHostPart),
		},
		{
			Name:  "hadoop.rpc.protection",
			Value: "privacy",
		},
	}
}

// SecurityCoreSiteXml make kerberos config for core-site.xml
func SecurityCoreSiteXml(instanceName string, ns string) []util.XmlNameValuePair {
	principalHostPart := PrincipalHostPart(instanceName, ns)
	return []util.XmlNameValuePair{
		{
			Name:  "hadoop.security.authentication",
			Value: "kerberos",
		},
		{
			Name:  "dfs.journalnode.kerberos.principal",
			Value: fmt.Sprintf("jn/%s", principalHostPart),
		},
		{
			Name:  "dfs.journalnode.kerberos.internal.spnego.principal",
			Value: fmt.Sprintf("jn/%s", principalHostPart),
		},
		{
			Name:  "dfs.namenode.kerberos.principal",
			Value: fmt.Sprintf("nn/%s", principalHostPart),
		},
		{
			Name:  "dfs.datanode.kerberos.principal",
			Value: fmt.Sprintf("dn/%s", principalHostPart),
		},
		{
			Name:  "dfs.web.authentication.kerberos.principal",
			Value: fmt.Sprintf("HTTP/%s", principalHostPart),
		},
		{
			Name:  "dfs.journalnode.keytab.file",
			Value: fmt.Sprintf("%s/keytab", hdfsv1alpha1.KerberosMountPath),
		},
		{
			Name:  "dfs.namenode.keytab.file",
			Value: fmt.Sprintf("%s/keytab", hdfsv1alpha1.KerberosMountPath),
		},
		{
			Name:  "dfs.datanode.keytab.file",
			Value: fmt.Sprintf("%s/keytab", hdfsv1alpha1.KerberosMountPath),
		},
		{
			Name:  "dfs.journalnode.kerberos.principal.pattern",
			Value: fmt.Sprintf("jn/%s", principalHostPart),
		},
		{
			Name:  "dfs.namenode.kerberos.principal.pattern",
			Value: fmt.Sprintf("nn/%s", principalHostPart),
		},
		{
			Name:  "hadoop.rpc.protection",
			Value: "privacy",
		},
	}
}

func SecurityEnvs(container ContainerComponent, jvmArgs *[]string) []corev1.EnvVar {
	envs := []corev1.EnvVar{
		{
			Name:  "HADOOP_OPTS",
			Value: fmt.Sprintf("-Djava.security.krb5.conf=%s/krb5.conf", hdfsv1alpha1.KerberosMountPath),
		},
		{
			Name:  "KRB5_CONFIG",
			Value: fmt.Sprintf("%s/krb5.conf", hdfsv1alpha1.KerberosMountPath),
		},
		{
			Name:  "KRB5_CLIENT_KTNAME",
			Value: fmt.Sprintf("%s/keytab", hdfsv1alpha1.KerberosMountPath),
		},
	}
	*jvmArgs = append(*jvmArgs, fmt.Sprintf("-Djava.security.krb5.conf=%s/krb5.conf", hdfsv1alpha1.KerberosMountPath))
	return envs
}

func SecurityVolumeMounts() []corev1.VolumeMount {
	return []corev1.VolumeMount{
		{
			Name:      KrbVolumeName,
			MountPath: hdfsv1alpha1.KerberosMountPath,
		},
	}
}

func CreateKerberosSecretPvc(secretClass string, instanceName string, role Role) corev1.Volume {
	kerberosServiceName := GetKerberosServiceName(role)

	return corev1.Volume{
		Name: KrbVolumeName,
		VolumeSource: corev1.VolumeSource{
			Ephemeral: &corev1.EphemeralVolumeSource{
				VolumeClaimTemplate: &corev1.PersistentVolumeClaimTemplate{
					ObjectMeta: metav1.ObjectMeta{
						Annotations: map[string]string{
							volume.SecretsZncdataClass:                secretClass,
							volume.SecretsZncdataScope:                fmt.Sprintf("service=%s", instanceName),
							volume.SecretsZncdataKerberosServiceNames: kerberosServiceName + ",HTTP",
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

func CreateExportKrbRealmEnvData(clusterConfig *hdfsv1alpha1.ClusterConfigSpec) map[string]interface{} {
	return map[string]interface{}{
		"kerberosEnabled": IsKerberosEnabled(clusterConfig),
		"kerberosEnv":     ExportKrbRealmFromConfig(hdfsv1alpha1.KerberosMountPath + "/krb5.conf"),
	}
}

func CreateGetKerberosTicketData(principal string) map[string]interface{} {
	return map[string]interface{}{
		"kinitScript": GetKerberosTicket(principal),
	}
}

// ParseKerberosScript Parse script for kerberos
func ParseKerberosScript(tmpl string, data map[string]interface{}) []string {
	parser := config.TemplateParser{
		Value:    data,
		Template: tmpl,
	}
	if content, err := parser.Parse(); err != nil {
		panic(err)
	} else {
		return []string{content}
	}
}

func PrincipalHostPart(instanceName string, ns string) string {
	return fmt.Sprintf("%s.%s.svc.cluster.local@${env.KERBEROS_REALM}", instanceName, ns)
}

func CreateKerberosPrincipal(instanceName string, ns string, role Role) string {
	host := fmt.Sprintf("%s.%s.svc.cluster.local@${KERBEROS_REALM}", instanceName, ns)
	return fmt.Sprintf("%s/%s", GetKerberosServiceName(role), host)
}

func GetKerberosServiceName(role Role) string {
	switch role {
	case NameNode:
		return "nn"
	case DataNode:
		return "dn"
	case JournalNode:
		return "jn"
	default:
		panic(fmt.Sprintf("unsupported role for kerberos: %s", role))
	}
}

func GetKerberosTicket(principal string) string {
	return fmt.Sprintf(`
echo "Getting ticket for %s" from /stackable/kerberos/keytab
kinit "%s" -kt %s
`, principal, principal, hdfsv1alpha1.KerberosMountPath+"/keytab")
}
