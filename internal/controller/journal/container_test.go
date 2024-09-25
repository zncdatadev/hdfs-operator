package journal_test

// Generates correct command arguments for journalnode setup
import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	hdfsv1alpha1 "github.com/zncdatadev/hdfs-operator/api/v1alpha1"
	"github.com/zncdatadev/hdfs-operator/internal/controller/journal"
	corev1 "k8s.io/api/core/v1"
)

var _ = Describe("CommandArgs", func() {
	Context("when generating command args", func() {
		It("should correctly generate command args for journalnode setup", func() {
			// given
			clusterConfig := &hdfsv1alpha1.ClusterConfigSpec{
				Authentication: &hdfsv1alpha1.AuthenticationSpec{
					Kerberos: &hdfsv1alpha1.KerberosSpec{
						SecretClass: "krb-hdfs",
					},
				},
			}
			builder := journal.NewJournalNodeContainerBuilder(&hdfsv1alpha1.HdfsCluster{
				Spec: hdfsv1alpha1.HdfsClusterSpec{
					ClusterConfigSpec: clusterConfig,
				},
			}, corev1.ResourceRequirements{})

			// when
			args := builder.CommandArgs()

			// then
			Expect(args[0]).NotTo(BeNil())
			Expect(args[0]).To(ContainSubstring("mkdir -p /kubedoop/config/journalnode"))
			Expect(args[0]).To(ContainSubstring("cp /kubedoop/mount/config/journalnode/*.xml /kubedoop/config/journalnode"))
			Expect(args[0]).To(ContainSubstring("/kubedoop/hadoop/bin/hdfs journalnode &"))
			Expect(args[0]).To(ContainSubstring("export KERBEROS_REALM=$(grep -oP 'default_realm = \\K.*' /kubedoop/kerberos/krb5.conf)"))
			Expect(args[0]).To(ContainSubstring("/kubedoop/hadoop/bin/hdfs journalnode &"))
			Expect(args[0]).To(ContainSubstring("export POD_ADDRESS=$(cat /kubedoop/listener/default-address/address)"))
			Expect(args[0]).To(ContainSubstring("mkdir -p /kubedoop/log/_vector/ && touch /kubedoop/log/_vector/shutdown"))
		})

		It("should correctly generate command args for journalnode setup with kerberos enabled", func() {
			// given
			clusterConfig := &hdfsv1alpha1.ClusterConfigSpec{
				Authentication: &hdfsv1alpha1.AuthenticationSpec{
					Kerberos: nil,
				},
			}
			builder := journal.NewJournalNodeContainerBuilder(&hdfsv1alpha1.HdfsCluster{
				Spec: hdfsv1alpha1.HdfsClusterSpec{
					ClusterConfigSpec: clusterConfig,
				},
			}, corev1.ResourceRequirements{})

			// when
			args := builder.CommandArgs()

			// then
			Expect(args[0]).NotTo(BeNil())
			// not containing krb script
			Expect(args[0]).NotTo(ContainSubstring("export KERBEROS_REALM=$(grep -oP 'default_realm = \\K.*' /kubedoop/kerberos/krb5.conf)"))
			Expect(args[0]).To(ContainSubstring("mkdir -p /kubedoop/config/journalnode"))
			Expect(args[0]).To(ContainSubstring("cp /kubedoop/mount/config/journalnode/*.xml /kubedoop/config/journalnode"))
			Expect(args[0]).To(ContainSubstring("/kubedoop/hadoop/bin/hdfs journalnode &"))
			Expect(args[0]).To(ContainSubstring("export POD_ADDRESS=$(cat /kubedoop/listener/default-address/address)"))
			Expect(args[0]).To(ContainSubstring("mkdir -p /kubedoop/log/_vector/ && touch /kubedoop/log/_vector/shutdown"))
		})

	})
})
