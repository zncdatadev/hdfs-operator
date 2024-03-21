package common

import (
	stackv1alpha1 "github.com/zncdata-labs/alluxio-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const DefaultLog4jProperties = `
log4j.rootLogger=INFO, ${alluxio.logger.type}, ${alluxio.remote.logger.type}

log4j.category.alluxio.logserver=INFO, ${alluxio.logserver.logger.type}
log4j.additivity.alluxio.logserver=false

log4j.logger.AUDIT_LOG=INFO, ${alluxio.master.audit.logger.type}
log4j.logger.JOB_MASTER_AUDIT_LOG=INFO, ${alluxio.job.master.audit.logger.type}
log4j.logger.PROXY_AUDIT_LOG=INFO, ${alluxio.proxy.audit.logger.type}
log4j.additivity.AUDIT_LOG=false
log4j.additivity.JOB_MASTER_AUDIT_LOG=false
log4j.additivity.PROXY_AUDIT_LOG=false

# Configures an appender whose name is "" (empty string) to be NullAppender.
# By default, if a Java class does not specify a particular appender, log4j will
# use "" as the appender name, then it will use Null appender.
log4j.appender.=org.apache.log4j.varia.NullAppender

log4j.appender.Console=org.apache.log4j.ConsoleAppender
log4j.appender.Console.Target=System.out
log4j.appender.Console.layout=org.apache.log4j.PatternLayout
log4j.appender.Console.layout.ConversionPattern=%d{ISO8601} %-5p [%t](%F:%L) - %m%n

# The ParquetWriter logs for every row group which is not noisy for large row group size,
# but very noisy for small row group size.
log4j.logger.org.apache.parquet.hadoop.InternalParquetRecordWriter=WARN
log4j.logger.org.apache.parquet.hadoop.InternalParquetRecordReader=WARN

# Appender for Job Master
log4j.appender.JOB_MASTER_LOGGER=org.apache.log4j.RollingFileAppender
log4j.appender.JOB_MASTER_LOGGER.File=${alluxio.logs.dir}/job_master.log
log4j.appender.JOB_MASTER_LOGGER.MaxFileSize=10MB
log4j.appender.JOB_MASTER_LOGGER.MaxBackupIndex=100
log4j.appender.JOB_MASTER_LOGGER.layout=org.apache.log4j.PatternLayout
log4j.appender.JOB_MASTER_LOGGER.layout.ConversionPattern=%d{ISO8601} %-5p [%t](%F:%L) - %m%n

# Appender for Job Workers
log4j.appender.JOB_WORKER_LOGGER=org.apache.log4j.RollingFileAppender
log4j.appender.JOB_WORKER_LOGGER.File=${alluxio.logs.dir}/job_worker.log
log4j.appender.JOB_WORKER_LOGGER.MaxFileSize=10MB
log4j.appender.JOB_WORKER_LOGGER.MaxBackupIndex=100
log4j.appender.JOB_WORKER_LOGGER.layout=org.apache.log4j.PatternLayout
log4j.appender.JOB_WORKER_LOGGER.layout.ConversionPattern=%d{ISO8601} %-5p [%t](%F:%L) - %m%n

# Appender for Master
log4j.appender.MASTER_LOGGER=org.apache.log4j.RollingFileAppender
log4j.appender.MASTER_LOGGER.File=${alluxio.logs.dir}/master.log
log4j.appender.MASTER_LOGGER.MaxFileSize=10MB
log4j.appender.MASTER_LOGGER.MaxBackupIndex=100
log4j.appender.MASTER_LOGGER.layout=org.apache.log4j.PatternLayout
log4j.appender.MASTER_LOGGER.layout.ConversionPattern=%d{ISO8601} %-5p [%t](%F:%L) - %m%n

# Appender for Master
log4j.appender.SECONDARY_MASTER_LOGGER=org.apache.log4j.RollingFileAppender
log4j.appender.SECONDARY_MASTER_LOGGER.File=${alluxio.logs.dir}/secondary_master.log
log4j.appender.SECONDARY_MASTER_LOGGER.MaxFileSize=10MB
log4j.appender.SECONDARY_MASTER_LOGGER.MaxBackupIndex=100
log4j.appender.SECONDARY_MASTER_LOGGER.layout=org.apache.log4j.PatternLayout
log4j.appender.SECONDARY_MASTER_LOGGER.layout.ConversionPattern=%d{ISO8601} %-5p [%t](%F:%L) - %m%n

# Appender for Master audit
log4j.appender.MASTER_AUDIT_LOGGER=org.apache.log4j.RollingFileAppender
log4j.appender.MASTER_AUDIT_LOGGER.File=${alluxio.logs.dir}/master_audit.log
log4j.appender.MASTER_AUDIT_LOGGER.MaxFileSize=10MB
log4j.appender.MASTER_AUDIT_LOGGER.MaxBackupIndex=100
log4j.appender.MASTER_AUDIT_LOGGER.layout=org.apache.log4j.PatternLayout
log4j.appender.MASTER_AUDIT_LOGGER.layout.ConversionPattern=%d{ISO8601} %-5p [%t](%F:%L) - %m%n

# Appender for Job Master audit
log4j.appender.JOB_MASTER_AUDIT_LOGGER=org.apache.log4j.RollingFileAppender
log4j.appender.JOB_MASTER_AUDIT_LOGGER.File=${alluxio.logs.dir}/job_master_audit.log
log4j.appender.JOB_MASTER_AUDIT_LOGGER.MaxFileSize=10MB
log4j.appender.JOB_MASTER_AUDIT_LOGGER.MaxBackupIndex=100
log4j.appender.JOB_MASTER_AUDIT_LOGGER.layout=org.apache.log4j.PatternLayout
log4j.appender.JOB_MASTER_AUDIT_LOGGER.layout.ConversionPattern=%d{ISO8601} %-5p [%t](%F:%L) - %m%n

# Appender for Proxy
log4j.appender.PROXY_LOGGER=org.apache.log4j.RollingFileAppender
log4j.appender.PROXY_LOGGER.File=${alluxio.logs.dir}/proxy.log
log4j.appender.PROXY_LOGGER.MaxFileSize=10MB
log4j.appender.PROXY_LOGGER.MaxBackupIndex=100
log4j.appender.PROXY_LOGGER.layout=org.apache.log4j.PatternLayout
log4j.appender.PROXY_LOGGER.layout.ConversionPattern=%d{ISO8601} %-5p [%t](%F:%L) - %m%n

# Appender for Proxy audit
log4j.appender.PROXY_AUDIT_LOGGER=org.apache.log4j.RollingFileAppender
log4j.appender.PROXY_AUDIT_LOGGER.File=${alluxio.logs.dir}/proxy_audit.log
log4j.appender.PROXY_AUDIT_LOGGER.MaxFileSize=10MB
log4j.appender.PROXY_AUDIT_LOGGER.MaxBackupIndex=100
log4j.appender.PROXY_AUDIT_LOGGER.layout=org.apache.log4j.PatternLayout
log4j.appender.PROXY_AUDIT_LOGGER.layout.ConversionPattern=%d{ISO8601} %-5p %c{2}[%t](%F:%M:%L) - %m%n

# Appender for Workers
log4j.appender.WORKER_LOGGER=org.apache.log4j.RollingFileAppender
log4j.appender.WORKER_LOGGER.File=${alluxio.logs.dir}/worker.log
log4j.appender.WORKER_LOGGER.MaxFileSize=10MB
log4j.appender.WORKER_LOGGER.MaxBackupIndex=100
log4j.appender.WORKER_LOGGER.layout=org.apache.log4j.PatternLayout
log4j.appender.WORKER_LOGGER.layout.ConversionPattern=%d{ISO8601} %-5p [%t](%F:%L) - %m%n

# Remote appender for Job Master
log4j.appender.REMOTE_JOB_MASTER_LOGGER=org.apache.log4j.net.SocketAppender
log4j.appender.REMOTE_JOB_MASTER_LOGGER.Port=${alluxio.logserver.port}
log4j.appender.REMOTE_JOB_MASTER_LOGGER.RemoteHost=${alluxio.logserver.hostname}
log4j.appender.REMOTE_JOB_MASTER_LOGGER.ReconnectionDelay=10000
log4j.appender.REMOTE_JOB_MASTER_LOGGER.filter.ID=alluxio.AlluxioRemoteLogFilter
log4j.appender.REMOTE_JOB_MASTER_LOGGER.filter.ID.ProcessType=JOB_MASTER
log4j.appender.REMOTE_JOB_MASTER_LOGGER.Threshold=WARN

# Remote appender for Job Workers
log4j.appender.REMOTE_JOB_WORKER_LOGGER=org.apache.log4j.net.SocketAppender
log4j.appender.REMOTE_JOB_WORKER_LOGGER.Port=${alluxio.logserver.port}
log4j.appender.REMOTE_JOB_WORKER_LOGGER.RemoteHost=${alluxio.logserver.hostname}
log4j.appender.REMOTE_JOB_WORKER_LOGGER.ReconnectionDelay=10000
log4j.appender.REMOTE_JOB_WORKER_LOGGER.filter.ID=alluxio.AlluxioRemoteLogFilter
log4j.appender.REMOTE_JOB_WORKER_LOGGER.filter.ID.ProcessType=JOB_WORKER
log4j.appender.REMOTE_JOB_WORKER_LOGGER.Threshold=WARN

# Remote appender for Master
log4j.appender.REMOTE_MASTER_LOGGER=org.apache.log4j.net.SocketAppender
log4j.appender.REMOTE_MASTER_LOGGER.Port=${alluxio.logserver.port}
log4j.appender.REMOTE_MASTER_LOGGER.RemoteHost=${alluxio.logserver.hostname}
log4j.appender.REMOTE_MASTER_LOGGER.ReconnectionDelay=10000
log4j.appender.REMOTE_MASTER_LOGGER.filter.ID=alluxio.AlluxioRemoteLogFilter
log4j.appender.REMOTE_MASTER_LOGGER.filter.ID.ProcessType=MASTER
log4j.appender.REMOTE_MASTER_LOGGER.Threshold=WARN

# Remote appender for Secondary Master
log4j.appender.REMOTE_SECONDARY_MASTER_LOGGER=org.apache.log4j.net.SocketAppender
log4j.appender.REMOTE_SECONDARY_MASTER_LOGGER.Port=${alluxio.logserver.port}
log4j.appender.REMOTE_SECONDARY_MASTER_LOGGER.RemoteHost=${alluxio.logserver.hostname}
log4j.appender.REMOTE_SECONDARY_MASTER_LOGGER.ReconnectionDelay=10000
log4j.appender.REMOTE_SECONDARY_MASTER_LOGGER.filter.ID=alluxio.AlluxioRemoteLogFilter
log4j.appender.REMOTE_SECONDARY_MASTER_LOGGER.filter.ID.ProcessType=SECONDARY_MASTER
log4j.appender.REMOTE_SECONDARY_MASTER_LOGGER.Threshold=WARN

# Remote appender for Proxy
log4j.appender.REMOTE_PROXY_LOGGER=org.apache.log4j.net.SocketAppender
log4j.appender.REMOTE_PROXY_LOGGER.Port=${alluxio.logserver.port}
log4j.appender.REMOTE_PROXY_LOGGER.RemoteHost=${alluxio.logserver.hostname}
log4j.appender.REMOTE_PROXY_LOGGER.ReconnectionDelay=10000
log4j.appender.REMOTE_PROXY_LOGGER.filter.ID=alluxio.AlluxioRemoteLogFilter
log4j.appender.REMOTE_PROXY_LOGGER.filter.ID.ProcessType=PROXY
log4j.appender.REMOTE_PROXY_LOGGER.Threshold=WARN

# Remote appender for Workers
log4j.appender.REMOTE_WORKER_LOGGER=org.apache.log4j.net.SocketAppender
log4j.appender.REMOTE_WORKER_LOGGER.Port=${alluxio.logserver.port}
log4j.appender.REMOTE_WORKER_LOGGER.RemoteHost=${alluxio.logserver.hostname}
log4j.appender.REMOTE_WORKER_LOGGER.ReconnectionDelay=10000
log4j.appender.REMOTE_WORKER_LOGGER.filter.ID=alluxio.AlluxioRemoteLogFilter
log4j.appender.REMOTE_WORKER_LOGGER.filter.ID.ProcessType=WORKER
log4j.appender.REMOTE_WORKER_LOGGER.Threshold=WARN

# (Local) appender for log server itself
log4j.appender.LOGSERVER_LOGGER=org.apache.log4j.RollingFileAppender
log4j.appender.LOGSERVER_LOGGER.File=${alluxio.logs.dir}/logserver.log
log4j.appender.LOGSERVER_LOGGER.MaxFileSize=10MB
log4j.appender.LOGSERVER_LOGGER.MaxBackupIndex=100
log4j.appender.LOGSERVER_LOGGER.layout=org.apache.log4j.PatternLayout
log4j.appender.LOGSERVER_LOGGER.layout.ConversionPattern=%d{ISO8601} %-5p [%t](%F:%L) - %m%n

# (Local) appender for log server to log on behalf of log clients
# No need to configure file path because log server will dynamically
# figure out for each appender.
log4j.appender.LOGSERVER_CLIENT_LOGGER=org.apache.log4j.RollingFileAppender
log4j.appender.LOGSERVER_CLIENT_LOGGER.MaxFileSize=10MB
log4j.appender.LOGSERVER_CLIENT_LOGGER.MaxBackupIndex=100
log4j.appender.LOGSERVER_CLIENT_LOGGER.layout=org.apache.log4j.PatternLayout
log4j.appender.LOGSERVER_CLIENT_LOGGER.layout.ConversionPattern=%d{ISO8601} %-5p [%t](%F:%L) - %m%n

# Appender for User
log4j.appender.USER_LOGGER=org.apache.log4j.RollingFileAppender
log4j.appender.USER_LOGGER.File=${alluxio.user.logs.dir}/user_${user.name}.log
log4j.appender.USER_LOGGER.MaxFileSize=10MB
log4j.appender.USER_LOGGER.MaxBackupIndex=10
log4j.appender.USER_LOGGER.layout=org.apache.log4j.PatternLayout
log4j.appender.USER_LOGGER.layout.ConversionPattern=%d{ISO8601} %-5p [%t](%F:%L) - %m%n

# Appender for Fuse
log4j.appender.FUSE_LOGGER=org.apache.log4j.RollingFileAppender
log4j.appender.FUSE_LOGGER.File=${alluxio.logs.dir}/fuse.log
log4j.appender.FUSE_LOGGER.MaxFileSize=100MB
log4j.appender.FUSE_LOGGER.MaxBackupIndex=10
log4j.appender.FUSE_LOGGER.layout=org.apache.log4j.PatternLayout
log4j.appender.FUSE_LOGGER.layout.ConversionPattern=%d{ISO8601} %-5p [%t](%F:%L) - %m%n

# Disable noisy DEBUG logs
log4j.logger.com.amazonaws.util.EC2MetadataUtils=OFF
log4j.logger.io.grpc.netty.NettyServerHandler=OFF

# Disable noisy INFO logs from ratis
log4j.logger.org.apache.ratis.grpc.server.GrpcLogAppender=ERROR
log4j.logger.org.apache.ratis.grpc.server.GrpcServerProtocolService=WARN
log4j.logger.org.apache.ratis.server.impl.FollowerInfo=WARN
log4j.logger.org.apache.ratis.server.leader.FollowerInfo=WARN
log4j.logger.org.apache.ratis.server.impl.RaftServerImpl=WARN
log4j.logger.org.apache.ratis.server.RaftServer$Division=WARN
`
const Log4jCfgName = "log4j.properties"

type Logger string

const (
	JobMasterLogger Logger = "JOB_MASTER_LOGGER"
	JobWorkerLogger Logger = "JOB_WORKER_LOGGER"
	MasterLogger    Logger = "MASTER_LOGGER"
	WorkerLogger    Logger = "WORKER_LOGGER"
)

//var RoleLoggerMap = map[Role][]Logger{
//	Master: {MasterLogger, JobMasterLogger},
//	Worker: {WorkerLogger, JobWorkerLogger},
//}

type RoleLoggingDataBuilder interface {
	MakeContainerLog4jData() map[string]string
}

type LoggingRecociler struct {
	GeneralResourceStyleReconciler[*stackv1alpha1.Alluxio, any]
	RoleLoggingDataBuilder RoleLoggingDataBuilder
	role                   Role
}

// NewLoggingReconciler new logging reconcile
func NewLoggingReconciler(
	scheme *runtime.Scheme,
	instance *stackv1alpha1.Alluxio,
	client client.Client,
	groupName string,
	mergedLabels map[string]string,
	mergedCfg any,
	logDataBuilder RoleLoggingDataBuilder,
	role Role,
) *LoggingRecociler {
	return &LoggingRecociler{
		GeneralResourceStyleReconciler: *NewGeneraResourceStyleReconciler[*stackv1alpha1.Alluxio, any](
			scheme,
			instance,
			client,
			groupName,
			mergedLabels,
			mergedCfg,
		),
		RoleLoggingDataBuilder: logDataBuilder,
		role:                   role,
	}
}

// Build log4j config map
func (l *LoggingRecociler) Build() (client.Object, error) {
	cmData := l.RoleLoggingDataBuilder.MakeContainerLog4jData()
	if len(cmData) == 0 {
		return nil, nil
	}
	obj := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      CreateRoleGroupLoggingConfigMapName(l.Instance.Name, string(l.role), l.GroupName),
			Namespace: l.Instance.Namespace,
			Labels:    l.MergedLabels,
		},
		Data: cmData,
	}
	return obj, nil
}

func PropertiesValue(logger Logger, loggingConfig *stackv1alpha1.LoggingConfigSpec) string {
	properties := make(map[string]string)
	if loggingConfig.Loggers != nil {
		for k, level := range loggingConfig.Loggers {
			if level != nil {
				v := *level
				properties["log4j.logger."+k] = v.Level
			}
		}
	}
	if loggingConfig.Console != nil {
		properties["log4j.logger."+string(logger)] = loggingConfig.Console.Level + ", Console"
	}

	props := log4jProperties(properties)

	return DefaultLog4jProperties +
		"\n\n" +
		"# alluxio-operator modify logging\n" +
		props
}

func log4jProperties(properties map[string]string) string {
	data := ""
	for k, v := range properties {
		data += k + "=" + v + "\n"
	}
	return data
}

func CreateLoggerConfigMapKey(logger Logger) string {
	return string(logger) + ".log4j"
}
func Log4jVolumeName() string {
	return "log4j"
}
