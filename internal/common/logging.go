package common

import (
	"context"
	"fmt"
	"github.com/zncdata-labs/hdfs-operator/internal/util"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type RoleLoggingDataBuilder interface {
	MakeContainerLogData() map[string]string
}

type BaseRoleLoggingDataBuilder struct {
	Role Role
}

func (b *BaseRoleLoggingDataBuilder) MakeContainerLogData() map[string]string {
	// todo: make data for multi container, and support multi log framework by using LoggingPluggingDataBuilder interface, such as log4j, logback, etc
	return nil
}

type LoggingRecociler[T client.Object, G any] struct {
	GeneralResourceStyleReconciler[T, G]
	RoleLoggingDataBuilder  RoleLoggingDataBuilder
	role                    Role
	InstanceGetter          InstanceAttributes
	ConfigmapName           string
	CurrentLoggingConfigmap *corev1.ConfigMap
}

// NewLoggingReconciler new logging reconcile
func NewLoggingReconciler[T client.Object](
	scheme *runtime.Scheme,
	instance T,
	client client.Client,
	groupName string,
	mergedLabels map[string]string,
	mergedCfg any,
	logDataBuilder RoleLoggingDataBuilder,
	role Role,
	configmapName string,
	currentConfigMap *corev1.ConfigMap,
) *LoggingRecociler[T, any] {
	return &LoggingRecociler[T, any]{
		GeneralResourceStyleReconciler: *NewGeneraResourceStyleReconciler(
			scheme,
			instance,
			client,
			groupName,
			mergedLabels,
			mergedCfg,
		),
		RoleLoggingDataBuilder:  logDataBuilder,
		role:                    role,
		ConfigmapName:           configmapName,
		CurrentLoggingConfigmap: currentConfigMap,
	}
}

// Build log4j config map
func (l *LoggingRecociler[T, G]) Build(_ context.Context) (client.Object, error) {
	cmData := l.RoleLoggingDataBuilder.MakeContainerLogData()
	if len(cmData) == 0 {
		return nil, nil
	}
	if l.CurrentLoggingConfigmap != nil {
		l.CurrentLoggingConfigmap.Data = cmData
		return l.CurrentLoggingConfigmap, nil
	}
	obj := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      l.ConfigmapName,
			Namespace: l.Instance.GetNamespace(),
			Labels:    l.MergedLabels,
		},
		Data: cmData,
	}
	return obj, nil
}

type LoggingPluggingDataBuilder interface {
	MakeContainerLogProperties(origin string) string
}

type LogBuilderLoggers struct {
	logger string
	level  string
}

type LogBuilderAppender struct {
	appenderName string
	level        string
}

type Log4jLoggingDataBuilder struct {
	Loggers []LogBuilderLoggers
	Console *LogBuilderAppender
	File    *LogBuilderAppender
}

// MakeContainerLogProperties make log4j properties
func (l *Log4jLoggingDataBuilder) MakeContainerLogProperties(origin string) string {
	content, err := util.OverridePropertiesFileContent(origin, l.MakeOverrideLoggerProperties())
	if err != nil {
		return origin
	}
	return content
}

// MakeOverrideLoggerProperties get override logger properties
// do works below:
// 1. make custom loggers properties
// 2. make console logger properties
// 3. make file appender logger properties
// 4. merge all the properties
func (l *Log4jLoggingDataBuilder) MakeOverrideLoggerProperties() map[string]string {
	loggers := l.makeCustomLoggersProperties()
	console := l.makeConsoleLoggerProperties()
	file := l.makeFileLoggerProperties()
	properties := make(map[string]string)
	for k, v := range loggers {
		properties[k] = v
	}
	for k, v := range console {
		properties[k] = v
	}
	for k, v := range file {
		properties[k] = v
	}
	return properties
}

func (l *Log4jLoggingDataBuilder) makeCustomLoggersProperties() map[string]string {
	if l.Loggers == nil {
		return nil
	}
	properties := make(map[string]string)
	for _, logger := range l.Loggers {
		properties["log4j.logger."+logger.logger] = logger.level
	}
	return properties
}

// make console logger properties
// change console appender logger level:  "log4j.appender.CONSOLE.Threshold=INFO"
func (l *Log4jLoggingDataBuilder) makeConsoleLoggerProperties() map[string]string {
	if l.Console == nil {
		return nil
	}
	properties := make(map[string]string)
	key := fmt.Sprintf("log4j.appender.%s.Threshold", l.Console.appenderName)
	properties[key] = l.Console.level
	return properties
}

// make file appender logger properties
// change file appender logger level: "log4j.appender.FILE.Threshold=INFO"
func (l *Log4jLoggingDataBuilder) makeFileLoggerProperties() map[string]string {
	if l.File == nil {
		return nil
	}
	properties := make(map[string]string)
	key := fmt.Sprintf("log4j.appender.%s.Threshold", l.File.appenderName)
	properties[key] = l.File.level
	return properties
}
