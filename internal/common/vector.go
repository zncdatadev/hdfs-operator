package common

import (
	"context"
	"emperror.dev/errors"
	"fmt"
	"github.com/zncdatadev/hdfs-operator/api/v1alpha1"
	"github.com/zncdatadev/operator-go/pkg/builder"
	appsv1 "k8s.io/api/apps/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var vectorLogger = ctrl.Log.WithName("vector")

const ContainerVector ContainerComponent = "vector"

func IsVectorEnable(roleLoggingConfig interface{}) (bool, error) {
	if roleLoggingConfig == nil {
		return false, fmt.Errorf("role logging config is nil")
	}

	switch t := roleLoggingConfig.(type) {
	case *v1alpha1.NameNodeContainerLoggingSpec:
		return t.EnableVectorAgent, nil
	case *v1alpha1.DataNodeContainerLoggingSpec:
		return t.EnableVectorAgent, nil
	case *v1alpha1.JournalNodeContainerLoggingSpec:
		return t.EnableVectorAgent, nil
	default:
		return false, fmt.Errorf("unknown role logging type %T to check vector", t)
	}
}

type VectorConfigParams struct {
	Client        client.Client
	ClusterConfig *v1alpha1.ClusterConfigSpec
	Namespace     string
	InstanceName  string
	Role          string
	GroupName     string
}

func generateVectorYAML(ctx context.Context, params VectorConfigParams) (string, error) {
	aggregatorConfigMapName := params.ClusterConfig.VectorAggregatorConfigMapName
	if aggregatorConfigMapName == "" {
		return "", errors.New("vectorAggregatorConfigMapName is not set")
	}
	return builder.MakeVectorYaml(ctx, params.Client, params.Namespace, params.InstanceName, params.Role,
		params.GroupName, aggregatorConfigMapName)
}

func ExtendConfigMapByVector(ctx context.Context, params VectorConfigParams, data map[string]string) {
	vectorYaml, err := generateVectorYAML(ctx, params)
	if err != nil {
		vectorLogger.Error(errors.Wrap(err, "error creating vector YAML"), "failed to create vector YAML")
	} else {
		data[builder.VectorConfigFile] = vectorYaml
	}
}

func ExtendStatefulSetByVector(
	logProvider []string,
	dep *appsv1.StatefulSet,
	vectorConfigMapName string) {
	decorator := builder.VectorDecorator{
		WorkloadObject:           dep,
		LogVolumeName:            builder.VectorLogVolumeName,
		VectorConfigVolumeName:   builder.VectorConfigVolumeName,
		VectorConfigMapName:      vectorConfigMapName,
		LogProviderContainerName: logProvider,
	}
	err := decorator.Decorate()
	if err != nil {
		return
	}
}
