package common

import (
	"context"
	"fmt"

	"emperror.dev/errors"
	"github.com/zncdatadev/hdfs-operator/api/v1alpha1"
	hdfsv1alpha1 "github.com/zncdatadev/hdfs-operator/api/v1alpha1"
	"github.com/zncdatadev/operator-go/pkg/builder"
	"github.com/zncdatadev/operator-go/pkg/productlogging"
	"github.com/zncdatadev/operator-go/pkg/util"

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
	return productlogging.MakeVectorYaml(ctx, params.Client, params.Namespace, params.InstanceName, params.Role,
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
	image *util.Image,
	vectorConfigMapName string) {
	decorator := builder.VectorDecorator{
		WorkloadObject:           dep,
		Image:                    image,
		LogVolumeName:            hdfsv1alpha1.KubedoopLogVolumeMountName,
		VectorConfigVolumeName:   hdfsv1alpha1.HdfsConfigVolumeMountName,
		VectorConfigMapName:      vectorConfigMapName,
		LogProviderContainerName: logProvider,
	}
	err := decorator.Decorate()
	if err != nil {
		return
	}
}
