package common

import (
	"context"

	"emperror.dev/errors"
	hdfsv1alpha1 "github.com/zncdatadev/hdfs-operator/api/v1alpha1"
	"github.com/zncdatadev/hdfs-operator/internal/constant"
	commonsv1alpha1 "github.com/zncdatadev/operator-go/pkg/apis/commons/v1alpha1"
	"github.com/zncdatadev/operator-go/pkg/builder"
	"github.com/zncdatadev/operator-go/pkg/client"
	"github.com/zncdatadev/operator-go/pkg/productlogging"
	"github.com/zncdatadev/operator-go/pkg/reconciler"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
)

// ConfigMapBuilder is the common builder for HDFS ConfigMaps
type ConfigMapBuilder struct {
	builder.ConfigMapBuilder
	client                        *client.Client
	roleType                      constant.Role
	clusterName                   string
	roleGroupInfo                 *reconciler.RoleGroupInfo
	hdfsCluster                   *hdfsv1alpha1.HdfsCluster
	overrides                     *commonsv1alpha1.OverridesSpec
	roleConfig                    *commonsv1alpha1.RoleGroupConfigSpec
	ctx                           context.Context
	component                     ConfigMapComponentBuilder
	vectorAggregatorConfigMapName string
}

func NewConfigMapReconciler(
	ctx context.Context,
	client *client.Client,
	roleType constant.Role,
	roleGroupInfo *reconciler.RoleGroupInfo,
	overrides *commonsv1alpha1.OverridesSpec,
	roleConfig *commonsv1alpha1.RoleGroupConfigSpec,
	hdfsCluster *hdfsv1alpha1.HdfsCluster,
	component ConfigMapComponentBuilder,
	vectorAggregatorConfigMapName string,
) reconciler.ResourceReconciler[builder.ConfigBuilder] {
	builder := NewConfigMapBuilder(
		ctx,
		client,
		roleType,
		roleGroupInfo,
		overrides,
		roleConfig,
		hdfsCluster,
		component,
		vectorAggregatorConfigMapName,
	)

	return reconciler.NewGenericResourceReconciler(
		client,
		builder,
	)
}

// NewConfigMapBuilder creates a new ConfigMapBuilder with common configuration
func NewConfigMapBuilder(
	ctx context.Context,
	client *client.Client,
	roleType constant.Role,
	roleGroupInfo *reconciler.RoleGroupInfo,
	overrides *commonsv1alpha1.OverridesSpec,
	roleConfig *commonsv1alpha1.RoleGroupConfigSpec,
	hdfsCluster *hdfsv1alpha1.HdfsCluster,
	component ConfigMapComponentBuilder,
	vectorAggregatorConfigMapName string,
) builder.ConfigBuilder {
	return &ConfigMapBuilder{
		ConfigMapBuilder: *builder.NewConfigMapBuilder(
			client,
			roleGroupInfo.GetFullName(),
			func(o *builder.Options) {
				o.Labels = roleGroupInfo.GetLabels()
				o.Annotations = roleGroupInfo.GetAnnotations()
			},
		),
		client:                        client,
		roleType:                      roleType,
		clusterName:                   roleGroupInfo.GetClusterName(),
		roleGroupInfo:                 roleGroupInfo,
		hdfsCluster:                   hdfsCluster,
		overrides:                     overrides,
		roleConfig:                    roleConfig,
		ctx:                           ctx,
		component:                     component,
		vectorAggregatorConfigMapName: vectorAggregatorConfigMapName,
	}
}

// ConfigMapComponentBuilder defines methods that should be implemented by role-specific builders
type ConfigMapComponentBuilder interface {
	// BuildConfig returns component-specific configuration content
	BuildConfig() (map[string]string, error)
	// GetConfigOverrides returns any configuration overrides specific to this component
	GetConfigOverrides() map[string]map[string]string
}

// Build constructs the ConfigMap object combining common and component-specific configurations
func (b *ConfigMapBuilder) Build(ctx context.Context) (ctrlclient.Object, error) {
	// Get component-specific configurations
	configs, err := b.component.BuildConfig()
	if err != nil {
		return nil, err
	}

	// Add configurations to ConfigMap
	for filename, content := range configs {
		b.AddItem(filename, content)
	}

	// Apply any global overrides from the spec
	if b.overrides != nil && b.overrides.ConfigOverrides != nil {
		for filename, overrides := range b.overrides.ConfigOverrides {
			if content, ok := overrides[filename]; ok {
				b.AddItem(filename, content)
			}
		}
	}

	// Apply component-specific configuration overrides
	if componentOverrides := b.component.GetConfigOverrides(); componentOverrides != nil {
		if err := b.applyComponentOverrides(componentOverrides); err != nil {
			return nil, err
		}
	}

	// vector config
	if b.roleConfig != nil {
		if isEnabled, err := IsVectorEnable(b.roleConfig.Logging); err != nil {
			return nil, err
		} else if isEnabled {
			if vectorConfig, err := b.buildVectorConfig(ctx); err != nil {
				return nil, err
			} else if vectorConfig != "" {
				b.AddItem(builder.VectorConfigFileName, vectorConfig) // vector.yaml
			}
		}
	}

	return b.GetObject(), nil
}

// applyComponentOverrides applies component-specific configuration overrides
func (b *ConfigMapBuilder) applyComponentOverrides(overrides map[string]map[string]string) error {
	// Handle configuration file overrides
	if overrides != nil {
		if coreSiteOverrides, ok := overrides[hdfsv1alpha1.CoreSiteFileName]; ok {
			for key, value := range coreSiteOverrides {
				// This would need proper merging logic with existing core-site.xml
				_ = key
				_ = value
			}
		}
		if hdfsSiteOverrides, ok := overrides[hdfsv1alpha1.HdfsSiteFileName]; ok {
			for key, value := range hdfsSiteOverrides {
				// This would need proper merging logic with existing hdfs-site.xml
				_ = key
				_ = value
			}
		}
		// Add more specific override handling as needed
	}
	return nil
}

// vector config
func (b *ConfigMapBuilder) buildVectorConfig(ctx context.Context) (string, error) {
	if b.roleConfig != nil && b.roleConfig.Logging != nil && b.roleConfig.Logging.EnableVectorAgent != nil {
		if b.vectorAggregatorConfigMapName == "" {
			return "", errors.New("vector is enabled but vectorAggregatorConfigMapName is not set")
		}
		if *b.roleConfig.Logging.EnableVectorAgent {
			s, err := productlogging.MakeVectorYaml(
				ctx,
				b.client.Client,
				b.client.GetOwnerNamespace(),
				b.clusterName,
				b.roleGroupInfo.RoleName,
				b.roleGroupInfo.RoleGroupName,
				b.vectorAggregatorConfigMapName,
			)
			if err != nil {
				return "", err
			}
			return s, nil
		}
	}
	return "", nil
}

// GetVectorConfigMapName extracts vector aggregator config map name from cluster spec
func GetVectorConfigMapName(cluster *hdfsv1alpha1.HdfsCluster) string {
	if cluster == nil {
		return ""
	}
	if cluster.Spec.ClusterConfig != nil && cluster.Spec.ClusterConfig.VectorAggregatorConfigMapName != "" {
		return cluster.Spec.ClusterConfig.VectorAggregatorConfigMapName
	}
	return ""
}
