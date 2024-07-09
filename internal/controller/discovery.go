package controller

import (
	"context"
	"fmt"
	"strings"

	"emperror.dev/errors"
	hdfsv1alpha1 "github.com/zncdatadev/hdfs-operator/api/v1alpha1"
	"github.com/zncdatadev/hdfs-operator/internal/common"
	"github.com/zncdatadev/hdfs-operator/internal/util"
	listenerv1alpha1 "github.com/zncdatadev/listener-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var discoveryLog = ctrl.Log.WithName("discovery")

type Discovery struct {
	common.GeneralResourceStyleReconciler[*hdfsv1alpha1.HdfsCluster, any]
}

func NewDiscovery(
	scheme *runtime.Scheme,
	instance *hdfsv1alpha1.HdfsCluster,
	client client.Client,
) *Discovery {
	var mergedCfg any
	d := &Discovery{
		GeneralResourceStyleReconciler: *common.NewGeneraResourceStyleReconciler(
			scheme,
			instance,
			client,
			"",
			nil,
			mergedCfg,
		),
	}
	return d
}

// Build implements the ResourceBuilder interface
func (d *Discovery) Build(ctx context.Context) (client.Object, error) {
	if hdfsSiteXml, err := d.makeHdfsSiteXmlData(ctx); err != nil {
		return nil, err
	} else {

		return &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      d.Instance.GetName(),
				Namespace: d.Instance.Namespace,
				Labels:    d.MergedLabels,
			},
			Data: map[string]string{
				"core-site.xml": d.makeCoreSiteXmlData(),
				"hdfs-site.xml": hdfsSiteXml,
			},
		}, nil
	}
}

func (d *Discovery) makeCoreSiteXmlData() string {
	generator := common.CoreSiteXmlGenerator{InstanceName: d.Instance.Name}
	return generator.EnableKerberos(d.Instance.Spec.ClusterConfigSpec, d.Instance.Namespace, true).Generate()
}

func (d *Discovery) makeHdfsSiteXmlData(ctx context.Context) (string, error) {
	xml := util.NewXmlConfiguration(d.commonHdfsSiteXml())
	properties, err := d.makeDynamicHdfsSiteXml(ctx)
	if err != nil {
		return "", err
	}
	if common.IsKerberosEnabled(d.Instance.Spec.ClusterConfigSpec) {
		properties = append(properties, common.SecurityDiscoveryHdfsSiteXml()...)
	}
	return xml.String(properties), nil
}

// make hdfs-site.xml data
func (d *Discovery) commonHdfsSiteXml() []util.XmlNameValuePair {
	return []util.XmlNameValuePair{
		{
			Name:  "dfs.nameservices",
			Value: d.Instance.GetName(),
		},
		{
			Name:  "dfs.client.failover.proxy.provider.simple-hdfs",
			Value: "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider",
		},
	}
}

func (d *Discovery) makeDynamicHdfsSiteXml(ctx context.Context) ([]util.XmlNameValuePair, error) {
	var hosts util.XmlNameValuePair
	var podNames []string
	var connections []util.XmlNameValuePair
	var err error
	nameNodeGroups := d.Instance.Spec.NameNode.RoleGroups

	// get pod names
	podNames = d.getPodNames(nameNodeGroups)
	// make discovery hosts
	hosts = d.makeDiscoveryHosts(podNames)
	// make connections
	if connections, err = d.createConnections(ctx, podNames); err != nil {
		return nil, err
	}

	var all []util.XmlNameValuePair
	all = append(all, hosts)
	all = append(all, connections...)
	return all, nil
}

// get pod names
// Note: pod name generated by group name and replicas
func (d *Discovery) getPodNames(nameNodeGroups map[string]*hdfsv1alpha1.NameNodeRoleGroupSpec) []string {
	var podNames []string
	for groupName := range nameNodeGroups {
		cacheKey := common.CreateRoleCfgCacheKey(d.Instance.Name, common.NameNode, groupName)
		nameNodeStatefulSetName := common.CreateNameNodeStatefulSetName(d.Instance.Name, groupName)
		if cfg, ok := common.MergedCache.Get(cacheKey); ok {
			roleGroupCfg := cfg.(*hdfsv1alpha1.NameNodeRoleGroupSpec)
			replicates := roleGroupCfg.Replicas
			groupPodNames := common.CreatePodNamesByReplicas(replicates, nameNodeStatefulSetName)
			podNames = append(podNames, groupPodNames...)
		}
	}
	return podNames
}

// make discovery hosts
func (d *Discovery) makeDiscoveryHosts(podNames []string) util.XmlNameValuePair {
	return util.XmlNameValuePair{
		Name:  "dfs.ha.namenodes." + d.Instance.GetName(),
		Value: strings.Join(podNames, ", "),
	}
}

// create http and rpc address
// http key pattern: "dfs.namenode.http-address.{hdfs_instance_name}.{podName}"
// rpc key pattern: "dfs.namenode.rpc-address.{hdfs_instance_name}.{podName}"
// http key example:
//
//	dfs.namenode.http-address.simple-hdfs.simple-hdfs-namenode-default-0
//	dfs.namenode.http-address.simple-hdfs.simple-hdfs-namenode-default-1
//
// rpc key example:
//
//	dfs.namenode.rpc-address.simple-hdfs.simple-hdfs-namenode-default-0
//	dfs.namenode.rpc-address.simple-hdfs.simple-hdfs-namenode-default-1
//
// value pattern: "{listener_address}:{listener_port}"
// value example:
//
//	0.0.0.0:9870
func (d *Discovery) createPortNameAddress(
	ctx context.Context,
	podNames []string,
	portName string,
	cache *map[string]*listenerv1alpha1.IngressAddressSpec) ([]util.XmlNameValuePair, error) {
	var connections []util.XmlNameValuePair
	for _, podName := range podNames {
		var address *listenerv1alpha1.IngressAddressSpec
		var err error
		if address, err = d.getListenerAddress(cache, ctx, podName); err != nil {
			return nil, err
		}
		port, err := d.getPort(address, portName)
		if err != nil {
			discoveryLog.Error(err, "failed to get port from address by port name", "address",
				address, "portName", portName)
			return nil, err
		}

		name := fmt.Sprintf("dfs.namenode.%s-address.%s.%s", portName, d.Instance.GetName(), podName)
		value := fmt.Sprintf("%s:%d", address.Address, port)
		connections = append(connections, util.XmlNameValuePair{Name: name, Value: value})
	}
	return connections, nil
}

// create discovery connections
func (d *Discovery) createConnections(ctx context.Context, podNames []string) ([]util.XmlNameValuePair, error) {
	cache := make(map[string]*listenerv1alpha1.IngressAddressSpec)
	var httpConnections, rpcConnections []util.XmlNameValuePair
	var err error
	var connections []util.XmlNameValuePair
	// create http address
	httpConnections, err = d.createPortNameAddress(ctx, podNames, common.PortHttpName(d.Instance.Spec.ClusterConfigSpec), &cache)
	if err != nil {
		discoveryLog.Error(err, "failed to create http connections")
		return nil, err
	} else {
		connections = append(connections, httpConnections...)
	}

	// create rpc address
	rpcConnections, err = d.createPortNameAddress(ctx, podNames, hdfsv1alpha1.RpcName, &cache)
	if err != nil {
		discoveryLog.Error(err, "failed to create rpc connections")
	} else {
		connections = append(connections, rpcConnections...)
	}
	return connections, nil
}

// get port from address by port name
func (d *Discovery) getPort(address *listenerv1alpha1.IngressAddressSpec, portName string) (int32, error) {
	for _, port := range *address.Ports {
		if port.Name == portName {
			return port.Port, nil
		}
	}
	return 0, errors.Errorf("not found port in address %s by port name", portName)
}
func (d *Discovery) getListenerAddress(
	cache *map[string]*listenerv1alpha1.IngressAddressSpec,
	ctx context.Context,
	podName string) (*listenerv1alpha1.IngressAddressSpec, error) {
	cacheKey := podName
	cacheObj := *cache
	if address, ok := cacheObj[cacheKey]; ok {
		return address, nil
	}

	// get listener
	listener := &listenerv1alpha1.Listener{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cacheKey,
			Namespace: d.Instance.Namespace,
		},
	}
	resourceClient := common.NewResourceClient(ctx, d.Client, d.Instance.Namespace)
	err := resourceClient.Get(listener)
	if err != nil {
		discoveryLog.Error(err, "failed to get listener", "cacheKey", cacheKey)
		return nil, err
	}
	address := &listener.Status.IngressAddress[0]
	cacheObj[cacheKey] = address
	return address, nil
}
