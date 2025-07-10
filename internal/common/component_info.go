package common

import (
	hdfsv1alpha1 "github.com/zncdatadev/hdfs-operator/api/v1alpha1"
	"github.com/zncdatadev/operator-go/pkg/reconciler"
)

// ComponentInfo contains information about a specific HDFS component role group
type ComponentInfo struct {
	InstanceName  string
	Namespace     string
	GroupName     string
	Replicas      int32
	Config        interface{}               // The merged role group config
	RoleGroupInfo *reconciler.RoleGroupInfo // RoleGroupInfo for getting full names
}

// ClusterComponentsInfo contains all component information for an HDFS cluster
type ClusterComponentsInfo struct {
	InstanceName  string
	Namespace     string
	NameNode      map[string]*ComponentInfo // key: groupName
	DataNode      map[string]*ComponentInfo // key: groupName
	JournalNode   map[string]*ComponentInfo // key: groupName
	ClusterConfig *hdfsv1alpha1.ClusterConfigSpec
}

// NewClusterComponentsInfo creates a new ClusterComponentsInfo
func NewClusterComponentsInfo(instanceName, namespace string, clusterConfig *hdfsv1alpha1.ClusterConfigSpec) *ClusterComponentsInfo {
	return &ClusterComponentsInfo{
		InstanceName:  instanceName,
		Namespace:     namespace,
		NameNode:      make(map[string]*ComponentInfo),
		DataNode:      make(map[string]*ComponentInfo),
		JournalNode:   make(map[string]*ComponentInfo),
		ClusterConfig: clusterConfig,
	}
}

// GetJournalNodeReplicas gets the total replicas from all JournalNode groups
// This replaces the MergedCache.getJournalNodeReplicates() call
func (c *ClusterComponentsInfo) GetJournalNodeReplicas(groupName string) int32 {
	var totalReplicas int32 = 0
	for _, info := range c.JournalNode {
		if info.GroupName == groupName {
			totalReplicas += info.Replicas
		}
	}
	return totalReplicas

}

// GetNameNodeServiceNames gets all NameNode service names
func (c *ClusterComponentsInfo) GetNameNodeServiceNames(groupName string) []string {
	nameNodeConfig := c.NameNode[groupName]
	if nameNodeConfig == nil {
		return nil
	}
	return []string{nameNodeConfig.RoleGroupInfo.GetFullName()}
}

// GetJournalNodeServiceNames gets all JournalNode service names
func (c *ClusterComponentsInfo) GetJournalNodeServiceNames() []string {
	var serviceNames []string
	for _, info := range c.JournalNode {
		serviceNames = append(serviceNames, info.RoleGroupInfo.GetFullName())
	}
	return serviceNames

}

// GetJournalNodeServicesForSharedEdits gets journal node services formatted for HDFS shared edits configuration
func (c *ClusterComponentsInfo) GetJournalNodeServicesForSharedEdits() []string {
	serviceNames := c.GetJournalNodeServiceNames()
	var journalNodes []string
	for _, serviceName := range serviceNames {
		// Add the journal port (8485) to each service
		journalNodes = append(journalNodes, serviceName+":8485")
	}

	return journalNodes
}

// GetStatefulSetName returns the StatefulSet name using RoleGroupInfo
func (info *ComponentInfo) GetStatefulSetName() string {
	if info.RoleGroupInfo != nil {
		return info.RoleGroupInfo.GetFullName()
	}
	// Fallback to old naming convention if RoleGroupInfo is not available
	return ""
}

// GetServiceName returns the Service name using RoleGroupInfo
func (info *ComponentInfo) GetServiceName() string {
	if info.RoleGroupInfo != nil {
		return info.RoleGroupInfo.GetFullName()
	}
	// Fallback to stored ServiceName if RoleGroupInfo is not available
	return ""
}
