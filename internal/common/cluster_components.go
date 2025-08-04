package common

import (
	hdfsv1alpha1 "github.com/zncdatadev/hdfs-operator/api/v1alpha1"
	"github.com/zncdatadev/hdfs-operator/internal/constant"
	"github.com/zncdatadev/operator-go/pkg/reconciler"
)

// PopulateClusterComponents populates the ComponentsInfo with all role groups from the HdfsCluster
func PopulateClusterComponents(
	instance *hdfsv1alpha1.HdfsCluster,
	componentsInfo *ClusterComponentsInfo,
	clusterInfo *reconciler.ClusterInfo,
) {
	// Populate NameNode components
	if instance.Spec.NameNode != nil && instance.Spec.NameNode.RoleGroups != nil {
		for groupName, nameNodeGroupSpec := range instance.Spec.NameNode.RoleGroups {
			roleInfo := reconciler.RoleInfo{ClusterInfo: *clusterInfo, RoleName: string(constant.NameNode)}
			roleGroupInfo := reconciler.RoleGroupInfo{RoleInfo: roleInfo, RoleGroupName: groupName}
			componentsInfo.NameNode[groupName] = &ComponentInfo{
				InstanceName:  instance.Name,
				Namespace:     instance.Namespace,
				GroupName:     groupName,
				Replicas:      *nameNodeGroupSpec.Replicas,
				Config:        nameNodeGroupSpec,
				RoleGroupInfo: &roleGroupInfo,
			}
		}
	}

	// Populate DataNode components
	if instance.Spec.DataNode != nil && instance.Spec.DataNode.RoleGroups != nil {
		for groupName, dataNodeGroupSpec := range instance.Spec.DataNode.RoleGroups {
			roleInfo := reconciler.RoleInfo{ClusterInfo: *clusterInfo, RoleName: string(constant.DataNode)}
			roleGroupInfo := reconciler.RoleGroupInfo{RoleInfo: roleInfo, RoleGroupName: groupName}
			componentsInfo.DataNode[groupName] = &ComponentInfo{
				InstanceName:  instance.Name,
				Namespace:     instance.Namespace,
				GroupName:     groupName,
				Replicas:      *dataNodeGroupSpec.Replicas,
				Config:        dataNodeGroupSpec,
				RoleGroupInfo: &roleGroupInfo,
			}
		}
	}

	// Populate JournalNode components
	if instance.Spec.JournalNode != nil && instance.Spec.JournalNode.RoleGroups != nil {
		for groupName, journalNodeGroupSpec := range instance.Spec.JournalNode.RoleGroups {
			roleInfo := reconciler.RoleInfo{ClusterInfo: *clusterInfo, RoleName: string(constant.JournalNode)}
			roleGroupInfo := reconciler.RoleGroupInfo{RoleInfo: roleInfo, RoleGroupName: groupName}
			componentsInfo.JournalNode[groupName] = &ComponentInfo{
				InstanceName:  instance.Name,
				Namespace:     instance.Namespace,
				GroupName:     groupName,
				Replicas:      *journalNodeGroupSpec.Replicas,
				Config:        journalNodeGroupSpec,
				RoleGroupInfo: &roleGroupInfo,
			}
		}
	}
}
