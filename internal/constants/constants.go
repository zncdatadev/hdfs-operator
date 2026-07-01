/*
Copyright 2024 zncdatadev.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package constants holds HDFS product-specific constants used by the operator
// (image defaults, container names, config file names). These are the values the
// operator applies when the user does not override them via the CR.
package constants

// Product image defaults. The container image is modeled by the SDK
// commonsv1alpha1.ImageSpec; these supply the product defaults used to build the
// operator's fallback image reference ({Repo}/{ProductName}:{ProductVersion}-kubedoop{KubedoopVersion}).
const (
	ProductName            = "hadoop"
	DefaultImageRepo       = "quay.io/zncdatadev"
	DefaultProductVersion  = "3.4.1"
	DefaultKubedoopVersion = "0.0.0-dev"
)

// Primary container names per role. The SDK BaseRoleGroupHandler renames the primary
// container (via MainContainerName) and keys per-container logging on these names.
const (
	NameNodeContainerName    = "namenode"
	DataNodeContainerName    = "datanode"
	JournalNodeContainerName = "journalnode"
)

// Config file names rendered into the role group ConfigMap.
const (
	CoreSiteXML = "core-site.xml"
	HdfsSiteXML = "hdfs-site.xml"
)
