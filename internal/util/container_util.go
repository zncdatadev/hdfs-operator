package util

// ContainerComponent defines the container component type
type ContainerComponent string

// Container component constants
const (
	NameNode        ContainerComponent = "namenode"
	DataNode        ContainerComponent = "datanode"
	JournalNode     ContainerComponent = "journalnode"
	Zkfc            ContainerComponent = "zkfc"
	FormatNameNode  ContainerComponent = "format-namenode"
	FormatZookeeper ContainerComponent = "format-zookeeper"
)
