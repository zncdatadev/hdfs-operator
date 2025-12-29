### Have you searched existing issues?  🔎

no check

### Summary 💡

Add JMX (Java Management Extensions) monitoring support to HDFS operator by integrating the Prometheus JMX Java Agent. This will enable metrics collection for NameNode, DataNode, and JournalNode components.

The implementation should add the following JVM argument to all HDFS components:
```
-javaagent:/stackable/jmx/jmx_prometheus_javaagent.jar={metrics_port}:/stackable/jmx/{hdfs_role}.yaml
```

Where:
- `{metrics_port}` is the configured metrics port for each component
- `{hdfs_role}` is the component type (namenode, datanode, journalnode)

### Examples 🌈

**Current Implementation Gap:**

Currently in `internal/common/util.go` (line 173), the JVM args only include:
```go
jvmArgs = append(jvmArgs, "-Xmx419430k")
```

**Expected Implementation:**

The JVM args should include the JMX agent:
```go
jvmArgs = append(jvmArgs, "-Xmx419430k")
jvmArgs = append(jvmArgs, fmt.Sprintf("-javaagent:/stackable/jmx/jmx_prometheus_javaagent.jar=%d:/stackable/jmx/%s.yaml", metricsPort, hdfsRole))
```

**Similar Implementations:**

This approach aligns with other Kubedoop/Stackable operators that already implement JMX monitoring for their respective components.

**Follow-up Work:**

After implementing JMX agent support, the next step would be to add ServiceMonitor resources to enable Prometheus scraping of the exposed metrics.

### Motivation 🔦

**Problem Statement:**

HDFS clusters currently lack built-in observability for JVM-level metrics, making it difficult to:
- Monitor JVM heap usage, garbage collection, and thread states
- Track HDFS-specific metrics (block operations, RPC calls, etc.)
- Set up alerts based on cluster health metrics
- Debug performance issues in production environments

**Business Value:**

1. **Consistency**: Aligns HDFS operator with other Kubedoop operators that already support JMX monitoring
2. **Foundation for Observability**: Establishes the groundwork for comprehensive monitoring stack
3. **Production Readiness**: Enables operators to monitor and maintain HDFS clusters effectively
4. **Kubernetes Native**: Integrates seamlessly with Prometheus/ServiceMonitor patterns common in Kubernetes

**Use Cases:**

- Operations teams need to monitor HDFS cluster health in production
- SREs require metrics for capacity planning and performance tuning
- Alerting systems need reliable metrics to trigger notifications
- Grafana dashboards need data sources for HDFS cluster visualization

**Implementation Steps:**

1. Add JMX agent JVM argument to all HDFS components (NameNode, DataNode, JournalNode)
2. Configure appropriate metrics ports for each component
3. Include JMX exporter configuration files for each HDFS role
4. Add ServiceMonitor CRDs for Prometheus integration
5. Document metrics endpoint usage in operator documentation
