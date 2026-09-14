# HDFS Operator E2E (Chainsaw)

These are [Chainsaw](https://kyverno.github.io/chainsaw/) suites (declarative YAML, not Go) that
deploy an `HdfsCluster` and assert on the resulting objects (and, for some suites, live HDFS
behaviour). Use them to regress the operator against a real cluster.

## Running

The Makefile drives the whole flow — creating a Kind cluster, installing the Kubedoop platform
operators (`OPERATOR_DEPENDS = commons-operator listener-operator secret-operator
zookeeper-operator`), building/loading the operator image, deploying it, and running Chainsaw:

```sh
make chart-e2e                     # full: kind + deps + build + deploy(helm chart) + chainsaw
# iterate on the tests once the cluster/operator are up:
make chainsaw-e2e                  # just re-run the Chainsaw suites
make cleanup-chainsaw-cluster      # tear the Kind cluster down
```

Override the image / product version as usual, e.g. `make chart-e2e IMG=<img>`. `.chainsaw.yaml`
sets `failFast` and `parallel: 1`; timeouts are generous (HDFS HA takes a while to format and elect
a leader).

## Product image requirements

Beyond what the Makefile installs, the suites assume the HDFS **product image** bundles the runtime
helpers the operator references:

- `/kubedoop/jmx/`: `jmx_prometheus_javaagent.jar` + the per-role rules `namenode.yaml` /
  `datanode.yaml` / `journalnode.yaml`. The operator adds `-javaagent:.../jmx_prometheus_javaagent.jar=<metricPort>:.../<role>.yaml`
  to `HDFS_<ROLE>_OPTS`, exposing Prometheus metrics on 8183 / 8082 / 8081 (`smoke/observability`).
- `/kubedoop/oauth2-proxy/oauth2-proxy` — the OIDC sidecar binary (`oidc`).
- Kerberos client tooling (`kinit`) — used by the init containers under Kerberos (`kerberos`).

## Suites

| Suite | Extra dependency (installed by the suite) | Verifies |
|-------|-------------------------------------------|----------|
| `smoke/simple` | — | HA cluster comes up (NN/JN/DN StatefulSets ready) |
| `smoke/cluster-operation` | — | `clusterOperation` pause/stop |
| `smoke/override-pdb` | — | `roleConfig.podDisruptionBudget` produces the role PDB |
| `smoke/observability` | Prometheus (Helm) | `-metrics` Services + Prometheus metrics on 8183/8082/8081, scraped |
| `logging` | Vector aggregator | `logging.enableVectorAgent` injects the Vector sidecar |
| `kerberos` | KDC / SecretClass | Kerberos HA cluster + authenticated access |
| `oidc` | Keycloak + `AuthenticationClass` | oauth2-proxy fronts the NameNode UI with OIDC |

Names are lowercase: StatefulSet `<cluster>-<role>-<group>`, discovery ConfigMap `<cluster>`,
metrics Service `<cluster>-<role>-<group>-metrics`.
