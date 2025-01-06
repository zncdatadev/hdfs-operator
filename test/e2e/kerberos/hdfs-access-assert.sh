---
# hdfs access assert script configmap
apiVersion: v1
kind: ConfigMap
metadata:
  name: krb5-hdfs-access
data:
    hdfs-access-assert.sh: |
        #!/bin/bash
        set -ex

        echo "Running HDFS access test"

        echo "list credential in krb5 keytab"
        klist -k /kubedoop/kerberos/keytab

        # Extract unique principals from keytab
        principals=$(klist -k /kubedoop/kerberos/keytab | grep -v "Keytab name:" | awk '{print $2}' | sort -u)

        for principal in $principals; do
            echo "Testing with principal: $principal"

            echo "Authenticating with keytab"
            kdestroy
            kinit -kt /kubedoop/kerberos/keytab "$principal"

            # Test HDFS operations
            TEST_DIR="/tmp/test-$(date +%s)"
            TEST_FILE="$TEST_DIR/test.txt"

            echo "Creating test directory"
            bin/hdfs dfs -mkdir -p "$TEST_DIR"

            echo "Writing test data"
            echo "Hello HDFS" | bin/hdfs dfs -put - "$TEST_FILE"

            echo "Reading test data"
            bin/hdfs dfs -cat "$TEST_FILE"

            echo "Listing directory"
            bin/hdfs dfs -ls "$TEST_DIR"

            echo "Cleaning up"
            bin/hdfs dfs -rm -r "$TEST_DIR"

            echo "Test completed for $principal"
        done

        echo "All HDFS access tests completed successfully"
