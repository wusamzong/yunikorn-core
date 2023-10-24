RANDOM=40;
NODES=15
WIDTH=5
START_NODE=1

# Create an associative array to track dependencies
declare -A dependencies
function DAG() {
  # Generate random dependencies
  for ((i = 1; i <= NODES; i++)); do
    if [ "$i" -eq "$START_NODE" ]; then
      # Make sure the starting node has no dependencies
      dependencies["$i"]=""
    else
      # Generate a random number of dependencies for each node
      num_dependencies=$((RANDOM % WIDTH + 1))
      for ((j = 1; j <= num_dependencies; j++)); do
          # Ensure the dependency is not the node itself
          while true; do
              dependency=$((RANDOM % NODES + 1))
              if [ "$dependency" != "$i" ]; then
                  break
              fi
          done
          if [ 1 -ne "$j" ]; then
            dependencies["$i"]+=","
          fi
          # Add the dependency to the array
          dependencies["$i"]+="$dependency"

      done
    fi
  done

  # Print the generated DAG
  for node in "${!dependencies[@]}"; do
      echo "Node $node depends on:${dependencies[$node]}"
  done

}

DAG

for (( i=1; i<=NODES; i++))
do
  parent=${dependencies[$i]}
  cat <<EOF >> "dag01.yaml"
apiVersion: batch/v1
kind: Job
metadata:
  name: task$i
spec:
  completions: 1
  parallelism: 1
  template:
    metadata:
      labels:
        app: sleep
        applicationId: "dag01"
        queue: root.default
        parent: $parent
      name: task$i
    spec:
      schedulerName: yunikorn
      restartPolicy: Never
      containers:
        - name: sleep300
          image: "alpine:latest"
          command: ["sleep", "300"]
          resources:
            requests:
              cpu: "100m"
              memory: "2000M"
---
EOF
done

