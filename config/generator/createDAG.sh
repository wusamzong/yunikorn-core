seed=0
RANDOM=$seed;
NODES=10
WIDTH=5
START_NODE=1

# Create an associative array to track dependencies
declare -A dependencies
declare -A visited
declare -A stack

# Function to check for cycles
function hasCycle() {
  local node=$1

  if [ "${visited[$node]}" = "true" ]; then
      return 0  # Cycle detected
  fi

  if [ "${stack[$node]}" = "true" ]; then
      return 1  # No cycle
  fi

  stack["$node"]="true"
  visited["$node"]="true"

  local deps=(${dependencies[$node]//-/ })

  for dep in "${deps[@]}"; do
      if ! hasCycle "$dep" "$stack" "$visited"; then
          return 0  # Cycle detected
      fi
  done

  stack["$node"]="false"
  return 1  # No cycle
}

function DAG() {
  # Generate random dependencies
  for ((i = 1; i <= NODES; i++)); do
    if [ "$i" -eq "$START_NODE" ]; then
      # Make sure the starting node has no dependencies
      dependencies["$i"]=""
    else
      # Generate a random number of dependencies for each node
      local dep_candidates=($(seq 1 $NODES))
      local num_dependencies=$((RANDOM % WIDTH+1))
      for ((j = 1; j <= num_dependencies; j++)); do
          # Ensure the dependency is not the node itself
          while true; do
              random_index=$((RANDOM % ${#dep_candidates[@]}))
              dependency=${dep_candidates[$random_index]}
              if [ "$dependency" != "$i" ]; then
                dep_candidates=(${dep_candidates[@]:0:$random_index} ${dep_candidates[@]:$((random_index+1))})
                break
              fi
          done
          # Add the dependency to the array
          if [ 1 -ne "$j" ]; then
            dependencies["$i"]+="-"
          fi
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

cycle=1  # Initialize cycle to 1 to enter the loop

while [ $cycle -eq 1 ]; do
  cycle=0  # Reset cycle to 0 at the beginning of each iteration

  for node in "${!dependencies[@]}"; do
    if ! hasCycle "$node"; then
      echo "Cycle detected in the graph. Regenerating dependencies..."
      cycle=1  # Set cycle to 1 to indicate that a cycle was detected
      seed=$((seed+1))
      RANDOM=$seed
      dependencies=()
      DAG
      break
    fi
  done
done

echo $seed

# for (( i=1; i<=NODES; i++))
# do
#   parent=${dependencies[$i]}
#   cat <<EOF >> "dag01.yaml"
# apiVersion: batch/v1
# kind: Job
# metadata:
#   name: task$i
# spec:
#   completions: 1
#   parallelism: 1
#   template:
#     metadata:
#       labels:
#         app: sleep
#         applicationId: "dag01"
#         queue: root.default
#         parent: "$parent"
#       name: task$i
#     spec:
#       schedulerName: yunikorn
#       restartPolicy: Never
#       containers:
#         - name: sleep300
#           image: "alpine:latest"
#           command: ["sleep", "300"]
#           resources:
#             requests:
#               cpu: "100m"
#               memory: "2000M"
# ---
# EOF
# done

