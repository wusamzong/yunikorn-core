for (( i=1; i<=15; i++))
do
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
      name: task$1
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