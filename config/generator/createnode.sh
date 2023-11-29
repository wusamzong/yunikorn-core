#!/bin/bash
# RANDOM=30
for (( i=1;i<=5; i++))
do
  cpu=$((RANDOM%4+1))
  cpu=$((cpu*4))
  memory=$((RANDOM%8+1))
  memory=$((memory*4))
  memory=$memory"Gi"
  echo $cpu $memory
  kubectl apply -f - <<EOF
  apiVersion: v1
  kind: Node
  metadata:
    annotations:
      node.alpha.kubernetes.io/ttl: "0"
      kwok.x-k8s.io/node: fake
    labels:
      beta.kubernetes.io/arch: amd64
      beta.kubernetes.io/os: linux
      kubernetes.io/arch: amd64
      kubernetes.io/hostname: worker$i
      kubernetes.io/os: linux
      kubernetes.io/role: agent
      node-role.kubernetes.io/agent: ""
      type: kwok
    name: worker$i
  spec:
    taints: # Avoid scheduling actual running pods to fake Node
    - effect: NoSchedule
      key: kwok.x-k8s.io/node
      value: fake
  status:
    allocatable:
      cpu: $cpu
      memory: $memory
      pods: 110
    capacity:
      cpu: $cpu
      memory: $memory
      pods: 110
    nodeInfo:
      architecture: amd64
      bootID: ""
      containerRuntimeVersion: ""
      kernelVersion: ""
      kubeProxyVersion: fake
      kubeletVersion: fake
      machineID: ""
      operatingSystem: linux
      osImage: ""
      systemUUID: ""
    phase: Running
EOF
done