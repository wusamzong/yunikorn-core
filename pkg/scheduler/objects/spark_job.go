package objects

import (
	"fmt"
	"math"
)

type JobsDAG struct {
	Vectors []*Job
}

type Job struct {
	ID         int
	replicaNum int
	replicaCpu int
	replicaMem int
	actionNum  int
	makespan   float64
	replicas   []*replica
	children   []*Job
	parent     []*Job
}

type replica struct {
	ID            int
	node          *node
	actions       []*action
	finalDataSize map[*Job]float64
	// node selected result
	minTime  float64
	minDr    float64
	minValue float64
}

type action struct {
	ID            int
	executionTime float64
	datasize      map[*replica]float64
}

type cell struct {
	currentNode   node
	executionTime float64
	datasize      map[*replica]float64
}

type node struct {
	ID            int
	cpu           int
	mem           int
	allocatedCpu  int
	allocatedMem  int
	executionRate float64
}

type bandwidth struct {
	values map[*node]map[*node]float64
}

func (j *Job) createReplica() *replica {
	r := &replica{
		ID:      len(j.replicas),
		node:    nil,
		actions: []*action{},
	}
	j.replicas = append(j.replicas, r)
	return r
}

func (job *Job) allParentScheduled(scheduledJob map[*Job][]bool) bool {
	allScheduled := true
	for _, parent := range job.parent {
		if _, exist := scheduledJob[parent]; !exist {
			allScheduled = false
		}
	}
	return allScheduled
}

func (job *Job) decideNode(nodes []*node, bw *bandwidth) bool {
	job.makespan = 0
	allocatedReplica := []*replica{}
	for idx, replica := range job.replicas {
		replica.minValue = math.MaxFloat64
		for _, node := range nodes {
			if node.cpu-node.allocatedCpu < job.replicaCpu || node.mem-node.allocatedMem < job.replicaMem {
				continue
			}

			var time float64

			// transmission time + Execution time "Inside" the Job
			for _, action := range replica.actions {
				var transmissionTime, executionTime float64
				executionTime = action.executionTime * node.executionRate
				transmissionTime = 0
				if idx != 0 {
					for i := 0; i < idx; i++ {
						from := node
						to := job.replicas[i].node
						datasize := action.datasize[job.replicas[i]]
						var curTransmissionTime float64
						if bw.values[from][to] == 0 {
							curTransmissionTime = 0
						} else {
							curTransmissionTime = datasize / bw.values[from][to]
						}

						if transmissionTime < curTransmissionTime {
							transmissionTime = curTransmissionTime
						}
					}
				}
				time += (executionTime + transmissionTime)
			}

			// transmission time "between" the Jobs
			var transmissionTime float64
			transmissionTime = 0
			for _, parent := range job.parent {
				for _, parentReplica := range parent.replicas {
					from := parentReplica.node
					to := node
					datasize := parentReplica.finalDataSize[job]
					var curTransmissionTime float64
					if bw.values[from][to] == 0 {
						curTransmissionTime = 0
					} else {
						curTransmissionTime = datasize / bw.values[from][to]
					}

					if transmissionTime < curTransmissionTime {
						transmissionTime = curTransmissionTime
					}
				}
			}
			time += transmissionTime

			// DR of replica on node
			nodeCapacityVector := []float64{
				float64(node.cpu) / float64(node.cpu+node.mem),
				float64(node.mem) / float64(node.cpu+node.mem),
			}
			requestVector := []float64{
				float64(job.replicaCpu) / float64(node.cpu),
				float64(job.replicaMem) / float64(node.mem),
			}
			resourceShare := []float64{
				requestVector[0] / nodeCapacityVector[0],
				requestVector[1] / nodeCapacityVector[1],
			}
			var dr float64
			if resourceShare[0] > resourceShare[1] {
				dr = resourceShare[0]
			} else {
				dr = resourceShare[1]
			}
			if time*dr < replica.minValue {
				replica.minTime = time
				replica.minDr = dr
				// replica.minValue = math.Pow(time, 2) + math.Pow(dr, 2)
				replica.minValue = time * dr
				replica.node = node
			}
		}
		if replica.node == nil {
			fmt.Println("no enough node for job", job.ID, "'s replica", idx)
			fmt.Println("release allocated Replica")
			for _, replica := range allocatedReplica {
				replica.node.allocatedCpu -= job.replicaCpu
				replica.node.allocatedMem -= job.replicaMem
			}
			return false
		}
		allocatedReplica = append(allocatedReplica, replica)
		replica.node.allocatedCpu += job.replicaCpu
		replica.node.allocatedMem += job.replicaMem

		if replica.minTime > job.makespan {
			job.makespan = replica.minTime
		}
	}
	for idx, replica := range job.replicas {
		fmt.Println("Job", job.ID, ",replica", idx, ",nodeID:", replica.node.ID,
			",minTime:", replica.minTime, ",min DR:", replica.minDr, ",minValue:", replica.minValue)
	}
	return true
}

func (r *replica) createAction(exeTime float64) *action {
	a := &action{
		ID:            len(r.actions),
		executionTime: exeTime,
		datasize:      map[*replica]float64{},
	}
	r.actions = append(r.actions, a)
	return a
}
