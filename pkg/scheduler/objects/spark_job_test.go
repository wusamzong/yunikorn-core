package objects

import (
	"container/heap"
	"fmt"
	"math"
	"math/rand"
	"testing"
)

func TestDecideNode(t *testing.T) {
	var randomSeed int64 = 100
	rand.Seed(randomSeed)

	nodes, bw := createRandNode()
	jobsDag := createRandJobDAG()

	job := jobsDag.Vectors[11]
	job.decideNode(nodes, bw)
}

func TestCalculateJobs(t *testing.T) {
	var randomSeed int64 = 100
	rand.Seed(randomSeed)

	nodes, bw := createRandNode()
	// jobsDag := createRandJobDAG()
	jobsDag := generateRandomDAG()
	allocManager := intervalAllocManager{current: 0}

	availJobsHeap := &JobHeap{}
	// storage the job has been tried but fail, fifo
	reserveQueue := []*Job{}
	scheduledJob := map[*Job][]bool{}
	heap.Init(availJobsHeap)

	// If the job is end of DAG then pop it into available Jobs Heap
	for _, job := range jobsDag.Vectors {
		if len(job.parent) == 0 {
			heap.Push(availJobsHeap, job)
		}
	}

	for availJobsHeap.Len() > 0 || len(reserveQueue) > 0 {
		var job *Job
		for availJobsHeap.Len() > 0 {
			job = availJobsHeap.Pop().(*Job)
			if _, exist := scheduledJob[job]; exist {
				continue
			}
			done := job.decideNode(nodes, bw)
			if done {
				allocManager.allocate(job)
				scheduledJob[job] = append(scheduledJob[job], true)
				for _, child := range job.children {
					_, exist := scheduledJob[child]
					if child.allParentScheduled(scheduledJob) && !exist {
						heap.Push(availJobsHeap, child)
					}
				}
			} else {
				reserveQueue = append(reserveQueue, job)
			}
		}

		for len(reserveQueue) > 0 {
			allocManager.nextInterval()
			fmt.Println("updateCurrent time", allocManager.current)
			releaseAlloc := allocManager.releaseResource()
			fmt.Println("release", releaseAlloc)

			for i:=0 ;i<len(reserveQueue);i++{
				job = reserveQueue[0]
				reserveQueue = reserveQueue[1:]
				if _, exist := scheduledJob[job]; exist {
					continue
				}
				done := job.decideNode(nodes, bw)
				if done {
					allocManager.allocate(job)
					scheduledJob[job] = append(scheduledJob[job], true)
					for _, child := range job.children {
						_, exist := scheduledJob[child]
						if child.allParentScheduled(scheduledJob) && !exist {
							heap.Push(availJobsHeap, child)
						}
					}
				}else{
					reserveQueue = append(reserveQueue, job)
					if len(allocManager.allocations) == 0 {
						fmt.Println("There is no enough space for job", job.ID)
						fmt.Println("Job", job)
						for _, node := range nodes {
							fmt.Printf("nodeId:%d, capacity:{%d, %d}, allocated:{%d, %d}\n", node.ID, node.cpu, node.mem, node.allocatedCpu, node.allocatedMem)
						}
						return
					}
				}
			}
		}
	}

	fmt.Println("makespan =", allocManager.getMakespan())
}

func TestCalculateLastJob(t *testing.T) {
	var randomSeed int64 = 100
	rand.Seed(randomSeed)

	nodes, bw := createRandNode()
	jobsDag := createRandJobDAG()

	job := jobsDag.Vectors[11]
	for idx, replica := range job.replicas {
		replica.minValue = math.MaxFloat64
		for _, node := range nodes {
			if node.cpu-node.allocatedCpu < job.replicaCpu || node.mem-node.allocatedMem < job.replicaMem {
				continue
			}

			var time float64
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

		replica.node.allocatedCpu += job.replicaCpu
		replica.node.allocatedMem += job.replicaMem
	}
	for idx, replica := range job.replicas {
		fmt.Println("Job", job.ID, ",replica", idx, ",nodeID:", replica.node.ID,
			",minTime:", replica.minTime, ",min DR:", replica.minDr, ",minValue:", replica.minValue)
	}

}

func createRandJobDAG() *JobsDAG {
	jobsDAG := JobsDAG{
		Vectors: []*Job{},
	}
	for i := 0; i < 12; i++ {
		job := &Job{
			ID:         i,
			replicaNum: rand.Int()%7 + 1,
			replicaCpu: (rand.Int()%4 + 1) * 2 * 1000,
			replicaMem: (rand.Int()%4 + 1) * 2 * 1024,
			actionNum:  rand.Int()%7 + 1,
			children:   []*Job{},
		}
		createRandReplica(job)
		job.predictTime()
		jobsDAG.Vectors = append(jobsDAG.Vectors, job)
	}

	vectors := jobsDAG.Vectors
	jobsDAG.Vectors[0].children = []*Job{vectors[1], vectors[2], vectors[3]}
	jobsDAG.Vectors[1].children = []*Job{vectors[4]}
	jobsDAG.Vectors[2].children = []*Job{vectors[5]}
	jobsDAG.Vectors[3].children = []*Job{vectors[5]}
	jobsDAG.Vectors[4].children = []*Job{vectors[6]}
	jobsDAG.Vectors[5].children = []*Job{vectors[6], vectors[7], vectors[8]}
	jobsDAG.Vectors[6].children = []*Job{vectors[9]}
	jobsDAG.Vectors[7].children = []*Job{vectors[9], vectors[10]}
	jobsDAG.Vectors[8].children = []*Job{vectors[10]}
	jobsDAG.Vectors[9].children = []*Job{vectors[11]}
	jobsDAG.Vectors[10].children = []*Job{vectors[11]}
	jobsDAG.Vectors[11].children = []*Job{}
	for i, job := range vectors {
		Log(fmt.Sprintf("job:%d", i), job)
	}
	// create parent for each vectors by using children
	jobsDAG = *ChildToParent(&jobsDAG)

	return &jobsDAG
}

// func ChildToParent(jobsDAG *JobsDAG) *JobsDAG {
// 	vectors := jobsDAG.Vectors
// 	for _, parent := range vectors {
// 		for _, child := range parent.children {
// 			child.parent = append(child.parent, parent)
// 		}
// 	}
// 	return jobsDAG
// }

// func createRandReplica(j *Job) {
// 	for i := 0; i < j.replicaNum; i++ {
// 		j.createReplica()
// 	}
// 	for i := 0; i < j.actionNum; i++ {
// 		randExecutionTime := rand.Float64() * 1000
// 		for _, r := range j.replicas {
// 			a := r.createAction(randExecutionTime)
// 			for _, r := range j.replicas {
// 				a.datasize[r] = rand.Float64() * 1000
// 			}
// 		}
// 	}
// 	for _, r := range j.replicas {
// 		for _, child := range j.children {
// 			r.finalDataSize[child] = rand.Float64() * 1000
// 		}
// 		// Log(fmt.Sprintf("replica:%d", i), r)
// 	}
// }

func Log(describe string, a any) {
	fmt.Println(describe+":", a)
}

func createRandNode() ([]*node, *bandwidth) {
	nodeCount := 6
	nodes := []*node{}
	bw := &bandwidth{
		values: map[*node]map[*node]float64{},
	}
	for i := 0; i < nodeCount; i++ {
		n := &node{
			ID:            i,
			cpu:           (rand.Int()%4 + 1) * 8 * 1000,
			mem:           (rand.Int()%4 + 1) * 8 * 1024,
			allocatedCpu:  0,
			allocatedMem:  0,
			executionRate: rand.Float64() + 1,
		}
		nodes = append(nodes, n)
	}

	for i := 0; i < nodeCount; i++ {
		from := nodes[i]
		if _, exist := bw.values[from]; !exist {
			bw.values[from] = map[*node]float64{}
		}
		for j := i; j < nodeCount; j++ {
			to := nodes[j]
			if _, exist := bw.values[to]; !exist {
				bw.values[to] = map[*node]float64{}
			}
			var randBandwidth float64
			if i == j {
				randBandwidth = 0
			} else {
				randBandwidth = rand.Float64()*100 + 1
			}

			bw.values[from][to] = randBandwidth
			bw.values[to][from] = randBandwidth
		}
	}

	for idx, n := range nodes {
		Log(fmt.Sprintf("node%d", idx), n)
		Log(fmt.Sprintf("bandwidth%d", idx), bw.values[n])
	}

	return nodes, bw

}
