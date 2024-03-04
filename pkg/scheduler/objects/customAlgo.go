package objects

import (
	"container/heap"
	"fmt"
	"math"
)

type customAlgo struct {
	nodes []*node
	jobs  []*Job
	bw    *bandwidth
}

func createCustomAlgo(jobs []*Job, nodes []*node, bw *bandwidth) *customAlgo {
	return &customAlgo{
		nodes: nodes,
		jobs:  jobs,
		bw:    bw,
	}
}

func (c *customAlgo) simulate() (float64, float64) {

	allocManager := intervalAllocManager{
		totalCapacity: []float64{},
		totalAllocte:  []float64{0.0, 0.0},
		totalUsage:    []float64{0.0, 0.0},
		availableTime: map[*node]float64{},
		current:       0,
	}

	transManager := intervalTransmissionManager{
		doneTransmission:   map[*replica]map[*replica]bool{},
		customTransmission: map[*replica]map[*replica]float64{},
		transmission:       []*intervalTransmission{},
	}

	allocManager.initCapacity(c.nodes)
	allocManager.initAvailableTime(c.nodes)

	transManager.initTransmission(c.jobs)

	aveExecRate, aveBw := calcAve(c.nodes, c.bw)
	availJobsHeap := &JobHeap{
		averageBandwidth:     aveBw,
		averageExecutionRate: aveExecRate,
	}
	// fmt.Println(aveBw)
	// fmt.Println(aveExecRate)
	// storage the job has been tried but fail, fifo


	scheduledJob := map[*Job]bool{}
	heap.Init(availJobsHeap)

	// If the job is end of DAG then pop it into available Jobs Heap
	for _, job := range c.jobs {
		if len(job.parent) == 0 {
			heap.Push(availJobsHeap, job)
		}
	}
	

	// for _, node := range c.nodes {
	// 	fmt.Printf("nodeId:%d, capacity:{%d, %d}, allocated:{%d, %d}\n", node.ID, node.cpu, node.mem, node.allocatedCpu, node.allocatedMem)
	// }
	for availJobsHeap.Len() > 0 {
		var job *Job
		reserveQueue := []*Job{}
		for availJobsHeap.Len() > 0 {
			job = availJobsHeap.Pop().(*Job)
			if _, exist := scheduledJob[job]; exist {
				continue
			}
			done := job.decideNode(c.nodes, c.bw)
			allParentDone := job.allParentDone()
			// isAllParentTransmitted := transManager.transmittedStatus(job)
			if done && allParentDone {
				fmt.Println("JobID:", job.ID, " is allocated")
				// for _, replica := range job.replicas{
				// 	fmt.Println(replica.minValue)
				// }
				transManager.addJobInterval(job, allocManager.current, c.bw)
				allocManager.allocate(job, &transManager)
				// for _, node := range c.nodes {
				// 	fmt.Printf("nodeId:%d, capacity:{%d, %d}, allocated:{%d, %d}\n", node.ID, node.cpu, node.mem, node.allocatedCpu, node.allocatedMem)
				// }
				scheduledJob[job] = true
				for _, child := range job.children {
					if child.allParentScheduled(scheduledJob) && !scheduledJob[child] {
						heap.Push(availJobsHeap, child)
					}
				}
			} else {
				reserveQueue = append(reserveQueue, job)
			}

		}
		for _, job := range reserveQueue {
			availJobsHeap.Push(job)
		}

		nextInterval(&transManager, &allocManager)
		_ = allocManager.releaseResource()

		if allocManager.current == math.MaxFloat64 {
			return 0.0, 0.0
		}

	}

	// fmt.Printf("makespan = %.2f\n", allocManager.getMakespan())
	return allocManager.getResult()
}


		// for len(reserveQueue) > 0 {
		// 	nextInterval(&transManager, &allocManager)
		// 	// fmt.Printf("updateCurrent time: %.0f\n", allocManager.current)
		// 	_ = allocManager.releaseResource()
		// 	// transManager.addInterval(releaseAlloc, allocManager.current, c.bw)
		// 	// fmt.Println("Lens of transmission",len(transManager.transmission))
		// 	// transManager.releaseInterval(allocManager.current)
		// 	// for _, node := range c.nodes {
		// 	// 	fmt.Printf("nodeId:%d, capacity:{%d, %d}, allocated:{%d, %d}\n", node.ID, node.cpu, node.mem, node.allocatedCpu, node.allocatedMem)
		// 	// }
		// 	for i := 0; i < len(reserveQueue); i++ {
		// 		job = reserveQueue[0]
		// 		reserveQueue = reserveQueue[1:]
		// 		if _, exist := scheduledJob[job]; exist {
		// 			continue
		// 		}
		// 		done := job.decideNode(c.nodes, c.bw)
		// 		allParentDone := job.allParentDone()
		// 		// isAllParentTransmitted := transManager.transmittedStatus(job)
		// 		// fmt.Println(isAllParentTransmitted)
		// 		if done && allParentDone  {

		// 			fmt.Println("JobID:", job.ID, " is allocated")
		// 			transManager.addJobInterval(job, allocManager.current, c.bw)
		// 			allocManager.allocate(job, &transManager)
		// 			// for _, node := range c.nodes {
		// 			// 	fmt.Printf("nodeId:%d, capacity:{%d, %d}, allocated:{%d, %d}\n", node.ID, node.cpu, node.mem, node.allocatedCpu, node.allocatedMem)
		// 			// }
		// 			scheduledJob[job] = true
		// 			for _, child := range job.children {

		// 				if child.allParentScheduled(scheduledJob) && !scheduledJob[child] {
		// 					heap.Push(availJobsHeap, child)
		// 				}
		// 			}
		// 		} else {
		// 			reserveQueue = append(reserveQueue, job)
		// 			if len(allocManager.allocations) == 0 && len(transManager.transmission) == 0 {
		// 				// fmt.Println("There is no enough space for job", job.ID)
		// 				// fmt.Println("Job", job)
		// 				// for _, node := range c.nodes {
		// 				// 	fmt.Printf("nodeId:%d, capacity:{%d, %d}, allocated:{%d, %d}\n", node.ID, node.cpu, node.mem, node.allocatedCpu, node.allocatedMem)
		// 				// }
		// 				return 0.0, 0.0
		// 			}
		// 			if allocManager.current == math.MaxFloat64 {
		// 				return 0.0, 0.0
		// 			}
		// 		}
		// 	}
		// }