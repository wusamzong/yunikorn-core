package objects

import (
	"container/heap"
	"fmt"
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
	fmt.Println("Custom Algorithm")
	simulator := createSimulator(c.nodes, c.bw)

	aveExecRate, aveBw := calcAve(c.nodes, c.bw)
	availJobsHeap := &JobHeap{
		averageBandwidth:     aveBw,
		averageExecutionRate: aveExecRate,
	}
	scheduledJob := map[*Job]bool{}
	enqueueJob := map[*Job]bool{}
	heap.Init(availJobsHeap)

	// If the job is end of DAG then pop it into available Jobs Heap
	for _, job := range c.jobs {
		scheduledJob[job]=false
		enqueueJob[job]=false
		if len(job.parent) == 0 {
			job.priority(aveBw, aveExecRate)
			heap.Push(availJobsHeap, job)
			enqueueJob[job]=true
		}
	}

	for availJobsHeap.Len() > 0 {
		var job *Job
		reserveQueue := []*Job{}
		// fmt.Println()
		// fmt.Print("Queue: ")
		// for i:=0;i<availJobsHeap.Len();i++{
		// 	fmt.Print((*availJobsHeap).jobs[i].ID, " ")
		// }
		// fmt.Println()

		for availJobsHeap.Len() > 0 {
			

			job = heap.Pop(availJobsHeap).(*Job)

			if exist := scheduledJob[job]; exist {
				continue
			}

			done := job.decideNode(c.nodes, c.bw)
			if done && simulator.isParentJobFinish(job) {
				// fmt.Println("JobID:", job.ID, " is allocated, Priority:", job.pathPriority)
				simulator.addPendJob(job)

				scheduledJob[job] = true
				for _, child := range job.children {
					if !scheduledJob[child] && !enqueueJob[child]{
						heap.Push(availJobsHeap, child)
						enqueueJob[child]=true
					}
				}
			} else {
				// if !done{
				// 	fmt.Println("no enough resource!")
				// }
				reserveQueue = append(reserveQueue, job)
			}
		}
		
		for _, job := range reserveQueue {
			heap.Push(availJobsHeap, job)
		}

		finishedLength:=len(simulator.finished)
		for len(simulator.allocations)+len(simulator.pending)>0{
			simulator.update()
			// printAllDetailStatus(simulator)
			if finishedLength < len(simulator.finished){
				break
			}
		}
		
	}
	for len(simulator.allocations)+len(simulator.pending)>0{
		simulator.update()
		// printAllDetailStatus(simulator)
		if len(simulator.pending) ==0 && len(simulator.allocations)==0{
			break
		}
	}

	simulator.printFinishedJob()
	makespan:= simulator.current
	SLR:=calSLR(c.nodes, getCriticalPath(c.jobs), makespan)

	return makespan, SLR
}

