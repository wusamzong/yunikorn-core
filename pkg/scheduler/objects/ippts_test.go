package objects

import (
	"fmt"
	"math/rand"
	// "sort"
	"testing"
)

func TestCalcPCM(t *testing.T) {
	nodes, bw := createRandNode()
	jobsDag := createStaticJobDAG()
	// jobsDag := generateRandomDAG()
	p := createIPPTS(jobsDag.Vectors, nodes, bw)
	p.calcTime()
	p.calcPCM()
}

func TestAllocation(t *testing.T) {
	nodes, bw := createRandNode()
	// jobsDag := createStaticJobDAG()
	jobsDag := generateRandomDAG()
	p := createIPPTS(jobsDag.Vectors, nodes, bw)
	p.allocation()
}

func TestSimulateIPPTS(t *testing.T) {
	var randomSeed int64 = 101
	rand.Seed(randomSeed)
	nodes, bw := createRandNode()
	jobsDag := createStaticJobDAG()
	p := createIPPTS(jobsDag.Vectors, nodes, bw)
	makespan, resourceUsage:=p.simulate()
	fmt.Println(makespan, resourceUsage)
	// p.allocation()
	// allocManager := intervalAllocManager{current: 0}
	// sort.Slice(p.jobs, func(i, j int) bool {
	// 	return p.Prank[p.jobs[i]] < p.Prank[p.jobs[j]]
	// })
	// queue := []*replica{}
	// scheduledReplica := map[*replica]bool{}
	// for _, j := range p.jobs {
	// 	j.predictTime(0.0)
	// 	if len(j.parent) == 0 {
	// 		queue = append(queue, j.replicas...)
	// 	}
	// }
	// for len(queue) > 0 {
	// 	replica := queue[0]
	// 	done := p.tryNode(replica)
	// 	allParentDone := replica.job.allParentDone()
	// 	if done && allParentDone {
	// 		fmt.Println("Replica ID:", replica.job.ID, ",Select Node ID:", replica.node.ID)
	// 		scheduledReplica[replica] = true
	// 		queue = queue[1:]
	// 		allocManager.allocate(replica)
	// 		// is child need to been consider??
	// 		for _, childReplica := range replica.children {
	// 			_, exist := scheduledReplica[childReplica]
	// 			if childReplica.allParentScheduled(scheduledReplica) && !exist {
	// 				queue = append(queue, childReplica)
	// 			}
	// 		}
	// 	} else {
	// 		allocManager.nextInterval()
	// 		fmt.Printf("updateCurrent time: %.2f\n", allocManager.current)
	// 		_ = allocManager.releaseResource()
	// 	}
	// }
	// fmt.Printf("makespan = %.2f\n", allocManager.getMakespan())
}
