package objects

import (
	"math"
	// "fmt"
)

type intervalAllocManager struct{
	current float64
	allocations []*intervalAlloc
}

type intervalAlloc struct{
	start float64
	end float64
	node *node
	allocatedCpu int
	allocatedMem int
}

func (am* intervalAllocManager) allocate(job *Job){
	for _, replica:= range job.replicas{
		allocation := &intervalAlloc{
			start: am.current,
			end: am.current+job.makespan,
			node: replica.node,
			allocatedCpu: job.replicaCpu,
			allocatedMem: job.replicaMem,
		}
		am.allocations = append(am.allocations, allocation)
	}
}

func (am* intervalAllocManager) releaseResource() []*intervalAlloc{
	releaseAlloc := []*intervalAlloc{}
	for _, alloc := range am.allocations{
		if am.current >= alloc.end{
			// node := alloc.node
			// fmt.Printf("nodeId:%d, capacity:{%d, %d}, allocated:{%d, %d}\n", node.ID, node.cpu, node.mem, node.allocatedCpu, node.allocatedMem)
			alloc.node.allocatedCpu -= alloc.allocatedCpu
			alloc.node.allocatedMem -= alloc.allocatedMem
			// fmt.Printf("nodeId:%d, capacity:{%d, %d}, allocated:{%d, %d}\n", node.ID, node.cpu, node.mem, node.allocatedCpu, node.allocatedMem)
			releaseAlloc = append(releaseAlloc, alloc)
		}
	}
	for _, alloc := range releaseAlloc{
		am.release(alloc) 
	}
	return releaseAlloc
}

func (am* intervalAllocManager) release(removeAlloc *intervalAlloc){
	for idx, alloc := range am.allocations{
		if alloc == removeAlloc{
			am.allocations = append(am.allocations[:idx], am.allocations[idx+1:]...)
		}
	}
}

func (am* intervalAllocManager) nextInterval(){
	var minEndTime float64=math.MaxFloat64
	for _, alloc := range am.allocations{
		if minEndTime > alloc.end{
			minEndTime = alloc.end
		}
	}
	am.current = minEndTime
}

func (am* intervalAllocManager) getMakespan() float64{
	var maxEndTime float64=0
	for _, alloc := range am.allocations{
		if maxEndTime < alloc.end{
			maxEndTime = alloc.end
		}
	}
	return maxEndTime
}