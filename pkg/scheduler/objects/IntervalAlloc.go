package objects

import (
	"math"
	"fmt"
	"os"
)

type intervalAllocManager struct{
	current float64
	allocations []*intervalAlloc
}

type intervalAlloc struct{
	replica *replica
	start float64
	end float64
	node *node
	allocatedCpu int
	allocatedMem int
}

func (am* intervalAllocManager) allocate(request interface{}){
	
	if job, ok := request.(*Job); ok{
		// fmt.Println(" => Job ID:", job.ID, "is scheduled.")
		for _, replica:= range job.replicas{
			allocation := &intervalAlloc{
				replica: replica,
				start: am.current,
				end: am.current+job.makespan,
				node: replica.node,
				allocatedCpu: job.replicaCpu,
				allocatedMem: job.replicaMem,
			}
			replica.node.allocatedCpu += job.replicaCpu
			replica.node.allocatedMem += job.replicaMem
			am.allocations = append(am.allocations, allocation)
		}
		// fmt.Printf("=> allocation Number: %d, allocate Number: %d\n",len(am.allocations), len(job.replicas))
	}else if replica, ok := request.(*replica); ok{
		job:=replica.job
		allocation := &intervalAlloc{
			replica: replica,
			start: am.current,
			end: am.current+job.predictExecutionTime,
			node: replica.node,
			allocatedCpu: job.replicaCpu,
			allocatedMem: job.replicaMem,
		}
		replica.node.allocatedCpu += job.replicaCpu
		replica.node.allocatedMem += job.replicaMem
		am.allocations = append(am.allocations, allocation)
	}else{
		fmt.Println("The type ",request," isn't exist")
		os.Exit(2)
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
		alloc.replica.finish=true
		alloc.replica.job.finish++
		am.release(alloc) 
	}
	// fmt.Print("release ")
	// for _, ra := range releaseAlloc{
	// 	fmt.Printf("(j-%d r-%d) ",ra.replica.job.ID,ra.replica.ID)
	// }
	// fmt.Println()
	// fmt.Printf("=> allocation Number: %d, release Number: %d\n",len(am.allocations), len(releaseAlloc))
	return releaseAlloc
}

func (am* intervalAllocManager) release(removeAlloc *intervalAlloc){
	for idx, alloc := range am.allocations{
		if alloc == removeAlloc{
			// alloc.node.allocatedCpu -= alloc.allocatedCpu
			// alloc.node.allocatedMem -= alloc.allocatedMem
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