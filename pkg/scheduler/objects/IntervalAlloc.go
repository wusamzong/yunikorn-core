package objects

import (
	"fmt"
	"math"
	"os"
)

type intervalAllocManager struct {
	totalCapacity []float64
	totalAllocte  []float64
	totalUsage    []float64
	availableTime map[*node]float64
	current       float64
	allocations   []*intervalAlloc
}

type intervalAlloc struct {
	replica      *replica
	start        float64
	end          float64
	node         *node
	allocatedCpu int
	allocatedMem int
}

func (am *intervalAllocManager) initCapacity(nodes []*node) {
	totalCPU:=0
	totalMem:=0
	for _, n:=range nodes{
		totalCPU+=n.cpu
		totalMem+=n.mem
	}
	am.totalCapacity=append(am.totalCapacity, float64(totalCPU))
	am.totalCapacity=append(am.totalCapacity, float64(totalMem))
}

func (am *intervalAllocManager) initAvailableTime(nodes []*node){
	for _, n := range nodes{
		am.availableTime[n]=0
	}
}

func (am *intervalAllocManager) allocate(request interface{}) {

	if job, ok := request.(*Job); ok {
		// fmt.Println(" => Job ID:", job.ID, "is scheduled.")
		for _, replica := range job.replicas {
			allocation := &intervalAlloc{
				replica:      replica,
				start:        am.current,
				end:          am.current + job.makespan,
				node:         replica.node,
				allocatedCpu: job.replicaCpu,
				allocatedMem: job.replicaMem,
			}
			replica.node.allocatedCpu += job.replicaCpu
			replica.node.allocatedMem += job.replicaMem
			am.totalAllocte[0] += float64(job.replicaCpu)*job.makespan
			am.totalAllocte[1] += float64(job.replicaMem)*job.makespan
			am.allocations = append(am.allocations, allocation)
		}
		// fmt.Printf("=> allocation Number: %d, allocate Number: %d\n",len(am.allocations), len(job.replicas))
	} else if replica, ok := request.(*replica); ok {
		job := replica.job
		allocation := &intervalAlloc{
			replica:      replica,
			start:        am.current,
			end:          am.current + job.predictExecutionTime,
			node:         replica.node,
			allocatedCpu: job.replicaCpu,
			allocatedMem: job.replicaMem,
		}
		replica.node.allocatedCpu += job.replicaCpu
		replica.node.allocatedMem += job.replicaMem
		am.totalAllocte[0] += float64(job.replicaCpu)*job.predictExecutionTime
		am.totalAllocte[1] += float64(job.replicaMem)*job.predictExecutionTime
		am.allocations = append(am.allocations, allocation)
	} else {
		fmt.Println("The type ", request, " isn't exist")
		os.Exit(2)
	}

}


func (am *intervalAllocManager) releaseResource() []*intervalAlloc {
	releaseAlloc := []*intervalAlloc{}
	for _, alloc := range am.allocations {
		if am.current >= alloc.end {
			// node := alloc.node
			// fmt.Printf("nodeId:%d, capacity:{%d, %d}, allocated:{%d, %d}\n", node.ID, node.cpu, node.mem, node.allocatedCpu, node.allocatedMem)
			alloc.node.allocatedCpu -= alloc.allocatedCpu
			alloc.node.allocatedMem -= alloc.allocatedMem
			// fmt.Printf("nodeId:%d, capacity:{%d, %d}, allocated:{%d, %d}\n", node.ID, node.cpu, node.mem, node.allocatedCpu, node.allocatedMem)
			releaseAlloc = append(releaseAlloc, alloc)
		}
	}
	for _, alloc := range releaseAlloc {
		alloc.replica.finish = true
		alloc.replica.job.finish++
		// fmt.Printf("release (j-%d, r-%d) \n",alloc.replica.job.ID, alloc.replica.ID)
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

func (am *intervalAllocManager) release(removeAlloc *intervalAlloc) {
	for idx, alloc := range am.allocations {
		if alloc == removeAlloc {
			// alloc.node.allocatedCpu -= alloc.allocatedCpu
			// alloc.node.allocatedMem -= alloc.allocatedMem
			am.allocations = append(am.allocations[:idx], am.allocations[idx+1:]...)
		}
	}
}

func (am *intervalAllocManager) nextInterval() {
	var minEndTime float64 = math.MaxFloat64
	for _, alloc := range am.allocations {
		if minEndTime > alloc.end {
			minEndTime = alloc.end
		}
	}
	am.current = minEndTime
}

func (am *intervalAllocManager) getMakespan() float64 {
	var maxEndTime float64 = 0
	for _, alloc := range am.allocations {
		if maxEndTime < alloc.end {
			maxEndTime = alloc.end
		}
	}
	return maxEndTime
}

func (am *intervalAllocManager) getResult() (float64, float64) {
	var maxEndTime float64 = 0
	for _, alloc := range am.allocations {
		if maxEndTime < alloc.end {
			maxEndTime = alloc.end
		}
	}
	if maxEndTime==0.0{
		maxEndTime=am.current
	}

	totalUsage:=((am.totalAllocte[0]/(am.totalCapacity[0]*maxEndTime))+(am.totalAllocte[1]/(am.totalCapacity[1]*maxEndTime)))/2
	return maxEndTime, totalUsage
}
