package objects

import (
	// "fmt"
	"math/rand"
	// "sort"
	"testing"
)

func TestCreateMPEFT(t *testing.T) {
	var randomSeed int64 = 100
	rand.Seed(randomSeed)

	nodes, bw := createRandNode()
	jobsDag := createStaticJobDAG()

	createMPEFT(jobsDag.Vectors, nodes, bw)
}

func TestCalcOffSpringSet(t *testing.T) {
	jobsDag := createStaticJobDAG()
	// jobsDag := generateRandomDAG()
	calcOffSpringSet(jobsDag.Vectors)
}

func TestCalcDCT(t *testing.T) {
	nodes, bw := createRandNode()
	// jobsDag := createStaticJobDAG()
	jobsDag := generateRandomDAG()
	m := createMPEFT(jobsDag.Vectors, nodes, bw)
	m.calcDCT()
}

func TestCalcRankAP(t *testing.T) {
	nodes, bw := createRandNode()
	jobsDag := createStaticJobDAG()
	// jobsDag := generateRandomDAG()
	m := createMPEFT(jobsDag.Vectors, nodes, bw)
	m.calcDCT()
	m.calcRankAP()
}

func TestCalcTime(t *testing.T) {
	nodes, bw := createRandNode()
	jobsDag := createStaticJobDAG()
	// jobsDag := generateRandomDAG()
	m := createMPEFT(jobsDag.Vectors, nodes, bw)
	m.calcTime()
}

func TestCalcOCTandCPS(t *testing.T) {
	nodes, bw := createRandNode()
	// jobsDag := createStaticJobDAG()
	jobsDag := generateRandomDAG()
	m := createMPEFT(jobsDag.Vectors, nodes, bw)
	m.calcTime()
	m.calcDCT()
	m.calcRankAP()
	m.calcOCTandCPS()
}

func TestCalcEFT(t *testing.T) {
	nodes, bw := createRandNode()
	jobsDag := createStaticJobDAG()
	// jobsDag := generateRandomDAG()
	m := createMPEFT(jobsDag.Vectors, nodes, bw)
	m.calcTime()
	m.calcEFT()
}

func TestCalcMEFT(t *testing.T) {
	nodes, bw := createRandNode()
	jobsDag := createStaticJobDAG()
	m := createMPEFT(jobsDag.Vectors, nodes, bw)
	m.calcTime()
	m.calcEFT()
	m.calcDCT()
	m.calcRankAP()
	m.calcOCTandCPS()
	m.calcK()
	m.calcMEFT()
}

func TestSimulateMPEFT(t *testing.T) {
	var randomSeed int64 = 100
	rand.Seed(randomSeed)
	nodes, bw := createRandNode()
	jobsDag := createStaticJobDAG()
	m := createMPEFT(jobsDag.Vectors, nodes, bw)
	m.simulate()
	// m.allocation()
	// allocManager := intervalAllocManager{current: 0}
	// sort.Slice(m.jobs, func(i, j int) bool {
	// 	return m.rankAP[m.jobs[i]] < m.rankAP[m.jobs[j]]
	// })
	// queue := []*replica{}
	// scheduledReplica := map[*replica]bool{}
	// for _, j := range m.jobs {
	// 	j.predictTime(0.0)
	// 	if len(j.parent) == 0 {
	// 		queue = append(queue, j.replicas...)
	// 	}
	// }
	// for len(queue) > 0 {
	// 	replica := queue[0]
	// 	done := m.tryNode(replica)
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
