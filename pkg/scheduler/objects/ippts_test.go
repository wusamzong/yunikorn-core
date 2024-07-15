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
	var randomSeed int64 = 100
	rand.Seed(randomSeed)
	nodes, bw := createRandNode()
	jobsDag := createStaticJobDAG()
	jobsWithOnlyReplica(jobsDag.Vectors)
	p := createIPPTS(jobsDag.Vectors, nodes, bw)
	metric :=p.simulate()
	fmt.Println(metric.makespan, metric.SLR)
}

func TestIPPTS(t *testing.T){
	rand.Seed(2)
	config := comparisonConfig{
		podCount:           100,
		alpha:              0.2,
		replicaNum:         4,
		actionNum:          6,
		nodeCount:          4,
		ccr:                5.0,
		speedHeterogeneity: 1.0,
		tcr: 				5.0,
	}

	config = settingConfig(config)

	nodes, bw := createRandNodeByConfig(config)
	jobsDag := generateRandomDAGWithConfig(config)
	// jobsWithOnlyReplica(jobsDag.Vectors)
	for i := 0; i < 1; i++ {
		jobsWithOnlyReplica(jobsDag.Vectors)
		p := createIPPTS(jobsDag.Vectors, nodes, bw)
		metric :=p.simulate()
		fmt.Println(metric.makespan, metric.SLR)
	}
}
