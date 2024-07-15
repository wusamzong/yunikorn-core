package objects

import (
	"fmt"
	"math/rand"
	// "sort"
	"testing"
)

func TestSimulateMacro(t *testing.T) {
	var randomSeed int64 = 100
	rand.Seed(randomSeed)
	nodes, bw := createRandNode()
	jobsDag := createStaticJobDAG()
	jobsWithOnlyReplica(jobsDag.Vectors)
	a := createMacro(jobsDag.Vectors, nodes, bw)
	metric := a.simulate()
	fmt.Println(metric.makespan, metric.SLR)
}

func TestMacroWithConfig(t *testing.T){


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

	
	for i := 0; i < 1; i++ {
		rand.Seed(int64(i))
		nodes, bw := createRandNodeByConfig(config)
		jobsDag := generateRandomDAGWithConfig(config)
		// jobsWithOnlyReplica(jobsDag.Vectors)
		a := createMacro(jobsDag.Vectors, nodes, bw)
		metric := a.simulate()
		fmt.Println(metric.makespan)
	}
}
