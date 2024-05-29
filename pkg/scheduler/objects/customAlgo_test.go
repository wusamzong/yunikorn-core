package objects

import (
	"fmt"
	// "os"
	"testing"
	"math/rand"
)

func TestSimulateCustom(t *testing.T) {
	var randomSeed int64 = 101
	
	rand.Seed(randomSeed)
	nodes, bw := createSampleNode()
	jobsDag := createSampleJobDAG()

	c := createCustomAlgo(jobsDag.Vectors, nodes, bw)
	metric:=c.simulate()
	fmt.Println(metric.makespan, metric.SLR)
}

func TestCustom(t *testing.T){
	rand.Seed(2)
	config := comparisonConfig{
		podCount:           100,
		alpha:              0.2,
		replicaNum:         4,
		nodeCount:          8,
		ccr:                20.0,
		speedHeterogeneity: 1.0,
	}
	config = settingConfig(config)

	nodes, bw := createRandNodeByConfig(config)
	jobsDag := generateRandomDAGWithConfig(config)
	for i := 0; i < 1; i++ {
		c := createCustomAlgo(jobsDag.Vectors, nodes, bw)
		metric:=c.simulate()
		fmt.Println(metric.makespan, metric.SLR)
	}

}
