package objects

import (
	"fmt"
	"math"
	"math/rand"
	"testing"
)

func TestGenerateRandomDAG(t *testing.T) {
	generateRandomDAG()
}

func TestGenerateRandomDAGWithConfig(t *testing.T) {
	config := comparisonConfig{
		podCount: 100,
		times: 10,
	}

	var i int64
	alpha:=0.2
	density:=0.4
	replicaCount:=4
	for i = 0; i < config.times; i++ {
		rand.Seed(i)
		width := int(math.Sqrt(float64(config.podCount) / ((1.0 - alpha) / alpha)))
		config.width = width
		config.percent = int(density * 10)
		config.replicaNum = replicaCount
		config.replicaCPURange = rand.Intn(8) + 1 // (rand.Int()%config.range + 1) * 500,
		config.replicaMemRange = rand.Intn(8) + 1
		config.actionNum = 10

		// jobsDag := simulateGenerateRandomDAGWithConfig(config)
		jobsDag := generateRandomDAGWithConfig(config)
		fmt.Printf("%d,%d\n", jobsDag.replicasCount, i)
	}
}
