package objects

import (
	"testing"
	"math/rand"
)

func TestSimulateCustom(t *testing.T) {
	var randomSeed int64 = 100
	rand.Seed(randomSeed)
	nodes, bw := createRandNode()
	jobsDag := createStaticJobDAG()
	c := createCustomAlgo(jobsDag.Vectors, nodes, bw)
	c.simulate()
}
