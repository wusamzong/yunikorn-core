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
	nodes, bw := createRandNode()
	jobsDag := createStaticJobDAG()

	c := createCustomAlgo(jobsDag.Vectors, nodes, bw)
	makespan, resourceUsage:=c.simulate()
	fmt.Println(makespan, resourceUsage)
}
