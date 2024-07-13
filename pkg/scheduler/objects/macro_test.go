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
