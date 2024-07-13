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
