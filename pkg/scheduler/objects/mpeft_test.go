package objects

import (
	"fmt"
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

// func TestCalcOffSpringSet(t *testing.T) {
// 	jobsDag := createStaticJobDAG()
// 	// jobsDag := generateRandomDAG()
// 	calcOffSpringSet(jobsDag.Vectors)
// }

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
	makespan, resourceUsage := m.simulate()
	fmt.Println(makespan, resourceUsage)
}
