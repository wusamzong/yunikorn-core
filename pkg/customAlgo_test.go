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
		c := createCustomAlgo(jobsDag.Vectors, nodes, bw)
		metric:=c.simulate()
		fmt.Println(metric.makespan, metric.SLR)
	}
}


func TestInferenceEffect(t *testing.T){
	w, file := createWriter()
	defer file.Close()
	defer w.Flush()

	config := comparisonConfig{
		podCount:           300,
		alpha:              0.2,
		replicaNum:         4,
		actionNum:          6,
		nodeCount:          16,
		ccr:                1.0,
		speedHeterogeneity: 1.0,
	}

	config = settingConfig(config)

	
	for i := 0; i < 30; i++ {
		rand.Seed(int64(i))
		nodes, bw := createRandNodeByConfig(config)
		jobsDag := generateRandomDAGWithConfig(config)
		c := createCustomAlgo(jobsDag.Vectors, nodes, bw)
		metric:=c.simulate()

		w.Write([]string{fmt.Sprint(metric.makespan)})
		w.Flush()
	}
}
