package objects

import (
	"fmt"
	"time"
	"math"
	"math/rand"
	// "os"
	// "sort"
)

func randomBasedOnHete(mid float64, hete float64)int{
	ran := mid*hete
	startFrom:=int(mid-ran)
	result := startFrom+rand.Int()%(int(ran*2)+1)
	if result<=1{
		return 1
	}
	return result
}

func doWithTimeout(seed int64, podCount int, alpha float64, density float64, replicaCount int, nodeCount int, CCR float64, RRC float64, speedHete float64, resouHete float64) []string {
    // Create a channel to signal completion of the function and return result
    done := make(chan []string, 1)

    // Start the function in a goroutine
    go func() {
        // Call your function here with parameters and capture the result
        result := testWithCase(seed, podCount, alpha, density, replicaCount, nodeCount, CCR, RRC, speedHete, resouHete)
        done <- result
    }()

    // Use select to wait for either the function to complete or the timeout
    select {
    case result := <-done:
        // Function completed
        fmt.Println("Function completed successfully")
        return result
    case <-time.After(90 * time.Second): // Set your timeout duration here
        // Function timed out
        fmt.Println("Function timed out")
        return nil
    }
}

// Replace this with your actual function
func performOperation() {
    // Simulating a task that might take time
    time.Sleep(1 * time.Second)
    fmt.Println("Operation performed")
}

func testWithCase(seed int64, podCount int, alpha float64, density float64, replicaCount int, nodeCount int, CCR float64, RRC float64, speedHete float64, resouHete float64) []string {
	config := comparisonConfig{
		podCount: podCount,
	}

	width := int(math.Sqrt(float64(podCount) / ((1.0 - alpha) / alpha)))
	config.width = width
	config.percent = int(density * 10)
	config.replicaNum = replicaCount
	config.replicaCPURange = rand.Intn(8) + 1 // (rand.Int()%config.range + 1) * 500,
	config.replicaMemRange = rand.Intn(8) + 1
	
	config.nodeCount = nodeCount
	config.ccr=CCR
	config.rrc=RRC
	config.speedHeterogeneity=speedHete
	config.resourceHeterogeneity=resouHete

	averageNodeResource := float64(podCount/nodeCount)*RRC
	config.averageNodeResource = averageNodeResource
	config.nodeCPURange = config.replicaCPURange*replicaCount/2 // (rand.Int()%config.nodeCPURange + 1) * 1000
	config.nodeMemRange = config.replicaMemRange*replicaCount/2
	
	config.actionNum = 10

	current:=[]string{}
	for algoCount := 0; algoCount < 3; algoCount++ {

		rand.Seed(seed)
		nodes, bw := createRandNodeByConfig(config)
		jobsDag := generateRandomDAGWithConfig(config)
		if jobsDag.replicasCount == 0 {
			continue
		}

		if algoCount == 0 {
			m := createMPEFT(jobsDag.Vectors, nodes, bw)
			// current = append(current, fmt.Sprintf("%d", jobsDag.replicasCount))
			makespan, resourceUsage := m.simulate()
			current = append(current, fmt.Sprintf("%.0f", makespan))
			current = append(current, fmt.Sprintf("%.3f", resourceUsage))
		} else if algoCount == 1 {
			p := createIPPTS(jobsDag.Vectors, nodes, bw)
			makespan, resourceUsage := p.simulate()
			current = append(current, fmt.Sprintf("%.0f", makespan))
			current = append(current, fmt.Sprintf("%.3f", resourceUsage))
		} else {
			c := createCustomAlgo(jobsDag.Vectors, nodes, bw)
			makespan, resourceUsage := c.simulate()
			current = append(current, fmt.Sprintf("%.0f", makespan))
			current = append(current, fmt.Sprintf("%.3f", resourceUsage))
		}
	}

	return current
}



func createRandNodeByConfig(config comparisonConfig) ([]*node, *bandwidth) {
	nodeCount := config.nodeCount + 1
	nodes := []*node{}
	bw := &bandwidth{
		values: map[*node]map[*node]float64{},
	}

	// CPU:= randomBasedOnHete(config.averageNodeResource,config.resourceHeterogeneity)
	// Mem:= randomBasedOnHete(config.averageNodeResource,config.resourceHeterogeneity)
	// fmt.Println(float64(config.nodeCPURange), config.resourceHeterogeneity, CPU)
	for i := 0; i < nodeCount; i++ {
		n := &node{
			ID:            i,
			cpu:           (rand.Intn(config.nodeCPURange)+1)* 2 * 500,
			mem:           (rand.Intn(config.nodeMemRange)+1)* 2 * 512,
			allocatedCpu:  0,
			allocatedMem:  0,
			executionRate: 1+(config.speedHeterogeneity+1.0)*rand.Float64(),
		}
		nodes = append(nodes, n)
	}

	for i := 0; i < nodeCount; i++ {
		from := nodes[i]
		if _, exist := bw.values[from]; !exist {
			bw.values[from] = map[*node]float64{}
		}
		for j := i; j < nodeCount; j++ {
			to := nodes[j]
			if _, exist := bw.values[to]; !exist {
				bw.values[to] = map[*node]float64{}
			}
			var randBandwidth float64
			if i == j {
				randBandwidth = 0
			} else {
				randBandwidth = 1+((config.speedHeterogeneity+1.0)*rand.Float64()*config.ccr*config.ccr)
				
			}

			bw.values[from][to] = randBandwidth
			bw.values[to][from] = randBandwidth
		}
	}

	// for idx, n := range nodes {
	// 	Log(fmt.Sprintf("node%d", idx), n)
	// 	Log(fmt.Sprintf("bandwidth%d", idx), bw.values[n])
	// }

	return nodes, bw
}

