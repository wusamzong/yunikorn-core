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

func doWithTimeout(seed int64, config comparisonConfig) []string {
    // Create a channel to signal completion of the function and return result
    done := make(chan []string, 1)

    // Start the function in a goroutine
    go func() {
        // Call your function here with parameters and capture the result
        result := testWithCase(seed, config)
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

func settingConfig(config comparisonConfig) comparisonConfig{
	width := int(math.Sqrt(float64(config.podCount) / ((1.0 - config.alpha) / config.alpha)))
	config.width = width
	config.replicaCPURange = 4 
	config.replicaMemRange = 4

	config.nodeCPURange = config.replicaNum*2 
	config.nodeMemRange = config.replicaNum*2
	
	config.actionNum = 10
	return config
}

func testWithCase(seed int64 ,config comparisonConfig) []string {
	config=settingConfig(config)

	current:=[]string{}
	for algoCount := 0; algoCount < 3; algoCount++ {

		rand.Seed(seed)
		nodes, bw := createRandNodeByConfig(config)
		jobsDag := generateRandomDAGWithConfig(config)
		if jobsDag.replicasCount == 0 {
			continue
		}

		if algoCount == 0 {
			// continue
			m := createMPEFT(jobsDag.Vectors, nodes, bw)
			// current = append(current, fmt.Sprintf("%d", jobsDag.replicasCount))
			makespan, SLR := m.simulate()
			current = append(current, fmt.Sprintf("%.0f", makespan))
			current = append(current, fmt.Sprintf("%.3f", SLR))
		} else if algoCount == 1 {
			// continue
			p := createIPPTS(jobsDag.Vectors, nodes, bw)
			makespan, SLR := p.simulate()
			current = append(current, fmt.Sprintf("%.0f", makespan))
			current = append(current, fmt.Sprintf("%.3f", SLR))
		} else {
			c := createCustomAlgo(jobsDag.Vectors, nodes, bw)
			makespan, SLR := c.simulate()
			current = append(current, fmt.Sprintf("%.0f", makespan))
			current = append(current, fmt.Sprintf("%.3f", SLR))
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


	for i := 0; i < nodeCount; i++ {
		resource := (rand.Intn(config.nodeCPURange)+4)
		n := &node{
			ID:            i,
			cpu:           resource * 500,
			mem:           resource * 512,
			allocatedCpu:  0,
			allocatedMem:  0,
			executionRate: 1+rand.Float64()*4*config.speedHeterogeneity,
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
				randBandwidth = 1+rand.Float64()*5
				
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
