package objects

import (
	"fmt"
	"math"
	"math/rand"
	"time"
	// "os"
	// "sort"
)

type metric struct {
	makespan   float64
	SLR        float64
	speedup    float64
	efficiency float64
}

func randomBasedOnHete(mid float64, hete float64) int {
	ran := mid * hete
	startFrom := int(mid - ran)
	result := startFrom + rand.Int()%(int(ran*2)+1)
	if result <= 1 {
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

func settingConfig(config comparisonConfig) comparisonConfig {
	config.alpha = 0.3
	width := int(math.Sqrt(float64(config.podCount) / ((1.0 - config.alpha) / config.alpha)))
	config.width = width
	config.replicaCPURange = 4
	config.replicaMemRange = 4

	config.nodeCPURange = config.replicaNum * 2
	config.nodeMemRange = config.replicaNum * 2

	return config
}

func testWithCase(seed int64, config comparisonConfig) []string {

	config = settingConfig(config)

	current := []string{}
	for algoCount := 0; algoCount < 4; algoCount++ {

		rand.Seed(seed)
		nodes, bw := createRandNodeByConfig(config)
		jobsDag := generateRandomDAGWithConfig(config)
		fmt.Println("replica count")
		if jobsDag.replicasCount == 0 {
			continue
		}
		fmt.Println("done")

		var metric metric
		if algoCount == 0 {
			// continue
			jobsDag.replicasCount = len(jobsDag.Vectors)
			m := createMPEFT(jobsDag.Vectors, nodes, bw)
			// current = append(current, fmt.Sprintf("%d", jobsDag.replicasCount))
			metric = m.simulate()
			current = append(current, fmt.Sprintf("%.0f", metric.makespan))
			current = append(current, fmt.Sprintf("%.3f", metric.SLR))
		} else if algoCount == 1 {
			// continue
			jobsDag.replicasCount = len(jobsDag.Vectors)
			p := createIPPTS(jobsDag.Vectors, nodes, bw)
			metric = p.simulate()
			current = append(current, fmt.Sprintf("%.0f", metric.makespan))
			current = append(current, fmt.Sprintf("%.3f", metric.SLR))
		} else if algoCount == 2{
			c := createCustomAlgo(jobsDag.Vectors, nodes, bw)
			metric = c.simulate()
			current = append(current, fmt.Sprintf("%.0f", metric.makespan))
			current = append(current, fmt.Sprintf("%.3f", metric.SLR))
		} else {
			jobsWithOnlyReplica(jobsDag.Vectors)
			a := createMacro(jobsDag.Vectors, nodes, bw)
			metric = a.simulate()
			current = append(current, fmt.Sprintf("%.0f", metric.makespan))
			current = append(current, fmt.Sprintf("%.3f", metric.SLR))
		}

		rand.Seed(seed)
		nodes, bw = createRandNodeByConfig(config)
		jobsDag = generateRandomDAGWithConfig(config)
		speedup := calSpeedup(nodes, jobsDag.Vectors, metric.makespan)
		efficiency := speedup/float64(len(nodes))

		current = append(current, fmt.Sprintf("%.3f", speedup))
		current = append(current, fmt.Sprintf("%.3f", efficiency))
	}

	return current
}

func createRandNodeByConfig(config comparisonConfig) ([]*node, *bandwidth) {
	nodeCount := config.nodeCount + 1
	nodes := []*node{}
	bw := &bandwidth{
		values: map[*node]map[*node]float64{},
	}

	basedPerformance := 50.0
	for i := 0; i < nodeCount; i++ {
		resource := 16
		variation := rand.Float64()*5*config.speedHeterogeneity - config.speedHeterogeneity
		n := &node{
			ID:            i,
			cpu:           resource * 500,
			mem:           resource * 512,
			allocatedCpu:  0,
			allocatedMem:  0,
			executionRate: basedPerformance + variation,
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
				randBandwidth = basedPerformance + rand.Float64()*5

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