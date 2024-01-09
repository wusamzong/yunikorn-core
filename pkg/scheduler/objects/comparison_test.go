package objects

import (
	"encoding/csv"
	"fmt"
	"os"
	"time"
	"log"
	"math/rand"
	"testing"
)

const (
	path     = "/home/lab/document/01-yunikorn/yunikorn-core/pkg/scheduler/objects/result"
	filename = "comparsion"
)

func write(result [][]string) {
	var filePath string

	filePath = path + "/" + filename + "-" + RandSeq(7) + ".csv"
	file, err := os.Create(filePath)

	defer file.Close()
	if err != nil {
		log.Fatalln("failed to open file", err)
	}
	w := csv.NewWriter(file)
	defer w.Flush()
	w.WriteAll(result)
}

func RandSeq(n int) string {
	var letters = []rune("abcdefghijklmnopqrstuvwxyz0123456789")
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func TestComparison(t *testing.T) {
	config := comparisonConfig{
		times: 300,
	}
	result := [][]string{}
	result = append(result, []string{"MPEFT", "IPPTS", "Custom"})
	for i := 0; i < config.times; i++ {
		var randomSeed int64 = int64(i)
		rand.Seed(randomSeed)

		config.minPerRank = rand.Intn(15) + 1
		config.maxPerRank = config.minPerRank + rand.Intn(15)
		config.minRanks = rand.Intn(15) + 1
		config.maxRanks = config.minRanks + rand.Intn(15)
		config.percent = rand.Intn(40) + 1

		// config.replicaNumRange = rand.Intn(30) + 1
		config.replicaNumRange = 1
		config.replicaCPURange = rand.Intn(4) + 1 // (rand.Int()%config.range + 1) * 2 * 1000,
		config.replicaMemRange = rand.Intn(4) + 1
		config.actionNum = rand.Intn(20) + 1

		config.nodeCount = rand.Intn(30) + 1
		config.nodeCPURange = config.replicaCPURange*2 + rand.Intn(8) + 1 // (rand.Int()%config.nodeCPURange + 1) * 4 * 1000
		config.nodeMemRange = config.replicaMemRange*2 + rand.Intn(8) + 1

		current := []string{}
		nodes, bw := createRandNodeByConfig(config)
		jobsDag := generateRandomDAGWithConfig(config)
		m := createMPEFT(jobsDag.Vectors, nodes, bw)
		current = append(current, fmt.Sprintf("%.2f", m.simulate()))

		nodes, bw = createRandNodeByConfig(config)
		jobsDag = generateRandomDAGWithConfig(config)
		p := createIPPTS(jobsDag.Vectors, nodes, bw)
		current = append(current, fmt.Sprintf("%.2f", p.simulate()))

		nodes, bw = createRandNodeByConfig(config)
		jobsDag = generateRandomDAGWithConfig(config)
		c := createCustomAlgo(jobsDag.Vectors, nodes, bw)
		current = append(current, fmt.Sprintf("%.2f", c.simulate()))
	
		if current[0]=="0.00" || current[1]=="0.00" || current[2]=="0.00"{
			continue
		}
		result = append(result, current)
	}
	rand.Seed(time.Now().UnixNano())
	write(result)
}

func createRandNodeByConfig(config comparisonConfig) ([]*node, *bandwidth) {
	nodeCount := config.nodeCount + 1
	nodes := []*node{}
	bw := &bandwidth{
		values: map[*node]map[*node]float64{},
	}
	for i := 0; i < nodeCount; i++ {
		n := &node{
			ID:            i,
			cpu:           (rand.Int()%config.nodeCPURange + 1) * 4 * 1000,
			mem:           (rand.Int()%config.nodeMemRange + 1) * 4 * 1024,
			allocatedCpu:  0,
			allocatedMem:  0,
			executionRate: rand.Float64() + 1,
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
				randBandwidth = rand.Float64()*100 + 1
			}

			bw.values[from][to] = randBandwidth
			bw.values[to][from] = randBandwidth
		}
	}

	for idx, n := range nodes {
		Log(fmt.Sprintf("node%d", idx), n)
		Log(fmt.Sprintf("bandwidth%d", idx), bw.values[n])
	}

	return nodes, bw
}
