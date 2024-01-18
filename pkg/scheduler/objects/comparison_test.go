package objects

import (
	"encoding/csv"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"
)

type testCase struct {
	count        int
	podCount     []int
	alpha        []float64
	density      []float64
	replicaCount []int
	nodes        []int
	CCR          []float64
	RRC          []float64
	speedHete    []float64
	resouHete    []float64
}

const (
	path     = "/home/lab/document/01-yunikorn/yunikorn-core/pkg/scheduler/objects/result"
	filename = "comparsion"
)

func createWriter() (*csv.Writer, *os.File) {
	rand.Seed(time.Now().UnixNano())
	var filePath string

	filePath = path + "/" + filename + "-" + RandSeq(7) + ".csv"
	file, err := os.Create(filePath)

	if err != nil {
		log.Fatalln("failed to open file", err)
	}
	w := csv.NewWriter(file)
	return w, file
	// defer w.Flush()
	// w.WriteAll(result)
}

func RandSeq(n int) string {
	var letters = []rune("abcdefghijklmnopqrstuvwxyz0123456789")
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func TestParallel(t *testing.T) {
	state := [][]int{}
	var wg sync.WaitGroup
	// state = append(state, []int{0, 0, 0, 0, 0, 0, 0, 0})
	// state = append(state, []int{0, 1, 0, 0, 0, 0, 0, 0})
	// state = append(state, []int{0, 2, 0, 0, 0, 0, 0, 0})
	state = append(state, []int{1, 0, 0, 0, 0, 0, 0, 0})
	state = append(state, []int{1, 1, 0, 0, 0, 0, 0, 0})
	state = append(state, []int{1, 2, 0, 0, 0, 0, 0, 0})
	state = append(state, []int{2, 0, 0, 0, 0, 0, 0, 0})
	state = append(state, []int{2, 1, 0, 0, 0, 0, 0, 0})
	state = append(state, []int{2, 2, 0, 0, 0, 0, 0, 0})
	state = append(state, []int{3, 0, 0, 0, 0, 0, 0, 0})
	state = append(state, []int{3, 1, 0, 0, 0, 0, 0, 0})
	state = append(state, []int{3, 2, 0, 0, 0, 0, 0, 0})
	state = append(state, []int{4, 0, 0, 0, 0, 0, 0, 0})
	state = append(state, []int{4, 1, 0, 0, 0, 0, 0, 0})
	state = append(state, []int{4, 2, 0, 0, 0, 0, 0, 0})
	state = append(state, []int{5, 0, 0, 0, 0, 0, 0, 0})
	state = append(state, []int{5, 1, 0, 0, 0, 0, 0, 0})
	state = append(state, []int{5, 2, 0, 0, 0, 0, 0, 0})
	wg.Add(len(state))
	for _, s := range state {
		
		go comparison(s, true, &wg)
	}
	
	wg.Wait()
}

// 700,0.20,0.40,6,32,1.00,0.500,0.50,32,702,
// 700,0.2,0.6,4,16,10.0,0.05,0.5
// 900,0.5,0.6,4,16,5.0,0.01,0.1

func getState(state []int, isload bool, wg *sync.WaitGroup) {
	value := []float64{1100, 0.8, 0.4, 8, 32, 10.0, 0.01, 0.5}

	cases := testCase{
		podCount:     []int{100, 300, 500, 700, 900, 1100},
		alpha:        []float64{0.2, 0.5, 0.8},
		density:      []float64{0.4, 0.6},
		replicaCount: []int{4, 6, 8},
		nodes:        []int{4, 8, 16, 32},
		CCR:          []float64{0.5, 1, 5, 10},
		RRC:          []float64{0.01, 0.05, 0.1, 0.5},
		speedHete:    []float64{0.1, 0.5, 1, 2},
		resouHete:    []float64{0.1, 0.5, 1, 2},
	}

	for i := 0; i < len(cases.podCount); i++ {
		if float64(cases.podCount[i]) == value[0] {
			fmt.Printf("%d,", i)
			break
		}
	}
	for j := 0; j < len(cases.alpha); j++ {
		if cases.alpha[j] == value[1] {
			fmt.Printf("%d,", j)
			break
		}
	}
	for k := 0; k < len(cases.density); k++ {
		if cases.density[k] == value[2] {
			fmt.Printf("%d,", k)
			break
		}
	}
	for l := 0; l < len(cases.replicaCount); l++ {
		if float64(cases.replicaCount[l]) == value[3] {
			fmt.Printf("%d,", l)
			break
		}
	}
	for m := 0; m < len(cases.nodes); m++ {
		if float64(cases.nodes[m]) == value[4] {
			fmt.Printf("%d,", m)
			break
		}
	}
	for n := 0; n < len(cases.CCR); n++ {
		if cases.CCR[n] == value[5] {
			fmt.Printf("%d,", n)
			break
		}
	}
	for o := 0; o < len(cases.RRC); o++ {
		if cases.RRC[o] == value[6] {
			fmt.Printf("%d,", o)
			break
		}
	}
	for p := 0; p < len(cases.speedHete); p++ {
		if cases.speedHete[p] == value[7] {
			fmt.Printf("%d", p)
			break
		}
	}

}

// 700,0.20,0.40,6,32,1.00,0.500,0.50,32,702,
// 700,0.2,0.6,4,16,10.0,0.05,0.5
func comparison(state []int, isload bool, wg *sync.WaitGroup)  {
	w, file := createWriter()
	defer file.Close()
	defer w.Flush()
	defer wg.Done()

	w.Write([]string{"podCount", "alpha", "density", "replicaCount", "nodeCount", "CCR", "speedHete", "MPEFT", "MPEFTusage", "IPPTS", "IPPTSusage", "CUSTOM", "CUSTOMusage"})

	cases := testCase{
		count:        10,
		podCount:     []int{100, 300, 500, 700, 900, 1100},
		alpha:        []float64{0.2, 0.5, 0.8},
		density:      []float64{0.4, 0.6},
		replicaCount: []int{4, 6, 8},
		nodes:        []int{4, 8, 16, 32},
		CCR:          []float64{0.5, 1, 5, 10},
		RRC:          []float64{0.01, 0.05, 0.1, 0.5},
		speedHete:    []float64{0.1, 0.5, 1, 2},
		resouHete:    []float64{0.1, 0.5, 1, 2},
	}

	for i := 0; i < len(cases.podCount); i++ {
		if isload {
			i = state[0]
		}
		for j := 0; j < len(cases.alpha); j++ {
			if isload {
				j = state[1]
			}
			for k := 0; k < len(cases.density); k++ {
				if isload {
					k = state[2]
				}
				for l := 0; l < len(cases.replicaCount); l++ {
					if isload {
						l = state[3]
					}
					for m := 0; m < len(cases.nodes); m++ {
						if isload {
							m = state[4]
						}
						for n := 0; n < len(cases.CCR); n++ {
							if isload {
								n = state[5]
							}
							// for o := 0; o < len(cases.RRC); o++ {
							// 	if isload {
							// 		o = state[6]
							// 	}
							for p := 0; p < len(cases.speedHete); p++ {
								if isload {
									p = state[7]
									isload = false
								}
								var q int64
								for q = 0; q < int64(cases.count); q++ {
									current := []string{}
									current = append(current, fmt.Sprintf("%d", cases.podCount[i]))
									current = append(current, fmt.Sprintf("%.1f", cases.alpha[j]))
									current = append(current, fmt.Sprintf("%.1f", cases.density[k]))
									current = append(current, fmt.Sprintf("%d", cases.replicaCount[l]))
									current = append(current, fmt.Sprintf("%d", cases.nodes[m]))
									current = append(current, fmt.Sprintf("%.1f", cases.CCR[n]))
									// current = append(current, fmt.Sprintf("%.2f", cases.RRC[o]))
									current = append(current, fmt.Sprintf("%.1f", cases.speedHete[p]))
									// current = append(current, fmt.Sprintf("%.2f", resourceHete))
									current = append(current, doWithTimeout(q, cases.podCount[i], cases.alpha[j], cases.density[k],
										cases.replicaCount[l], cases.nodes[m], cases.CCR[n], 0.0, cases.speedHete[p], 0.0)...)
									w.Write(current)
									w.Flush()
									
								}
								
							}
							// }
						}
					}
				}
			}
			return
		}
	}

	// // init
	// for _, podCount := range cases.podCount {
	// 	for _, a := range cases.alpha {
	// 		for _, density := range cases.density {
	// 			for _, replicaCount := range cases.replicaCount {
	// 				for _, nodeCount := range cases.nodes {
	// 					for _, ccr := range cases.CCR {
	// 						for _, rrc := range cases.RRC {
	// 							for _, speedHete := range cases.speedHete {
	// 								// for _, resourceHete := range cases.resouHete {
	// 									var i int64
	// 									for i = 0; i < int64(cases.count); i++ {
	// 										current :=[]string{}
	// 										current = append(current, fmt.Sprintf("%d", podCount))
	// 										current = append(current, fmt.Sprintf("%.2f", a))
	// 										current = append(current, fmt.Sprintf("%.2f", density))
	// 										current = append(current, fmt.Sprintf("%d", replicaCount))
	// 										current = append(current, fmt.Sprintf("%d", nodeCount))
	// 										current = append(current, fmt.Sprintf("%.2f", ccr))
	// 										current = append(current, fmt.Sprintf("%.3f", rrc))
	// 										current = append(current, fmt.Sprintf("%.2f", speedHete))
	// 										// current = append(current, fmt.Sprintf("%.2f", resourceHete))
	// 										current=append(current, doWithTimeout(i, podCount, a, density, replicaCount, nodeCount, ccr, rrc, speedHete, 0.0)...)
	// 										w.Write(current)
	// 										w.Flush()
	// 									}
	// 								// }
	// 							}
	// 						}
	// 					}
	// 				}
	// 			}
	// 		}
	// 	}
	// }

	// config := comparisonConfig{
	// 	times: 1,
	// }
	// var i int64
	// for i = 0; i < config.times; i++ {
	// 	rand.Seed(i)
	// 	config.podCount=300
	// 	config.minPerRank = 2
	// 	config.maxPerRank = rand.Intn(10) + 5
	// 	// config.minRanks = 10
	// 	// config.maxRanks = rand.Intn(15) + 20
	// 	config.percent = 30
	// 	config.replicaNum = rand.Intn(11) + 4
	// 	// config.replicaNum = 1
	// 	config.replicaCPURange = rand.Intn(4) + 1 // (rand.Int()%config.range + 1) * 2 * 1000,
	// 	config.replicaMemRange = rand.Intn(4) + 1
	// 	config.actionNum = 10
	// 	for nodeCount := 20; nodeCount >= 8; nodeCount -= 4 {
	// 		config.nodeCount = nodeCount
	// 		config.nodeCPURange = config.replicaCPURange*2 + rand.Intn(8) + 1 // (rand.Int()%config.nodeCPURange + 1) * 4 * 1000
	// 		config.nodeMemRange = config.replicaMemRange*2 + rand.Intn(8) + 1
	// 		current := []string{}
	// 		nodes, bw := createRandNodeByConfig(config)
	// 		jobsDag := generateRandomDAGWithConfig(config)
	// 		current = append(current, fmt.Sprintf("%d", config.nodeCount))
	// 		current = append(current, fmt.Sprintf("%d", jobsDag.replicasCount))
	// 		for algoCount := 0; algoCount < 3; algoCount++ {
	// 			rand.Seed(i)
	// 			nodes, bw = createRandNodeByConfig(config)
	// 			jobsDag = generateRandomDAGWithConfig(config)
	// 			if jobsDag.replicasCount == 0 {
	// 				continue
	// 			}
	// 			if algoCount == 0 {
	// 				m := createMPEFT(jobsDag.Vectors, nodes, bw)
	// 				current = append(current, fmt.Sprintf("%.1f", m.simulate()))
	// 			} else if algoCount == 1 {
	// 				p := createIPPTS(jobsDag.Vectors, nodes, bw)
	// 				current = append(current, fmt.Sprintf("%.2f", p.simulate()))
	// 			} else {
	// 				c := createCustomAlgo(jobsDag.Vectors, nodes, bw)
	// 				current = append(current, fmt.Sprintf("%.2f", c.simulate()))
	// 			}
	// 		}
	// 		w.Write(current)
	// 		w.Flush()
	// 	}
	// }
}

func TestTestWithCase(t *testing.T) {
	var seed int64 = 1
	podCount := 100
	alpha := 0.2
	density := 0.4
	replicaCount := 4
	nodeCount := 4
	CCR := 0.5
	RRC := 0.01
	speedHete := 0.01
	resouHete := 0.1
	for i := 0; i < 10; i++ {
		testWithCase(seed, podCount, alpha, density, replicaCount, nodeCount, CCR, RRC, speedHete, resouHete)
	}

}
