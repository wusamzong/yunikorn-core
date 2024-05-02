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
	path     = "/home/hsuanzong/document/01-yunikorn/yunikorn-core/pkg/scheduler/objects/result"
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
	cases := testCase{
		count:        1,
		podCount:     []int{100, 300, 500, 700, 900, 1100},
		alpha:        []float64{0.2, 0.5, 0.8},
		replicaCount: []int{4, 6, 8},
		nodes:        []int{4, 8, 16, 32},
		CCR:          []float64{0.1, 0.5, 1, 5, 10, 20},
	}
	for i := 3; i < len(cases.podCount); i++ {
		for j := 0; j < len(cases.alpha); j++ {
			// for k := 0; k < len(cases.replicaCount); k++ {
				newState := make([]int, 8)
				newState[0] = i
				newState[1] = j
				// newState[2] = k
				state = append(state, newState)
			// }
		}
	}
	maxGoroutines := 20
	guard := make(chan struct{}, maxGoroutines)

	wg.Add(len(state))
	for _, s := range state {
		guard <- struct{}{} // 嘗試向 channel 發送一個空結構，如果 channel 滿了，這裡會阻塞
		go func(s []int) {
			comparison(s)
			<-guard
			wg.Done()
		}(s)
	}

	wg.Wait()
}

func getState(state []int, isload bool) {
	value := []float64{1100, 0.8, 0.4, 8, 32, 10.0, 0.01, 0.5}

	cases := testCase{
		podCount:     []int{100, 300, 500, 700, 900, 1100},
		alpha:        []float64{0.2, 0.5, 0.8},
		replicaCount: []int{4, 6, 8},
		nodes:        []int{4, 8, 16, 32},
		CCR:          []float64{0.1, 0.5, 1, 5, 10, 20},
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
func comparison(state []int) {
	w, file := createWriter()
	defer file.Close()
	defer w.Flush()

	w.Write([]string{"podCount", "alpha", "replicaCount", "nodeCount", "CCR", "speedHete", "MPEFT", "MPEFTSLR", "IPPTS", "IPPTSSLR", "HWS", "HWSSLR"})

	cases := testCase{
		count:        1,
		podCount:     []int{100, 300, 500, 700, 900, 1100},
		alpha:        []float64{0.2, 0.5, 0.8},
		replicaCount: []int{4, 6, 8},
		nodes:        []int{4, 8, 16, 32},
		CCR:          []float64{0.1, 0.5, 1, 5, 10, 20},
		speedHete:    []float64{0.1, 0.25, 0.5, 1.0, 2.0},
	}

	isload := true
	for i := 0; i < len(cases.podCount); i++ {
		if isload && state[0] != i {
			continue
		}
		for j := 0; j < len(cases.alpha); j++ {
			if isload && state[1] != j {
				continue
			}
			for l := 0; l < len(cases.replicaCount); l++ {
				if isload && state[2] != l {
					continue
				}
				for m := 0; m < len(cases.nodes); m++ {
					for n := 0; n < len(cases.CCR); n++ {
						for o := 0; o < len(cases.speedHete); o++ {
							var q int64
							for q = 0; q < int64(cases.count); q++ {
								current := []string{}
								current = append(current, fmt.Sprintf("%d", cases.podCount[i]))
								current = append(current, fmt.Sprintf("%.1f", cases.alpha[j]))
								current = append(current, fmt.Sprintf("%d", cases.replicaCount[l]))
								current = append(current, fmt.Sprintf("%d", cases.nodes[m]))
								current = append(current, fmt.Sprintf("%.1f", cases.CCR[n]))
								current = append(current, fmt.Sprintf("%.1f", cases.speedHete[o]))

								config := comparisonConfig{
									podCount:           cases.podCount[i],
									alpha:              cases.alpha[j],
									replicaNum:         cases.replicaCount[l],
									nodeCount:          cases.nodes[m],
									ccr:                cases.CCR[n],
									speedHeterogeneity: cases.speedHete[o],
								}

								current = append(current, doWithTimeout(q, config)...)
								w.Write(current)
								w.Flush()

							}
						}
					}
				}
				return
			}
		}
	}
}

func TestTestWithCase(t *testing.T) {
	var seed int64 = 1
	config := comparisonConfig{
		podCount:           100,
		alpha:              0.2,
		replicaNum:         4,
		nodeCount:          4,
		ccr:                20.0,
		speedHeterogeneity: 1.0,
	}
	for i := 0; i < 1; i++ {
		result := testWithCase(seed, config)
		fmt.Println(result)
	}

}

func TestComparisonSample(t *testing.T) {
	rand.Seed(19)
	nodes, bw := createSampleNode()
	jobsDag := createSampleJobDAG()

	c := createCustomAlgo(jobsDag.Vectors, nodes, bw)
	makespan, resourceUsage := c.simulate()
	fmt.Println("=>  ", makespan, resourceUsage)

	// nodes, bw = createSampleNode()
	// jobsDag = createSampleJobDAG()
	// m := createMPEFT(jobsDag.Vectors, nodes, bw)
	// makespan, resourceUsage := m.simulate()
	// fmt.Println("=>  ", makespan, resourceUsage)

	// nodes, bw = createSampleNode()
	// jobsDag = createSampleJobDAG()
	// p := createIPPTS(jobsDag.Vectors, nodes, bw)
	// makespan, resourceUsage = p.simulate()
	// fmt.Println("=>  ", makespan, resourceUsage)
}

func createSampleNode() ([]*node, *bandwidth) {
	nodes := []*node{}
	bw := &bandwidth{
		values: map[*node]map[*node]float64{},
	}

	n1 := &node{
		ID:            1,
		cpu:           2 * 500,
		mem:           2 * 512,
		allocatedCpu:  0,
		allocatedMem:  0,
		executionRate: 1.5,
	}
	n2 := &node{
		ID:            2,
		cpu:           2 * 500,
		mem:           2 * 512,
		allocatedCpu:  0,
		allocatedMem:  0,
		executionRate: 1.8,
	}
	n3 := &node{
		ID:            3,
		cpu:           2 * 500,
		mem:           2 * 512,
		allocatedCpu:  0,
		allocatedMem:  0,
		executionRate: 1.2,
	}
	nodes = append(nodes, n1)
	nodes = append(nodes, n2)
	nodes = append(nodes, n3)

	bw.values[n1] = map[*node]float64{}
	bw.values[n2] = map[*node]float64{}
	bw.values[n3] = map[*node]float64{}

	bw.values[n1][n1] = 0.0
	bw.values[n1][n2] = 1.5
	bw.values[n1][n3] = 1.7
	bw.values[n2][n1] = 1.5
	bw.values[n2][n2] = 0.0
	bw.values[n2][n3] = 1.3
	bw.values[n3][n1] = 1.7
	bw.values[n3][n2] = 1.3
	bw.values[n3][n3] = 0.0

	// for idx, n := range nodes {
	// 	Log(fmt.Sprintf("node%d", idx), n)
	// 	Log(fmt.Sprintf("bandwidth%d", idx), bw.values[n])
	// }

	return nodes, bw
}

func createSampleJobDAG() *JobsDAG {
	jobsDAG := JobsDAG{
		Vectors: []*Job{},
	}
	for i := 0; i < 7; i++ {
		job := &Job{
			ID:         i,
			replicaNum: 2,
			// replicaNum: 1,
			replicaCpu: 500,
			replicaMem: 512,
			actionNum:  3,
			children:   []*Job{},
			finish:     0,
		}
		// fmt.Println("=> job",i)
		createSampleReplica(job)
		job.predictExecutionTime = job.predictTime(0.0)
		jobsDAG.Vectors = append(jobsDAG.Vectors, job)
	}

	vectors := jobsDAG.Vectors
	jobsDAG.Vectors[0].children = []*Job{vectors[1], vectors[2], vectors[3]}
	jobsDAG.Vectors[1].children = []*Job{vectors[4]}
	jobsDAG.Vectors[2].children = []*Job{vectors[5]}
	jobsDAG.Vectors[3].children = []*Job{vectors[5]}
	jobsDAG.Vectors[4].children = []*Job{vectors[6]}
	jobsDAG.Vectors[5].children = []*Job{vectors[6]}
	jobsDAG.Vectors[6].children = []*Job{}
	for _, j := range vectors {
		// Log(fmt.Sprintf("job:%d", i), j)
		// fmt.Println("Job", j.ID)
		// Initialize final Data size
		for _, r := range j.replicas {
			for _, child := range j.children {
				r.finalDataSize[child] = 1 + rand.Float64()*30
				// fmt.Println("from",r.ID,"to",child.ID,"final data",r.finalDataSize[child])
			}
		}
	}

	// create parent for each vectors by using children
	jobsDAG = *ChildToParent(&jobsDAG)

	// create relationship between replicas
	for _, j := range vectors {
		childrenReplicas := j.getChildrenReplica()
		parentReplicas := j.getParentReplica()
		for _, r := range j.replicas {
			r.children = childrenReplicas
			r.parent = parentReplicas
		}
	}

	return &jobsDAG
}

func createSampleReplica(j *Job) {
	for i := 0; i < j.replicaNum; i++ {
		j.createReplica()
	}

	for i := 0; i < j.actionNum; i++ {

		randExecutionTime := 1 + rand.Float64()*5
		// if randExecutionTime<1{
		// 	randExecutionTime+=1
		// }
		// fmt.Println(randExecutionTime)

		for _, pr := range j.replicas {
			a := pr.createAction(randExecutionTime)
			if i == j.actionNum-1 {
				continue
			}
			for _, r := range j.replicas {
				a.datasize[r] = 1 + rand.Float64()*5
				// fmt.Println("from",pr.ID,"to",r.ID,"data",a.datasize[r])
			}
		}
	}
	// fmt.Println()

}
