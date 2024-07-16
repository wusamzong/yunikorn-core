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

	"github.com/joho/godotenv"
)

type testCase struct {
	count        int
	podCount     []int
	alpha        []float64
	density      []float64
	replicaCount []int
	nodes        []int
	CCR          []float64
	TCR          []float64
	RRC          []float64
	speedHete    []float64
	resouHete    []float64
	actionCount  []int
}

const (
	filename = "comparsion"
)

func createWriter() (*csv.Writer, *os.File) {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	path := os.Getenv("storagePath")

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

func TestP1(t *testing.T) {
	podCountIdx := []int{0}
	runByIdx(podCountIdx)
}
func TestP2(t *testing.T) {
	podCountIdx := []int{1}
	runByIdx(podCountIdx)
}
func TestP3(t *testing.T) {
	podCountIdx := []int{2}
	runByIdx(podCountIdx)
}
func TestP4(t *testing.T) {
	podCountIdx := []int{3}
	runByIdx(podCountIdx)
}
func TestP5(t *testing.T) {
	podCountIdx := []int{4}
	runByIdx(podCountIdx)
}

func TestP6(t *testing.T) {
	podCountIdx := []int{5}
	runByIdx(podCountIdx)
}

func runByIdx(podCountIdx []int) {
	state := createStates(podCountIdx)
	maxGoroutines := 30
	guard := make(chan struct{}, maxGoroutines)
	var wg sync.WaitGroup
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

func createStates(podCountIdx []int) [][]int {
	state := [][]int{}
	cases := testCase{
		podCount: []int{800, 900,1000, 1100},
		replicaCount: []int{8,10,12},
	}
	for _, i := range podCountIdx {
		for j := 0; j < len(cases.replicaCount); j++ {
			newState := make([]int, 8)
			newState[0] = i
			newState[1] = j
			state = append(state, newState)
		}
	}
	return state
}

func comparison(state []int) {
	w, file := createWriter()
	defer file.Close()
	defer w.Flush()
	
	w.Write([]string{"podCount", "replicaCount", "nodeCount", "CCR", "CTV", "TCR", "stageCount", 
	"MPEFT", "MPEFTSLR","MPEFTspeedup","MPEFTefficiency", 
	"IPPTS", "IPPTSSLR","IPPTSspeedup","IPPTSefficiency",
	"HWS", "HWSSLR","HWSspeedup","HWSefficiency",
	"MACRO", "MACROSLR","MACROspeedup","MACROefficiency"})

	// only test dynamic
	// w.Write([]string{"podCount", "replicaCount", "nodeCount", "CCR", "CTV", "TCR", "stageCount", 
	// "HWS", })

	cases := testCase{
		count:        10,
		podCount:     []int{700,800, 900,1000},
		replicaCount: []int{8,10,12},
		nodes:        []int{12,16,20},
		CCR:          []float64{0.2, 0.5, 2, 5},
		speedHete:    []float64{0.25, 0.5, 1.0, 2.0},
		TCR:          []float64{0.2, 0.5, 2, 5}, //Transmission Cost Ratio
		actionCount:  []int{2, 3, 4},

		// alpha:        []float64{0.08},
		// replicaCount: []int{6},
		// nodes:        []int{16},
		// CCR:          []float64{1},
		// speedHete:    []float64{1.0},
		// TCR:          []float64{1}, //Transmission Cost Ratio
		// actionCount:  []int{3},
	}

	isload := true
	for i := 0; i < len(cases.podCount); i++ {
		if isload && state[0] != i {
			continue
		}
		for l := 0; l < len(cases.replicaCount); l++ {
			if isload && state[1] != l {
				continue
			}
			for m := 0; m < len(cases.nodes); m++ {
				for n := 0; n < len(cases.CCR); n++ {
					for o := 0; o < len(cases.speedHete); o++ {
						for p := 0; p < len(cases.TCR); p++ {
							for r := 0; r < len(cases.actionCount); r++ {
								var q int64
								for q = 0; q < int64(cases.count); q++ {
									current := []string{}
									current = append(current, fmt.Sprintf("%d", cases.podCount[i]))
									current = append(current, fmt.Sprintf("%d", cases.replicaCount[l]))
									current = append(current, fmt.Sprintf("%d", cases.nodes[m]))
									current = append(current, fmt.Sprintf("%.1f", cases.CCR[n]))
									current = append(current, fmt.Sprintf("%.1f", cases.speedHete[o]))
									current = append(current, fmt.Sprintf("%.1f", cases.TCR[p]))
									current = append(current, fmt.Sprintf("%d", cases.actionCount[r]))
									config := comparisonConfig{
										podCount:           cases.podCount[i],
										replicaNum:         cases.replicaCount[l],
										nodeCount:          cases.nodes[m],
										ccr:                cases.CCR[n],
										speedHeterogeneity: cases.speedHete[o],  
										tcr:                cases.TCR[p],
										actionNum:          cases.actionCount[r],
									}

									current = append(current, doWithTimeout(q, config)...)
									w.Write(current)
									w.Flush()
								}
							}
						}
					}
				}
			}
			return
		}
	}
}

func TestTestWithCase(t *testing.T) {
	var seed int64 = 4
	config := comparisonConfig{
		podCount:           40,
		alpha:              0.5,
		replicaNum:         4,
		actionNum:          4,
		nodeCount:          3,
		ccr:                1.0,
		speedHeterogeneity: 1.0,
		tcr:                1.0,
	}
	for i := 0; i < 1; i++ {
		result := testWithCase(seed, config)
		for algoCount:=0; algoCount<4; algoCount++{
			for metricCount:=0; metricCount<4; metricCount++{
				fmt.Print(result[algoCount*4+metricCount]+", ")
			}
			fmt.Println()
		}
	}
}

func TestComparisonSample(t *testing.T) {
	var randomSeed int64
	randomSeed = 31

	rand.Seed(randomSeed)
	nodes, bw := createSampleNode()
	jobsDag := createSampleJobDAG()
	c := createCustomAlgo(jobsDag.Vectors, nodes, bw)
	metric := c.simulate()
	fmt.Println("=>  ", metric.makespan, metric.SLR)
	fmt.Println()

	// rand.Seed(randomSeed)
	// nodes, bw = createSampleNode()
	// jobsDag = createSampleJobDAG()
	// jobsWithOnlyReplica(jobsDag.Vectors)
	// m := createMPEFT(jobsDag.Vectors, nodes, bw)
	// metric = m.simulate()
	// fmt.Println("=>  ", metric.makespan, metric.SLR)
	// fmt.Println()

	// rand.Seed(randomSeed)
	// nodes, bw = createSampleNode()
	// jobsDag = createSampleJobDAG()
	// jobsWithOnlyReplica(jobsDag.Vectors)
	// a := createMacro(jobsDag.Vectors, nodes, bw)
	// metric = a.simulate()
	// fmt.Println("=>  ", metric.makespan, metric.SLR)
	// fmt.Println()

	// rand.Seed(randomSeed)
	// nodes, bw = createSampleNode()
	// jobsDag = createSampleJobDAG()
	// jobsWithOnlyReplica(jobsDag.Vectors)
	// p := createIPPTS(jobsDag.Vectors, nodes, bw)
	// metric = p.simulate()
	// fmt.Println("=>  ", metric.makespan, metric.SLR)
	// fmt.Println()
}

func createSampleNode() ([]*node, *bandwidth) {
	nodes := []*node{}
	bw := &bandwidth{
		values: map[*node]map[*node]float64{},
	}

	n1 := &node{
		ID:            1,
		cpu:           3 * 500,
		mem:           3 * 512,
		allocatedCpu:  0,
		allocatedMem:  0,
		executionRate: 2.5,
	}
	n2 := &node{
		ID:            2,
		cpu:           3 * 500,
		mem:           3 * 512,
		allocatedCpu:  0,
		allocatedMem:  0,
		executionRate: 3,
	}
	n3 := &node{
		ID:            3,
		cpu:           3 * 500,
		mem:           3 * 512,
		allocatedCpu:  0,
		allocatedMem:  0,
		executionRate: 2,
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
			replicaNum: 4,
			// replicaNum: 1,
			replicaCpu:   500,
			replicaMem:   512,
			cpuIntensive: rand.Float64() * 1.2,
			memIntensive: rand.Float64() * 1.2,
			actionNum:    2,
			children:     []*Job{},
			finish:       0,
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
				r.finalDataSize[child] = rand.Float64()*45*3 + 10
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

		randExecutionTime := rand.Float64()*50 + 50
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
				a.datasize[r] = 1 + rand.Float64()*20 + 10
				// fmt.Println("from",pr.ID,"to",r.ID,"data",a.datasize[r])
			}
		}
	}
	// fmt.Println()

}

func TestNodeToActionExecutionTime(t *testing.T) {
	rand.Seed(31)
	nodes, _ := createSampleNode()
	jobsDag := createSampleJobDAG()

	table := [][]string{}
	for _, job := range jobsDag.Vectors {
		// create a table [][]string with columnID -> actionID, rowID -> nodeID

		JobID := []string{fmt.Sprintf("Job %d", job.ID)}
		table = append(table, JobID)

		header := []string{""}
		for _, action := range job.replicas[0].actions {
			header = append(header, fmt.Sprintf("Action%d", action.ID))
		}
		table = append(table, header)

		for _, node := range nodes {
			row := []string{fmt.Sprintf("Node%d", node.ID)}
			for _, action := range job.replicas[0].actions {
				value := action.executionTime / node.executionRate
				row = append(row, fmt.Sprintf("%.1f", value))
			}
			table = append(table, row)
		}
	}
	for _, row := range table {
		fmt.Println(row)
	}
	writingCSV("02_actionExecutionTime", table)
}

func TestActionDatasize(t *testing.T) {
	rand.Seed(31)
	// nodes, _ := createSampleNode()
	jobsDag := createSampleJobDAG()

	table := [][]string{}
	for _, job := range jobsDag.Vectors {
		// create a table [][]string with columnID -> actionID, rowID -> nodeID

		JobID := []string{fmt.Sprintf("Job %d", job.ID)}
		table = append(table, JobID)

		for actionIdx := 0; actionIdx < len(job.replicas[0].actions)-1; actionIdx++ {
			actionID := []string{fmt.Sprintf("Shuffle %d", actionIdx)}
			table = append(table, actionID)

			header := []string{""}
			for _, replica := range job.replicas {
				header = append(header, fmt.Sprintf("Replica%d", replica.ID))
			}
			table = append(table, header)
			for _, fromReplica := range job.replicas {
				row := []string{fmt.Sprintf("Replica%d", fromReplica.ID)}
				for _, toReplica := range job.replicas {

					value := fromReplica.actions[actionIdx].datasize[toReplica]
					row = append(row, fmt.Sprintf("%.1f", value))

				}
				table = append(table, row)

			}
		}

	}
	for _, row := range table {
		fmt.Println(row)
	}

	writingCSV("02_shuffleDataSize", table)
}

func TestFinalDatasize(t *testing.T) {
	rand.Seed(31)
	// nodes, _ := createSampleNode()
	jobsDag := createSampleJobDAG()

	table := [][]string{}
	for _, FromJob := range jobsDag.Vectors {
		// create a table [][]string with columnID -> actionID, rowID -> nodeID

		JobID := []string{fmt.Sprintf("Job %d", FromJob.ID)}
		table = append(table, JobID)

		header := []string{""}
		for _, ToJob := range jobsDag.Vectors {
			header = append(header, fmt.Sprintf("Job%d", ToJob.ID))
		}
		table = append(table, header)

		for _, FromReplica := range FromJob.replicas {
			row := []string{fmt.Sprintf("Replica%d", FromReplica.ID)}
			for _, ToJob := range jobsDag.Vectors {
				if _, exist := FromReplica.finalDataSize[ToJob]; exist {
					value := FromReplica.finalDataSize[ToJob]
					row = append(row, fmt.Sprintf("%.1f", value))
				} else {
					row = append(row, "")
				}
			}
			table = append(table, row)
		}

	}
	for _, row := range table {
		fmt.Println(row)
	}

	writingCSV("02_finalDataSize", table)
}

func writingCSV(filename string, table [][]string) {
	file, err := os.Create(fmt.Sprint(filename, ".csv"))
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer file.Close()

	// 創建CSV writer
	writer := csv.NewWriter(file)
	defer writer.Flush()

	// 寫入資料到CSV文件
	for _, record := range table {
		if err := writer.Write(record); err != nil {
			fmt.Println("Error:", err)
			return
		}
	}

	fmt.Println("CSV file created successfully")
}
