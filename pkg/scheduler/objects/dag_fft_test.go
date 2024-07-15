package objects

import (
	// "github.com/apache/yunikorn-core/pkg/scheduler/objects/custom"
	// // "github.com/apache/yunikorn-scheduler-interface/lib/go/si"
	"fmt"
	// "gotest.tools/v3/assert"
	// "strconv"
	"sync"
	"testing"
	"github.com/joho/godotenv"
	"os"
	"math/rand"
	"time"
	"log"
	"encoding/csv"

	// "net/http"
	// _ "net/http/pprof"
)

type fftTestCase struct {
	level []int
	CCR   []float64
	nodes []int
}

func TestDAGParallel(t *testing.T) {
	
	var wg sync.WaitGroup

	cases := fftTestCase{
		level: []int{4, 5, 6, 7}, 
		nodes: []int{8, 12, 16, 20},
		CCR:   []float64{0.2, 0.5, 2, 5},
		// level: []int{5}, 
		// nodes: []int{8},
		// CCR:   []float64{ 5},
	}
	numberOfCases:=len(cases.level)* len(cases.nodes)* len(cases.CCR)
	wg.Add(numberOfCases)
	for _, l := range cases.level {
		for _, n := range cases.nodes {
			for _, c:= range cases.CCR{
				go comparisonFFTDAG(l, n, c, &wg)
			}
		}

	}
	wg.Wait()
}

func TestSingleFFTDAG(t *testing.T){
	
	var wg sync.WaitGroup
	wg.Add(1)
	go comparisonFFTDAG(7, 8, 1, &wg)
	
	// go comparisonFFTDAG(6, 4, 0.5, &wg)
	// go comparisonFFTDAG(6, 4, 1, &wg)
	// go comparisonFFTDAG(6, 4, 5, &wg)
	// go comparisonFFTDAG(6, 4, 10, &wg)
	// go comparisonFFTDAG(6, 4, 20, &wg)
	wg.Wait()
}

func comparisonFFTDAG(level int, node int, ccr float64, wg *sync.WaitGroup) {
	w, file := createFFTWriter()
	defer file.Close()
	defer w.Flush()
	defer wg.Done()

	w.Write([]string{"level", "nodeCount", "CCR", 
	"MPEFT", "MPEFTSLR","MPEFTspeedup","MPEFTefficiency", 
	"IPPTS", "IPPTSSLR","IPPTSspeedup","IPPTSefficiency",
	"HWS", "HWSSLR","HWSspeedup","HWSefficiency",
	"MACRO", "MACROSLR","MACROspeedup","MACROefficiency"})
	for count := 0; count<1; count++{
		current := []string{}
		current = append(current, fmt.Sprintf("%d", level))
		current = append(current, fmt.Sprintf("%d", node))
		current = append(current, fmt.Sprintf("%.1f", ccr))
		current = append(current, executeFFTCase(count, level, node, ccr)...)
		w.Write(current)
		w.Flush()
	}
}


func createFFTWriter() (*csv.Writer, *os.File) {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	path := os.Getenv("fftStoragePath")

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

func TestGenerateFFTDAG(t *testing.T) {
	// CCR:   []float64{0.1, 0.5, 1, 5, 10, 20}

	config := createFFTConfig(7, 1000, 10)
	jobsDag := generateFFTDAG(config)
	fmt.Println(len(jobsDag.Vectors))

	// config = createFFTConfig(2, 100, 0.5)
	// generateFFTDAG(config)
	// config = createFFTConfig(2, 100, 1)
	// generateFFTDAG(config)
	// config = createFFTConfig(2, 100, 5)
	// generateFFTDAG(config)
	// config = createFFTConfig(2, 100, 10)
	// generateFFTDAG(config)
	// config = createFFTConfig(3, 100, 20)
	// generateFFTDAG(config)

	config.node=4
	nodes, bw := createRandNodeForFFT(config)
	c := createCustomAlgo(jobsDag.Vectors, nodes, bw)
	metric := c.simulate()
	speedup := calSpeedup(nodes, jobsDag.Vectors, metric.makespan)
	efficiency := speedup/float64(len(nodes))
	fmt.Printf("makespan: %.0f, speedup: %.3f, efficiency: %.3f\n", metric.makespan, speedup, efficiency)
}

func TestCustomAlgoInFFT(t *testing.T){
	// config := createFFTConfig(6, 100, 0.1)
	config := createFFTConfig(3, 75, 10)
	config.node=4
	nodes, bw := createRandNodeForFFT(config)
	jobsDag := generateFFTDAG(config)
	c := createCustomAlgo(jobsDag.Vectors, nodes, bw)
	metric := c.simulate()
	fmt.Printf("%.0f\n", metric.makespan)
}

func TestFFTSimulate(t *testing.T) {
	fft(2)
}

