package objects

import (
	// "github.com/apache/yunikorn-core/pkg/scheduler/objects/custom"
	// // "github.com/apache/yunikorn-scheduler-interface/lib/go/si"
	"fmt"
	// "gotest.tools/v3/assert"
	// "strconv"
	"sync"
	"testing"
)

type fftTestCase struct {
	level []int
	CCR   []float64
	nodes []int
}

func TestDAGParallel(t *testing.T) {
	var wg sync.WaitGroup

	cases := fftTestCase{
		level: []int{3, 4, 5, 6}, 
		nodes: []int{4, 8, 16, 32},
		CCR:   []float64{0.1, 0.5, 1, 5, 10, 20},
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
	wg.Add(6)
	go comparisonFFTDAG(3, 4, 0.1, &wg)
	
	// go comparisonFFTDAG(6, 4, 0.5, &wg)
	// go comparisonFFTDAG(6, 4, 1, &wg)
	// go comparisonFFTDAG(6, 4, 5, &wg)
	// go comparisonFFTDAG(6, 4, 10, &wg)
	// go comparisonFFTDAG(6, 4, 20, &wg)
	wg.Wait()
}

func comparisonFFTDAG(level int, node int, ccr float64, wg *sync.WaitGroup) {
	w, file := createWriter()
	defer file.Close()
	defer w.Flush()
	defer wg.Done()

	w.Write([]string{"level", "nodeCount", "CCR", 
	"MPEFT", "MPEFTSLR","MPEFTspeedup","MPEFTefficiency", 
	"IPPTS", "IPPTSSLR","IPPTSspeedup","IPPTSefficiency",
	"HWS", "HWSSLR","HWSspeedup","HWSefficiency",})
	for count := 0; count<30; count++{
		current := []string{}
		current = append(current, fmt.Sprintf("%d", level))
		current = append(current, fmt.Sprintf("%d", node))
		current = append(current, fmt.Sprintf("%.1f", ccr))
		current = append(current, executeFFTCase(level, node, ccr)...)
		w.Write(current)
		w.Flush()
	}


}

func TestGenerateFFTDAG(t *testing.T) {
	// CCR:   []float64{0.1, 0.5, 1, 5, 10, 20}

	config := createFFTConfig(3, 100, 0.001)
	jobsDag := generateFFTDAG(config)
	

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

	config.node=2
	nodes, bw := createRandNodeForFFT(config)
	c := createCustomAlgo(jobsDag.Vectors, nodes, bw)
	metric := c.simulate()
	fmt.Printf("makespan: %.0f\n", metric.makespan)
}

func TestCustomAlgoInFFT(t *testing.T){
	// config := createFFTConfig(6, 100, 0.1)
	config := createFFTConfig(3, 100, 10)
	config.node=4
	nodes, bw := createRandNodeForFFT(config)
	jobsDag := generateFFTDAG(config)
	c := createCustomAlgo(jobsDag.Vectors, nodes, bw)
	metric := c.simulate()
	fmt.Printf("%.0f\n", metric.makespan)
}

func TestFFTSimulate(t *testing.T) {
	fft()
}

func TestFFTEdge(t *testing.T) {
	fftEdge(4)
}
