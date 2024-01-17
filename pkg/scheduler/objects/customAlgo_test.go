package objects

import (
	// "fmt"
	"os"
	"testing"
	"math/rand"
)

func TestSimulateCustom(t *testing.T) {
	
	var i int64
	for i=0;i< 1;i++{
		rand.Seed(i)
		nodes, bw := createRandNode()
		// jobsDag := createStaticJobDAG()
		jobsDag := generateRandomDAG()

		c := createCustomAlgo(jobsDag.Vectors, nodes, bw)
		makespan, resourceUsage:=c.simulate()
		if makespan==0.0 || resourceUsage==0.0{
			os.Exit(-1)
		}
	}
	
}
