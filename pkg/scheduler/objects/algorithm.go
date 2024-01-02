package objects

// import (
// 	"fmt"
// )

type optimizeStrategy interface {
	optimized(j *JobsDAG) error
}

type HEFT struct {
}

type DRHEFT struct {
}

type NodeBandWidth struct {
	links map[string]map[string]float64
}

var (
	nodeBandWidth = NodeBandWidth{
		links: map[string]map[string]float64{
			"worker1": map[string]float64{
				"worker2": 60.0,
				"worker3": 40.0,
				"worker4": 90.0,
				"worker5": 70.0,
			},
			"worker2": map[string]float64{
				"worker1": 50.0,
				"worker3": 40.0,
				"worker4": 30.0,
				"worker5": 40.0,
			},
			"worker3": map[string]float64{
				"worker1": 50.0,
				"worker2": 70.0,
				"worker4": 90.0,
				"worker5": 70.0,
			},
			"worker4": map[string]float64{
				"worker1": 50.0,
				"worker2": 70.0,
				"worker3": 60.0,
				"worker5": 50.0,
			},
			"worker5": map[string]float64{
				"worker1": 50.0,
				"worker2": 70.0,
				"worker3": 60.0,
				"worker4": 90.0,
			},
		},
	}
	nodeComputingSpeed = map[string]float64{
		"worker1": 1.0,
		"worker2": 1.2,
		"worker3": 1.4,
		"worker4": 1.6,
		"worker5": 1.8,
	}
)

func (a *HEFT) optimized(j *JobsDAG) error {

	return nil
}

func (a *DRHEFT) optimized(j *JobsDAG) error {

	return nil
}

// func tryNode(v *Vector) float64{
// 	if len(v.children)==0{
// 		iterator := nodeIterator()			
// 		iterator.ForEachNode(func(node *Node) bool {
//      })
// 	}
// }

func graphRetrieval(vectors map[string]*Vector) error {

	return nil
}
