package objects

import (
	"fmt"
	"github.com/apache/yunikorn-core/pkg/scheduler/objects/custom"
	"strconv"
	"testing"
	"time"
	"github.com/apache/yunikorn-core/pkg/scheduler/policies"
	"github.com/apache/yunikorn-core/pkg/common/resources"
)

func CreateTestGraph() map[string]*Vector {
	// show execution time
	// start := time.Now()
	// defer fmt.Println(time.Now().Sub(start))

	config := custom.LoadTestAppConfig()
	app1 := newApplicationWithTags(config.ApplicationID, "default", "root.default", nil)
	dependency := []string{"2-3", "5-7", "6", "5", "6", "8", "8", "9-10", "", ""} // 10, 9, 8....1
	for i := 0; i < 10; i++ {
		saa := &AllocationAsk{
			tags: map[string]string{
				"kubernetes.io/label/job-name":      "task" + strconv.Itoa(i+1),
				"kubernetes.io/label/children":      dependency[i],
				"kubernetes.io/label/executionTime": "300",
			},
		}

		app1.requests["task"+strconv.Itoa(i+1)] = saa
	}
	app1.dag = CreateDagManager(app1, true)
	return app1.dag.vectors
}

func TestHEFTOptimized(t *testing.T) {

	v := map[string]*Vector{}
	h := &HEFT{}
	h.optimized(v)
}

func TestDRHEFTOptimized(t *testing.T) {
	v := map[string]*Vector{}
	d := &DRHEFT{}
	_ = CreateFakeNode()
	d.optimized(v)

}

func TestTryDRHEFT(t *testing.T){
	v := CreateTestGraph()
	// d := &DRHEFT{}
	iterator := CreateFakeNode().GetNodeIterator()
	isPassed := []*Vector{}

	for _, vector := range v{
		if isExist(isPassed, vector) {
			continue
		}
		TryNodes(vector, isPassed, iterator)
	}
}

func TryNodes(v *Vector, isPassed []*Vector, iterator NodeIterator){
	var minmum_result float64
	var selectNode string
	isPassed = append(isPassed, v)
	iterator.ForEachNode(func(node *Node) bool {
		// get dominant resource share
		res := v.instance.GetAllocatedResource()
		if !node.FitInNode(res) {
			return true
		}
		if !node.availableResource.FitInMaxUndef(res){
			return true
		}
		dr_share := calculateDrShare(node.GetAvailableResource() , res)
		// get execution time
		executionTime_value:=v.executionTime*nodeComputingSpeed[node.NodeID]

		if len(v.children)!=0 {
			var maximum_transmission_cost float64
			maximum_transmission_cost = 0
			for _, child := range v.children {
				if isExist(isPassed, child) {
					continue
				}else{
					calculateChild(child, isPassed)
				}
				node_from := node.NodeID
				node_to := child.instance.GetRequiredNode()
				bandWidth:=nodeBandWidth.links[node_from][node_to]

				task_from := v.instance.taskName
				task_to := child.instance.taskName

				dataSize:=GetEdgeDataSize(LoadTestAppConfig(), task_from, task_to)

				transmission_cost := bandWidth/dataSize
				if maximum_transmission_cost < transmission_cost{
					maximum_transmission_cost = transmission_cost
				}
			}
			executionTime_value += maximum_transmission_cost
		}

		value := dr_share * executionTime_value
		if minmum_result > value{
			minmum_result = value
			selectNode = node.NodeID
		}
		return false
	})
	v.value=minmum_result
	v.instance.SetRequiredNode(selectNode)
}

func calculateDrShare(nodeResource ,askRes *resources.Resource) float64{
	var dominantResource float64
	dominantResource = 0
	for k, v := range askRes.Resources {
		largerValue := nodeResource.Resources[k]
		// skip if not defined (queue quota checks: undefined resources are considered max)
		share:=float64(v)/float64(largerValue)
		if share > dominantResource{
			dominantResource = share
		}
	}
	return dominantResource
}

var (
	count = 0
)

func TestGraphRetrieval(t *testing.T) {
	vectors := CreateTestGraph()
	isPassed := []*Vector{}
	start := time.Now()
	defer fmt.Println(time.Now().Sub(start))
	for _, vector := range vectors {
		if isExist(isPassed, vector) {
			continue
		}
		isPassed = append(isPassed, vector)
		calculateChild(vector, isPassed)
	}
	fmt.Println(count)
}

func calculateChild(v *Vector, isPassed []*Vector) {
	count++

	if len(v.children) == 0 {
		return
	}

	for _, child := range v.children {
		if isExist(isPassed, child) {
			continue
		}
		isPassed = append(isPassed, child)
		calculateChild(child, isPassed)
	}
}

func isExist(v []*Vector, cur *Vector) bool {
	for _, child := range v {
		if child == cur {
			return true
		}
	}
	return false
}

func CreateFakeNode() NodeCollection{
	nc := NewNodeCollection("test")

	fixResource:=map[string][]int64{
		"worker1": []int64{1000,16*1024*1024*1024},
		"worker2": []int64{2000,8*1024*1024*1024},
		"worker3": []int64{4000,4*1024*1024*1024},
		"worker4": []int64{8000,2*1024*1024*1024},
		"worker5": []int64{16000,1*1024*1024*1024},
	}

	// Initialization of nodes and application
	for i := 1; i <= 5; i++ {
		nodeName := fmt.Sprintf("worker%d", i)
		cpu:=fixResource[nodeName][0]
		memory:=fixResource[nodeName][1]
		node := newNode(nodeName, map[string]resources.Quantity{"vcore": resources.Quantity(cpu), "memory":resources.Quantity(memory)})

		if err := nc.AddNode(node); err != nil {
			fmt.Println("Adding another node into BC failed.")
		}
	}

	// Fair policy
	nc.SetNodeSortingPolicy(NewNodeSortingPolicy(policies.FairnessPolicy.String(), nil))
	iter := nc.GetNodeIterator()

	iter.ForEachNode(func(node *Node) bool {
		fmt.Println(node.NodeID)
		return true
	})
	return nc
}

