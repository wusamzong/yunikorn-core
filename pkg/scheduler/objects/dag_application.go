package objects

import (
	"github.com/apache/yunikorn-core/pkg/scheduler/objects/custom"
	// "github.com/apache/yunikorn-core/pkg/log"
	// "go.uber.org/zap"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/dominikbraun/graph"
	"github.com/dominikbraun/graph/draw"
)

const (
	TaskPodNameTag       = "kubernetes.io/meta/podName"
	TaskJobNameTag       = "kubernetes.io/label/job-name"
	TaskChildrenTag      = "kubernetes.io/label/children"
	TaskExecutionTimeTag = "kubernetes.io/label/executionTime"
)

type DAG struct {
	createPic        bool
	head             *Vector
	vectors          map[string]*Vector
	OptimizeStrategy optimizeStrategy
}

type Vector struct {
	taskName      string
	instance      *AllocationAsk
	executionTime float64
	value		  float64
	children      map[string]*Vector
}

func isDagApp(sa *Application) bool {
	config := custom.LoadAppConfig()
	if sa.ApplicationID == config.ApplicationID {
		// log.Log(log.SchedApplication).Info("ture dag")
		return true
	}
	return false
}

func allRequestWaiting(sa *Application) bool {
	config := custom.LoadAppConfig()
	// log.Log(log.SchedApplication).Info("meet request count",
	// 	zap.Int("application requests",len(sa.requests)),
	// 	zap.Int("config",config.PodCount))

	if len(sa.requests) == config.PodCount {
		return true
	}
	return false
}

func CreateDagManager(sa *Application, createPic bool) *DAG {
	dag := &DAG{
		createPic: createPic,
		vectors:   map[string]*Vector{},
	}
	g := graph.New(graph.StringHash, graph.Directed(), graph.Acyclic())
	for _, request := range sa.requests {
		jobName := request.tags[TaskJobNameTag]
		node := &Vector{
			taskName: jobName,
			instance: request,
			children: map[string]*Vector{},
		}
		dag.vectors[jobName] = node
		if jobName == "task1" {
			dag.head = node
		}

		_ = g.AddVertex(jobName)
	}
	for jobName, vector := range dag.vectors {

		executionTime, _ := strconv.ParseFloat(vector.instance.tags[TaskExecutionTimeTag], 64)
		vector.executionTime = executionTime

		children := parseChildren(vector.instance.tags[TaskChildrenTag])
		// fmt.Println(jobName, children)
		for _, child := range children {
			vector.children[child] = dag.vectors[child]

			_ = g.AddEdge(jobName, child)
		}
	}
	if createPic {
		file, _ := os.Create("/tmp/simple.gv")
		_ = draw.DOT(g, file)
		time.Sleep(time.Second)
		cmd := exec.Command("dot", "-Tsvg", "/tmp/simple.gv", "-o", "/tmp/simple.svg")
		if err := cmd.Run(); err != nil {
			fmt.Println(err)
			os.Exit(-1)
		}
	}
	return dag
}

func parseChildren(c string) []string {
	result := []string{}
	if c == "" {
		return result
	}

	prefix := "task"
	result = strings.Split(c, "-")

	for idx, s := range result {
		result[idx] = prefix + s
	}
	return result
}

func (dag *DAG) optimized(o optimizeStrategy) {
	// heft := &HEFT{}
	// dag.OptimizeStrategy = heft
	// dag.OptimizeStrategy.optimized(dag.vectors)

	// drheft := &DRHEFT{}
	// dag.OptimizeStrategy = drheft
	// dag.OptimizeStrategy.optimized(dag.vectors)
}
