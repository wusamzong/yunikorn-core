package objects

import (
	"github.com/apache/yunikorn-core/pkg/scheduler/objects/custom"
	// "github.com/apache/yunikorn-core/pkg/log"
	// "go.uber.org/zap"
	"strings"
	"os"
	"os/exec"
	"fmt"
	"time"
	"github.com/dominikbraun/graph"
	"github.com/dominikbraun/graph/draw"
	
)

const (
	TaskPodNameTag="kubernetes.io/meta/podName"
	TaskJobNameTag="kubernetes.io/label/job-name"
	TaskParentTag="kubernetes.io/label/parent"
	TaskDataTag="kubernetes.io/label/data"
)

type DAG struct{
	createPic bool
	head *Vector
	vectors map[string]*Vector
}

type Vector struct{
	instance *AllocationAsk
	children map[string]*Vector
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

func CreateDagManager(sa *Application, createPic bool) *DAG{
	dag := &DAG{
		createPic: createPic,
		vectors: map[string]*Vector{},
	}
	g := graph.New(graph.StringHash, graph.Directed(), graph.Acyclic())
	for _, request := range sa.requests{
		jobName:=request.tags[TaskJobNameTag]
		node:=&Vector{
			instance: request,
			children: map[string]*Vector{},
		}
		dag.vectors[jobName]=node
		if jobName=="task1"{
			dag.head = node
		}
		
		_ = g.AddVertex(jobName)
	}
	for jobName, vector := range dag.vectors{
		parents:=parseParent(vector.instance.tags[TaskParentTag])
		fmt.Println(jobName, parents)
		for _, parent := range parents{
			dag.vectors[parent].children[jobName]=vector
			
			_ = g.AddEdge(parent, jobName)
		}
	}
	if createPic{
		file, _ := os.Create("/tmp/simple.gv")
		_ = draw.DOT(g, file)
		time.Sleep(time.Second)
		cmd := exec.Command("dot","-Tsvg", "/tmp/simple.gv", "-o", "/tmp/simple.svg")
		if err := cmd.Run(); err != nil {
			fmt.Println(err)
			os.Exit(-1)
		}
	}
	return dag
}

func parseParent(p string)[]string{
	result:=[]string{}
	if p==""{
		return result
	}

	prefix:= "task"	
	result=strings.Split(p,"-")
	
	for idx, s := range result{
		result[idx]=prefix+s
	}
	return result
}

func (dag *DAG)optimized(){
	
}

