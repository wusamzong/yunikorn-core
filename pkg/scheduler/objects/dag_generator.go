package objects

import (
	"fmt"
	"math/rand"
	"sort"
)


const (
	minPerRank      = 3 // Nodes/Rank: How 'fat' the DAG should be.
	maxPerRank      = 3
	minRanks        = 5 // Ranks: How 'tall' the DAG should be.
	maxRanks        = 5
	percent         = 30 // Chance of having an Edge.
	filePath        = "dag02.yaml"
	appConfigPath   = "../workflow-config.yaml"
	JobTemplate     = "job-template.yaml"
	KwokPodTemplate = "kwok-pod-template.yaml"
)

type Edge struct {
	Idx    string `yaml:"idx"`
}

func generateRandomDAG()*JobsDAG{
	jobsDAG := &JobsDAG{
		Vectors: []*Job{},
	}
	ranks := minRanks + rand.Intn(maxRanks-minRanks+1)
	nodes := 0
	edges := []Edge{}
	createdJobs := map[int]bool{}
	// will storage parent of nodes
	DependencyStruct := map[int][]int{}

	// 1. create dependency
	fmt.Println("digraph {")
	for i := 0; i < ranks; i++ {
		// New nodes of 'higher' rank than all nodes generated till now.
		newNodes := minPerRank + rand.Intn(maxPerRank-minPerRank+1)

		// Edges from old nodes ('nodes') to new ones ('newNodes').
		for j := 0; j < nodes; j++ {
			for k := 0; k < newNodes; k++ {
				if rand.Intn(100) < percent {
					edge := Edge{
						Idx:    fmt.Sprintf("%d-%d", j, k+nodes),
					}
					edges = append(edges, edge)
					createdJobs[j]=true
					createdJobs[k+nodes]=true
					DependencyStruct[j] = append(DependencyStruct[j], k+nodes)
					fmt.Printf("  %d -> %d ;\n", j, k+nodes) // An Edge.
				}
			}
		}
		nodes += newNodes // Accumulate into old node set.
	}
	fmt.Println("}")

	// 2. format jobId
	tmpJobID:=[]int{}
	for key := range createdJobs{
		tmpJobID = append(tmpJobID, key)
	}
	sort.Ints(tmpJobID)
	formatedJobID:= map[int]int{}
	for idx, value := range tmpJobID{
		formatedJobID[value]=idx
	}
	fmt.Println(formatedJobID)

	// 3. create jobs by dependency
	for _, value := range formatedJobID {
		job := &Job{
			ID:         value,
			replicaNum: rand.Int()%7 + 1,
			replicaCpu: (rand.Int()%4 + 1) * 2 * 1000,
			replicaMem: (rand.Int()%4 + 1) * 2 * 1024,
			actionNum:  rand.Int()%7 + 1,
			parent:     []*Job{},
			children:   []*Job{},
		}
		createRandReplica(job)
		job.predictTime()
		jobsDAG.Vectors = append(jobsDAG.Vectors, job)
	}

	// 4. establish relationship for jobs
	for idx, children := range DependencyStruct {
		idx := formatedJobID[idx]
		for _, childID := range children{
			childID = formatedJobID[childID]
			vectors := jobsDAG.Vectors
			vectors[idx].children = append(vectors[idx].children, vectors[childID])
		}
	}
	jobsDAG = ChildToParent(jobsDAG)

	return jobsDAG
}

func ChildToParent(jobsDAG *JobsDAG) *JobsDAG {
	vectors := jobsDAG.Vectors
	for _, parent := range vectors {
		for _, child := range parent.children {
			child.parent = append(child.parent, parent)
		}
	}
	return jobsDAG
}

func createRandReplica(j *Job) {
	for i := 0; i < j.replicaNum; i++ {
		j.createReplica()
	}

	for i := 0; i < j.actionNum; i++ {
		randExecutionTime := rand.Float64() * 1000
		for _, r := range j.replicas {
			a := r.createAction(randExecutionTime)
			for _, r := range j.replicas {
				a.datasize[r] = rand.Float64() * 1000
			}
		}
	}

	for _, r := range j.replicas {
		for _, child := range j.children {
			r.finalDataSize[child] = rand.Float64() * 10000
		}

		// Log(fmt.Sprintf("replica:%d", i), r)
	}
}