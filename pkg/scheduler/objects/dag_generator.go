package objects

import (
	"fmt"
	"math/rand"
	"sort"
)

const (
	minPerRank      = 5 // Nodes/Rank: How 'fat' the DAG should be.
	maxPerRank      = 5
	minRanks        = 10 // Ranks: How 'tall' the DAG should be.
	maxRanks        = 10
	percent         = 20 // Chance of having an Edge.
	filePath        = "dag02.yaml"
	appConfigPath   = "../workflow-config.yaml"
	JobTemplate     = "job-template.yaml"
	KwokPodTemplate = "kwok-pod-template.yaml"
)

type comparisonConfig struct {
	podCount   int
	times      int64
	randomSeed int
	// about DAG
	alpha      float64
	width      int
	minPerRank int
	maxPerRank int
	minRanks   int
	maxRanks   int
	percent    int
	// about Job
	replicaNum      int
	replicaCPURange int
	replicaMemRange int
	actionNum       int
	// about node
	nodeCount             int
	nodeCPURange          int
	nodeMemRange          int
	ccr                   float64
	rrc                   float64
	tcr                   float64
	speedHeterogeneity    float64
	resourceHeterogeneity float64
	averageNodeResource   float64
}

type Edge struct {
	Idx string `yaml:"idx"`
}

func generateRandomDAG() *JobsDAG {
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
	// fmt.Println("digraph {")
	for i := 0; i < ranks; i++ {
		// New nodes of 'higher' rank than all nodes generated till now.
		newNodes := minPerRank + rand.Intn(maxPerRank-minPerRank+1)
		// Edges from old nodes ('nodes') to new ones ('newNodes').
		for j := 0; j < nodes; j++ {
			for k := 0; k < newNodes; k++ {
				if rand.Intn(100) < percent {
					edge := Edge{
						Idx: fmt.Sprintf("%d-%d", j, k+nodes),
					}
					edges = append(edges, edge)
					createdJobs[j] = true
					createdJobs[k+nodes] = true
					DependencyStruct[j] = append(DependencyStruct[j], k+nodes)
					// fmt.Printf("  %d -> %d ;\n", j, k+nodes) // An Edge.
				}
			}
		}
		nodes += newNodes // Accumulate into old node set.
	}
	// fmt.Println("}")

	// 2. format jobId
	tmpJobID := []int{}
	for key := range createdJobs {
		tmpJobID = append(tmpJobID, key)
	}
	sort.Ints(tmpJobID)
	formatedJobID := map[int]int{}
	for idx, value := range tmpJobID {
		formatedJobID[value] = idx
	}

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
			finish:     0,
		}
		createRandReplica(job)
		job.predictExecutionTime = job.predictTime(0.0)
		jobsDAG.Vectors = append(jobsDAG.Vectors, job)
	}

	// 4. establish relationship for jobs
	for idx, children := range DependencyStruct {
		idx := formatedJobID[idx]
		for _, childID := range children {
			childID = formatedJobID[childID]
			vectors := jobsDAG.Vectors
			vectors[idx].children = append(vectors[idx].children, vectors[childID])
		}
	}
	jobsDAG = ChildToParent(jobsDAG)

	// 5. create relationship between replicas
	for _, j := range jobsDAG.Vectors {
		childrenReplicas := j.getChildrenReplica()
		parentReplicas := j.getParentReplica()
		for _, r := range j.replicas {
			r.children = childrenReplicas
			r.parent = parentReplicas
		}
	}
	return jobsDAG
}

func generateRandomDAGWithConfig(config comparisonConfig) *JobsDAG {
	jobsDAG := &JobsDAG{
		Vectors:       []*Job{},
		replicasCount: 0,
	}

	nodes := 0
	edges := []Edge{}
	createdJobs := map[int]bool{}
	// will storage parent of nodes
	DependencyStruct := map[int][]int{}

	// 1. create dependency
	// fmt.Println("digraph {")
	for len(createdJobs)*config.replicaNum < config.podCount {
		// New nodes of 'higher' rank than all nodes generated till now.
		newNodes := config.width
		// fmt.Println("add number of nodes: ", newNodes)
		// fmt.Println(newNodes+nodes, config.podCount)

		// Edges from old nodes ('nodes') to new ones ('newNodes').
		for j := 0; j < nodes; j++ {
			for k := 0; k < newNodes; k++ {
				if len(createdJobs)*config.replicaNum >= config.podCount {
					break
				}
				if rand.Intn(100) <= config.percent {
					edge := Edge{
						Idx: fmt.Sprintf("%d-%d", j, k+nodes),
					}
					edges = append(edges, edge)
					createdJobs[j] = true
					createdJobs[k+nodes] = true
					DependencyStruct[j] = append(DependencyStruct[j], k+nodes)
					// fmt.Printf("  %d -> %d ;\n", j, k+nodes) // An Edge.
				}
			}
		}
		nodes += newNodes // Accumulate into old node set.
	}
	// fmt.Println("}")
	// 2. format jobId
	tmpJobID := []int{}
	for key := range createdJobs {
		tmpJobID = append(tmpJobID, key)
	}
	sort.Ints(tmpJobID)

	formatedJobID := map[int]int{}
	for idx, value := range tmpJobID {
		formatedJobID[value] = idx
	}
	// fmt.Println(formatedJobID)
	// 3. create jobs by dependency
	for _, value := range formatedJobID {
		replicaNum := config.replicaNum
		// fmt.Println(replicaNum)
		replicaCpu := config.replicaCPURange * 500
		replicaMem := config.replicaMemRange * 512
		job := &Job{
			ID:           value,
			replicaNum:   replicaNum,
			replicaCpu:   replicaCpu,
			replicaMem:   replicaMem,
			cpuIntensive: rand.Float64()*1.2,
			memIntensive: rand.Float64()*1.2,
			actionNum:    config.actionNum,
			parent:       []*Job{},
			children:     []*Job{},
			finish:       0,
		}
		createRandReplicaByCCR(job, config.ccr)
		jobsDAG.replicasCount += replicaNum
		job.predictExecutionTime = job.predictTime(0.0)
		jobsDAG.Vectors = append(jobsDAG.Vectors, job)
	}
	// 4. establish relationship for jobs
	for idx, children := range DependencyStruct {
		idx := formatedJobID[idx]
		for _, childID := range children {
			childID = formatedJobID[childID]
			vectors := jobsDAG.Vectors
			vectors[idx].children = append(vectors[idx].children, vectors[childID])
		}
	}
	jobsDAG = ChildToParent(jobsDAG)
	// 5. create relationship between replicas
	for _, j := range jobsDAG.Vectors {
		childrenReplicas := j.getChildrenReplica()
		parentReplicas := j.getParentReplica()
		for _, r := range j.replicas {
			r.children = childrenReplicas
			r.parent = parentReplicas

			for _, child := range j.children {
				r.finalDataSize[child] = rand.Float64() * 75 * config.ccr * config.tcr / float64(j.replicaNum)
			}

		}
	}
	// fmt.Println(jobsDAG.replicasCount)
	return jobsDAG
}

func simulateGenerateRandomDAGWithConfig(config comparisonConfig) *JobsDAG {
	jobsDAG := &JobsDAG{
		Vectors:       []*Job{},
		replicasCount: 0,
	}
	ranks := config.minRanks + rand.Intn(config.maxRanks-config.minRanks+1)
	nodes := 0
	edges := []Edge{}
	createdJobs := map[int]bool{}
	// will storage parent of nodes
	DependencyStruct := map[int][]int{}

	// 1. create dependency
	// fmt.Println("digraph {")
	for i := 0; i < ranks; i++ {
		// New nodes of 'higher' rank than all nodes generated till now.
		newNodes := config.minPerRank + rand.Intn(config.maxPerRank-config.minPerRank+1)

		// Edges from old nodes ('nodes') to new ones ('newNodes').
		for j := 0; j < nodes; j++ {
			for k := 0; k < newNodes; k++ {
				if rand.Intn(100) < config.percent {
					edge := Edge{
						Idx: fmt.Sprintf("%d-%d", j, k+nodes),
					}
					edges = append(edges, edge)
					createdJobs[j] = true
					createdJobs[k+nodes] = true
					DependencyStruct[j] = append(DependencyStruct[j], k+nodes)
					// fmt.Printf("  %d -> %d ;\n", j, k+nodes) // An Edge.
				}
			}
		}
		nodes += newNodes // Accumulate into old node set.
	}
	// fmt.Println("}")

	// 2. format jobId
	tmpJobID := []int{}
	for key := range createdJobs {
		tmpJobID = append(tmpJobID, key)
	}
	// sort.Ints(tmpJobID)
	formatedJobID := map[int]int{}
	for idx, value := range tmpJobID {
		formatedJobID[value] = idx
	}
	// fmt.Println(formatedJobID)

	// 3. create jobs by dependency
	for _, value := range formatedJobID {
		replicaNum := rand.Int()%config.replicaNum + 1
		// fmt.Println(replicaNum)
		replicaCpu := (rand.Int()%config.replicaCPURange + 1) * 2 * 1000
		replicaMem := (rand.Int()%config.replicaMemRange + 1) * 2 * 1024
		job := &Job{
			ID:         value,
			replicaNum: replicaNum,
			replicaCpu: replicaCpu,
			replicaMem: replicaMem,
			actionNum:  rand.Int()%config.actionNum + 1,
			parent:     []*Job{},
			children:   []*Job{},
			finish:     0,
		}
		createRandReplica(job)
		jobsDAG.replicasCount += replicaNum
		// job.predictExecutionTime = job.predictTime(0.0)
		// jobsDAG.Vectors = append(jobsDAG.Vectors, job)
	}

	// 4. establish relationship for jobs
	// for idx, children := range DependencyStruct {
	// 	idx := formatedJobID[idx]
	// 	for _, childID := range children {
	// 		childID = formatedJobID[childID]
	// 		vectors := jobsDAG.Vectors
	// 		vectors[idx].children = append(vectors[idx].children, vectors[childID])
	// 	}
	// }
	// jobsDAG = ChildToParent(jobsDAG)

	// 5. create relationship between replicas
	// for _, j := range jobsDAG.Vectors {
	// 	childrenReplicas := j.getChildrenReplica()
	// 	parentReplicas := j.getParentReplica()
	// 	for _, r := range j.replicas {
	// 		r.children = childrenReplicas
	// 		r.parent = parentReplicas
	// 	}
	// }
	// fmt.Println(jobsDAG.replicasCount)
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
	}
}

func createRandReplicaByCCR(j *Job, ccr float64) {
	for i := 0; i < j.replicaNum; i++ {
		j.createReplica()
	}

	for i := 0; i < j.actionNum; i++ {
		randExecutionTime := rand.Float64() * 75
		for _, r := range j.replicas {
			a := r.createAction(randExecutionTime)
			for _, r := range j.replicas {
				a.datasize[r] = rand.Float64() * 75 * ccr /float64(j.replicaNum)
			}
		}
	}
}

func getCriticalPath(jobs []*Job)[]*Job{
	max := 0.0
	head := []*Job{}
	for _,j:=range jobs{
		if len(j.parent)==0{
			head = append(head, j)
		}
	}

	criticalPath := []*Job{}
	currentPath := []*Job{}
	var backtracking func(current float64, job *Job)

	backtracking = func(current float64, job *Job) {
		for _, child := range job.children{
			executionTime := child.predictTime(1.0)
			currentPath = append(currentPath, child)
			current += executionTime
			backtracking(current, child)
			if current > max && len(child.children)==0{
				max = current
				criticalPath = make([]*Job, len(currentPath))
				copy(criticalPath, currentPath)
			}
			currentPath = currentPath[:len(currentPath)-1]
			current -= executionTime
		}
	}

	for _, h := range head{
		max=0.0
		currentPath = []*Job{}
		currentPath = append(currentPath, h)
		backtracking(0, h)
	}
	return criticalPath
}