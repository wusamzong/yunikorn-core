package objects

import (
	"fmt"
	"math"
	"math/rand"
)

type edge struct {
	from [2]int
	to   [2]int
}

type fftConfig struct {
	level         int
	totalCost     float64
	tasksPerLevel int
	ccr           float64
	node          int
}

func createFFTConfig(level int, totalCost float64, ccr float64) fftConfig {
	tasksPerLevel := int(math.Pow(2.0, float64(level)))
	return fftConfig{
		level:         level,
		totalCost:     totalCost,
		tasksPerLevel: tasksPerLevel,
		ccr:           ccr,
	}
}

func fft(level int) [][2]int {
	// edges := []edge{}
	result := [][2]int{}
	tasksPerLevel := int(math.Pow(2.0, float64(level)))

	for i := 1; i <= int(level); i++ {
		taskNumberOfCurrentJob := int(math.Pow(2.0, float64(i)))
		jobNumber := tasksPerLevel / taskNumberOfCurrentJob
		// fmt.Println("level", i)
		for j := 0; j < jobNumber; j++ {
			// fmt.Println(" Job", j)
			for k := 0; k < taskNumberOfCurrentJob; k++ {

				// fmt.Println("  task:", j+k)

				// currentLevelFirstID := i*tasksPerLevel
				currentJobFirstID := (i - 1) * taskNumberOfCurrentJob
				// anchor := currentJobFirstID + currentLevelFirstID
				from := currentJobFirstID + k
				to := currentJobFirstID + (k+i)%taskNumberOfCurrentJob

				// newEdge1 := edge{
				// 	from: [2]int{i, from},
				// 	to:   [2]int{i + 1, from},
				// }
				// newEdge2 := edge{
				// 	from: [2]int{i, from},
				// 	to:   [2]int{i + 1, to},
				// }

				// edges = append(edges, newEdge1)
				// edges = append(edges, newEdge2)

				newEdge1 := [2]int{from, from + tasksPerLevel}
				newEdge2 := [2]int{from, to + tasksPerLevel}

				result = append(result, newEdge1)
				result = append(result, newEdge2)
				// fmt.Println("   ", newEdge1)
				// fmt.Println("   ", newEdge2)

			}
		}
		// fmt.Println()
	}
	return result
}

func generateFFTDAG(config fftConfig) *JobsDAG {
	jobsDAG := &JobsDAG{
		Vectors:       []*Job{},
		replicasCount: 0,
	}

	level := config.level
	tasksPerLevel := int(math.Pow(2.0, float64(level)))
	jobID := 0
	replicaID := 0
	replicaIDToJob := map[int]*Job{}
	for i := 0; i <= int(level); i++ {
		for k := 0; k < tasksPerLevel; k++ {
			job := &Job{
				ID:           jobID,
				replicaNum:   1,
				replicaCpu:   500,
				replicaMem:   512,
				cpuIntensive: rand.Float64() * 1.2,
				memIntensive: rand.Float64() * 1.2,
				actionNum:    1,
				parent:       []*Job{},
				children:     []*Job{},
				finish:       0,
			}
			replicaIDToJob[replicaID] = job
			job.createFFTSimpleReplicaByCCR(replicaID, config)
			replicaID++
			jobsDAG.replicasCount += 1
			job.predictExecutionTime = job.predictTime(0.0)
			jobsDAG.Vectors = append(jobsDAG.Vectors, job)
			jobID++
		}

	}
	edges := fft(config.level)

	// for _, job := range jobsDAG.Vectors {
	// 	fmt.Println("JobID:", job.ID)
	// 	for _, replica := range job.replicas {
	// 		fmt.Println(" ReplicaID:", replica.ID)
	// 	}
	// }
	// fmt.Println(replicaIDToJob)

	for _, e := range edges {

		from := replicaIDToJob[e[0]]
		to := replicaIDToJob[e[1]]
		if from == to {
			continue
		}
		from.children = append(from.children, to)
	}

	jobsDAG = ChildToParent(jobsDAG)

	for _, j := range jobsDAG.Vectors {
		childrenReplicas := j.getChildrenReplica()
		parentReplicas := j.getParentReplica()
		for _, r := range j.replicas {
			r.children = childrenReplicas
			r.parent = parentReplicas

			for _, child := range j.children {
				cost := config.totalCost
				r.finalDataSize[child] = cost / 2 * config.ccr
			}
		}
	}

	return jobsDAG
}

func (j *Job) createFFTReplicaByCCR(levelID int, jobID int, replicaID int, config fftConfig) {
	taskNumberOfCurrentJob := j.replicaNum
	cost := config.totalCost / float64(config.tasksPerLevel)

	for i := 0; i < taskNumberOfCurrentJob; i++ {
		r := j.createReplica()
		r.ID = replicaID + i*2
	}

	for i := 0; i < j.actionNum; i++ {
		executionTime := config.totalCost
		for _, r := range j.replicas {
			a := r.createAction(executionTime)
			to := jobID*taskNumberOfCurrentJob + (r.ID+levelID)%taskNumberOfCurrentJob
			for _, child := range j.replicas {
				if r.ID == to || r.ID == child.ID {
					a.datasize[child] = cost / 2 * config.ccr
				} else {
					a.datasize[child] = 0.0
				}
			}
		}
	}
}

func (j *Job) createFFTSimpleReplicaByCCR(replicaID int, config fftConfig) {
	cost := config.totalCost
	r := j.createReplica()
	r.ID = replicaID
	a := r.createAction(cost)
	a.datasize[r] = 0.0
}

func executeFFTCase(count int, level int, node int, ccr float64) []string {
	config := createFFTConfig(level, rand.Float64()*50+50, ccr)
	config.node = node
	
	current := []string{}
	for algoCount := 0; algoCount < 4; algoCount++ {
		rand.Seed(int64(count))
		nodes, bw := createRandNodeForFFT(config)
		jobsDag := generateFFTDAG(config)

		var metric metric
		if algoCount == 0 {
			// continue
			jobsWithOnlyReplica(jobsDag.Vectors)
			m := createMPEFT(jobsDag.Vectors, nodes, bw)
			metric = m.simulate()
			current = append(current, fmt.Sprintf("%.0f", metric.makespan))
			current = append(current, fmt.Sprintf("%.3f", metric.SLR))
		} else if algoCount == 1 {
			// continue
			jobsWithOnlyReplica(jobsDag.Vectors)
			p := createIPPTS(jobsDag.Vectors, nodes, bw)
			metric = p.simulate()
			current = append(current, fmt.Sprintf("%.0f", metric.makespan))
			current = append(current, fmt.Sprintf("%.3f", metric.SLR))
		} else if algoCount == 2 {
			c := createCustomAlgo(jobsDag.Vectors, nodes, bw)
			metric = c.simulate()
			current = append(current, fmt.Sprintf("%.0f", metric.makespan))
			current = append(current, fmt.Sprintf("%.3f", metric.SLR))
		} else {
			jobsWithOnlyReplica(jobsDag.Vectors)
			a := createMacro(jobsDag.Vectors, nodes, bw)
			metric = a.simulate()
			current = append(current, fmt.Sprintf("%.0f", metric.makespan))
			current = append(current, fmt.Sprintf("%.3f", metric.SLR))
		}
		rand.Seed(int64(count))
		nodes, bw = createRandNodeForFFT(config)
		jobsDag = generateFFTDAG(config)
		speedup := calSpeedup(nodes, jobsDag.Vectors, metric.makespan)
		efficiency := speedup/float64(len(nodes))

		current = append(current, fmt.Sprintf("%.3f", speedup))
		current = append(current, fmt.Sprintf("%.3f", efficiency))

	}
	return current
}

func createRandNodeForFFT(config fftConfig) ([]*node, *bandwidth) {
	nodeCount := config.node + 1
	nodes := []*node{}
	bw := &bandwidth{
		values: map[*node]map[*node]float64{},
	}

	for i := 0; i < nodeCount; i++ {
		// resource := rand.Intn(int(math.Pow(2.0, float64(config.level)-2)))+4
		resource := 8
		n := &node{
			ID:            i,
			cpu:           resource * 500,
			mem:           resource * 512,
			allocatedCpu:  0,
			allocatedMem:  0,
			executionRate: 1 + rand.Float64()*1.5,
		}
		nodes = append(nodes, n)
	}

	for i := 0; i < nodeCount; i++ {
		from := nodes[i]
		if _, exist := bw.values[from]; !exist {
			bw.values[from] = map[*node]float64{}
		}
		for j := i; j < nodeCount; j++ {
			to := nodes[j]
			if _, exist := bw.values[to]; !exist {
				bw.values[to] = map[*node]float64{}
			}
			var randBandwidth float64
			if i == j {
				randBandwidth = 0
			} else {
				randBandwidth = 1 + rand.Float64()

			}

			bw.values[from][to] = randBandwidth
			bw.values[to][from] = randBandwidth
		}
	}
	return nodes, bw
}
