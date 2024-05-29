package objects

import (
	"fmt"
	"math"
	"sort"
	"time"
	"math/rand"
)

type JobsDAG struct {
	Vectors []*Job
	replicasCount int
}

type Job struct {
	ID                   int
	replicaNum           int
	replicaCpu           int
	replicaMem           int
	cpuIntensive         float64
	memIntensive		 float64
	actionNum            int
	predictExecutionTime float64
	pathPriority         float64
	makespan             float64
	// receiveTime          float64
	finish               int
	replicas             []*replica
	children             []*Job
	parent               []*Job
}

type replica struct {
	ID            int
	job           *Job
	node          *node
	actions       []*action
	finalDataSize map[*Job]float64
	children      []*replica
	parent        []*replica
	finish        bool
	// node selected result
	minTime  float64
	minDr    float64
	minValue float64
}

type action struct {
	ID            int
	executionTime float64
	datasize      map[*replica]float64
}

type cell struct {
	currentNode   node
	executionTime float64
	datasize      map[*replica]float64
}

type node struct {
	ID            int
	cpu           int
	mem           int
	allocatedCpu  int
	allocatedMem  int
	executionRate float64
}

type bandwidth struct {
	values map[*node]map[*node]float64
}

func (dag *JobsDAG) sort(){
	sort.Slice(dag.Vectors, func(i, j int) bool{
		return dag.Vectors[i].ID<dag.Vectors[j].ID
	})
}

func (dag *JobsDAG) getAllReplicas() []*replica{
	dag.sort()
	result:= []*replica{}
	for _, job := range dag.Vectors{
		for _, replica := range job.replicas{
			result = append(result, replica)
		}
	}
	return result
}

func calSLR(nodes []*node, criticalPath []*Job, makespan float64) float64{
	
	sum := 0.0
	
	for _, job := range criticalPath{
		min:= math.MaxFloat64
		for _, node := range nodes{
			replicaExecutionTime := 0.0
			for _, replica := range job.replicas{
				for _, action := range replica.actions{
					// median of dynamic model is 3.294732
					replicaExecutionTime += action.executionTime/(node.executionRate/3.294732)
				}
			}
			if min > replicaExecutionTime{
				min = replicaExecutionTime
			}

		}
		sum+=min
	}
	if sum==0.0{
		return 0.0
	}

	return makespan/sum
}

func calSpeedup(nodes []*node, jobs []*Job, makespan float64) float64{
	result := math.MaxFloat64
	for _, node := range nodes{
		sum := 0.0


		for _, job := range jobs{
			concurrentCPU:= node.cpu/job.replicaCpu
			concurrentMem:= node.mem/job.replicaMem
			concurrent := 0
			if concurrentCPU > concurrentMem{
				concurrent = concurrentMem
			}else{
				concurrent = concurrentCPU
			}

			for _, replica := range job.replicas{
				for _, action := range replica.actions{
					sum += action.executionTime/(node.executionRate/3.294732)/float64(concurrent)
				}
			}
		}
		if sum < result{
			result = sum
		}
	}
	return result/makespan
}

func (j *Job) createReplica() *replica {
	r := &replica{
		ID:            len(j.replicas),
		job:           j,
		node:          nil,
		actions:       []*action{},
		finalDataSize: map[*Job]float64{},
		children:      []*replica{},
		parent:        []*replica{},
	}
	j.replicas = append(j.replicas, r)
	return r
}

func (job *Job) allParentScheduled(scheduledJob map[*Job]bool) bool {
	for _, parent := range job.parent {
		if scheduledJob[parent] == false {
			return false
		}
	}
	return true
}

func (job *Job) allParentDone() bool {
	if len(job.parent)==0{
		return true
	}
	for _, parent := range job.parent {
		if parent.finish != parent.replicaNum {
			return false
		}
	}
	return true
}

func (job *Job) oneParentReplicaDone()bool{
	for _, parent := range job.parent {
		if parent.finish < 1 {
			return false
		}
	}
	return true
}

func (replica *replica) allParentScheduled(scheduledReplica map[*replica]bool) bool {
	for _, parent := range replica.parent {
		if scheduledReplica[parent] == false {
			return false
		}
	}
	return true
}

func (job *Job) decideNode(nodes []*node, bw *bandwidth) bool {
	job.makespan = 0
	doneReplica := []*replica{}
	// availableTime:= map[*node]float64{}
	for idx, replica := range job.replicas {
		replica.node=nil
		replica.minValue = math.MaxFloat64
		
		for _, node := range nodes {
			var currentJobCpuUsage int 
			var currentJobMemUsage int	
			for _, r := range doneReplica{
				if r.node==node{
					currentJobCpuUsage+=job.replicaCpu
					currentJobMemUsage+=job.replicaMem
				}
			}
			if node.cpu-node.allocatedCpu-currentJobCpuUsage < job.replicaCpu || node.mem-node.allocatedMem-currentJobMemUsage < job.replicaMem {
				continue
			}
			var time float64
			cpuUsage := float64(currentJobCpuUsage+node.allocatedCpu)/float64(node.cpu)
			memUsage := float64(currentJobMemUsage+node.allocatedMem)/float64(node.mem)
			// transmission time + Execution time "Inside" the Job
			for _, action := range replica.actions {
				var transmissionTime, executionTime float64
				executionTime = action.executionTime/dynamicExecutionModel(node.executionRate, cpuUsage, memUsage, job)
				time += executionTime
				transmissionTime = 0
				if idx != 0 {
					for i := 0; i < idx; i++ {
						from := node
						to := job.replicas[i].node
						datasize := action.datasize[job.replicas[i]]
						var curTransmissionTime float64
						if bw.values[from][to] == 0 {
							curTransmissionTime = 0
						} else {
							curTransmissionTime = datasize / bw.values[from][to]
						}

						if transmissionTime < curTransmissionTime {
							transmissionTime = curTransmissionTime
						}
					}
					time+=transmissionTime
				}
			}
			


			// transmission time "between" the Jobs
			var transmissionTime float64
			transmissionTime = 0
			for _, parent := range job.parent {
				for _, parentReplica := range parent.replicas {
					from := parentReplica.node
					to := node
					datasize := parentReplica.finalDataSize[job]
					var curTransmissionTime float64
					if bw.values[from][to] == 0 {
						curTransmissionTime = 0
					} else {
						curTransmissionTime = datasize / bw.values[from][to]
					}

					if transmissionTime < curTransmissionTime {
						transmissionTime = curTransmissionTime
					}
				}
			}
			
			time += transmissionTime
			// DR of replica on node
			nodeCapacityVector := []float64{
				float64(node.cpu) / float64(node.cpu+node.mem),
				float64(node.mem) / float64(node.cpu+node.mem),
			}
			requestVector := []float64{
				float64(job.replicaCpu) / float64(node.cpu),
				float64(job.replicaMem) / float64(node.mem),
			}
			resourceShare := []float64{
				requestVector[0] / nodeCapacityVector[0],
				requestVector[1] / nodeCapacityVector[1],
			}
			var dr float64
			if resourceShare[0] > resourceShare[1] {
				dr = resourceShare[0]
			} else {
				dr = resourceShare[1]
			}
			// fmt.Printf("Job: %d, replica: %d, nodeID:, %d, value: %.1f\n", job.ID, idx, node.ID, time)
			if time < replica.minValue {
				replica.minTime = time
				replica.minDr = dr
				replica.minValue = time
				replica.node = node
			}
		}
		
		if replica.node == nil {
			job.reset()
			return false
		}else{
			// fmt.Printf("Job: %d, replica: %d, select nodeID: %d\n", job.ID, idx, replica.node.ID)
			doneReplica = append(doneReplica, replica)
		}
	}
	

	var time float64
	// maxReceive:=0.0
	for _, r :=range job.replicas{
		maxTime:=0.0	
		for _, a := range r.actions{
			var transmissionTime, executionTime float64
			executionTime = a.executionTime/r.node.executionRate
			transmissionTime = 0.0
			maxTransmissionTime:=0.0
			for _, child:=range r.children{
				from:=r.node
				to:= child.node
				datasize := a.datasize[child]
				if from == to{
					transmissionTime=0.0
				}else{
					transmissionTime=datasize/bw.values[from][to]
				}
				if transmissionTime>maxTransmissionTime{
					maxTransmissionTime=transmissionTime
				}
			}
			if maxTransmissionTime+executionTime>maxTime{
				maxTime=maxTransmissionTime+executionTime
			}
		}
		time+=maxTime
	}
	job.makespan=time
	// job.receiveTime=maxReceive
	// for idx, replica := range job.replicas {
	// 	fmt.Println("Job", job.ID, ",replica", idx, ",nodeID:", replica.node.ID,
	// 		",minTime:", replica.minTime, ",min DR:", replica.minDr, ",minValue:", replica.minValue)
	// 	fmt.Printf("Job: %d, replica: %d, nodeID:, %d, minTime: %.1f, minDR: %.1f, minValue: %.1f\n", job.ID, idx, replica.node.ID, replica.minTime, replica.minDr, replica.minValue)
	// 	fmt.Printf("Job: %d, replica: %d, nodeID:, %d, minValue: %.1f\n", job.ID, idx, replica.node.ID, replica.minValue)
	// }
	return true
}

func (j *Job) reset(){
	for _, r := range j.replicas{
		r.minTime = 0
		r.minDr = 0
		r.minValue = math.MaxFloat64
		r.node = nil
	}
}

func (r *replica) createAction(exeTime float64) *action {
	a := &action{
		ID:            len(r.actions),
		executionTime: exeTime,
		datasize:      map[*replica]float64{},
	}
	r.actions = append(r.actions, a)
	return a
}

func (j *Job) predictTime(aveBw float64) float64 {
	predictExecutionTime := 0.0
	for _, replica := range j.replicas {
		var maxTime float64 = 0
		var maxSize float64 = 0
		for _, action := range replica.actions {
			if action.executionTime > maxTime {
				maxTime = action.executionTime
			}

			for _, datasize := range action.datasize {
				if datasize > maxSize {
					maxSize = datasize
				}
			}
		}
		if aveBw == 0.0 {
			predictExecutionTime += maxTime
		} else {
			predictExecutionTime += (maxTime + maxSize/aveBw)
		}
	}
	return predictExecutionTime
}

func (job *Job) priority(avgExecution, avgBW float64) float64 {
	// fmt.Println(job.ID, job.pathPriority)
	if job.pathPriority != 0.0 {
		return job.pathPriority
	}
	// find current job predict execution time
	replica := *job.replicas[0]
	var time float64

	// transmission time + Execution time "Inside" the Job
	var transmissionTime, executionTime float64
	executionTime = 0.0
	transmissionTime = 0.0
	for _, action := range replica.actions {
		
		executionTime += action.executionTime * avgExecution
		maxDataSize := 0.0
		for i := 0; i < len(job.replicas); i++ {
			if maxDataSize < action.datasize[job.replicas[i]] {
				maxDataSize = action.datasize[job.replicas[i]]
			}
		}
		transmissionTime += maxDataSize / avgBW
	}
	fmt.Println("jobID:",job.ID ,"m_{j_h}",executionTime)
	time += (executionTime + transmissionTime)

	transmissionTime = 0.0
	maxDataSize := 0.0

	for _, parent := range job.parent {
		for _, parentReplica := range parent.replicas {
			if maxDataSize < parentReplica.finalDataSize[job] {
				maxDataSize = parentReplica.finalDataSize[job]
			}
		}
	}
	transmissionTime = maxDataSize / avgBW
	time += transmissionTime
	job.pathPriority = time

	// find max child path
	maxPath := 0.0
	for _, child := range job.children {
		if maxPath < child.priority(avgExecution, avgBW) {
			maxPath = child.priority(avgExecution, avgBW)
		}
	}
	job.pathPriority += maxPath
	// fmt.Printf("The path priority of Job %d is %.1f\n", job.ID, job.pathPriority)
	return job.pathPriority
}

func (job *Job) getChildrenReplica() []*replica {
	result := []*replica{}
	for _, childJob := range job.children {
		for _, childReplica := range childJob.replicas {
			result = append(result, childReplica)
		}
	}
	return result
}

func (job *Job) getParentReplica() []*replica {
	result := []*replica{}
	for _, parentJob := range job.parent {
		for _, parentReplica := range parentJob.replicas {
			result = append(result, parentReplica)
		}
	}
	return result
}

// requestExecuteVolume = rand.Float64() * 50 + 50
// executionRatio = 1+rand.Float64()*4*config.speedHeterogeneity (平均數為2.5)
// cpuUsage = rand.Float64
// memUsage = rand.Float64
// workflow.cpuIntensive = rand.Float64()*1.2
// workflow.memIntensive = rand.Float64()*1.2
func dynamicExecutionModel(executionRatio float64, cpuUsage float64, memUsage float64, workflow *Job) float64{
	alpha_1:=1.0
	alpha_2:=0.3
	alpha_3:=0.3
	
	term1 := alpha_1 * executionRatio
	term2 := alpha_2 * math.Pow(1+cpuUsage, workflow.cpuIntensive)
	term3 := alpha_3 * math.Pow(1+memUsage, workflow.memIntensive) 
	return term1/(term2+term3)
}

func medianOfModel()float64{
	rand.Seed(time.Now().UnixNano())

	speedHeterogeneity := 0.77 
	numSamples := 100000

	results := make([]float64, numSamples)

	for i := 0; i < numSamples; i++ {
		executionRatio := 1 + rand.Float64()*4*speedHeterogeneity
		cpuUsage := rand.Float64()
		memUsage := rand.Float64()
		cpuIntensive := rand.Float64() * 1.2
		memIntensive := rand.Float64() * 1.2
		workflow := &Job{
			cpuIntensive: cpuIntensive,
			memIntensive: memIntensive,
		}
		results[i] = dynamicExecutionModel(executionRatio, cpuUsage, memUsage, workflow)
	}

	sort.Float64s(results)
	median := results[numSamples/2]

	fmt.Printf("中位数是: %f\n", median)
	return median // 3.294732	
}

func printNodesUsage(nodes []*node){
	for _, node:= range nodes{
		fmt.Println("NodeID: ", node.ID, ",cpu usage:",node.allocatedCpu ,"/",node.cpu)
		fmt.Println("        ", node.ID, ",mem usage:",node.allocatedMem ,"/",node.mem)
	}
}

func printNodeUsage(node *node){
	fmt.Println("NodeID: ", node.ID, ",cpu usage:",node.allocatedCpu ,"/",node.cpu)
	fmt.Println("        ", node.ID, ",mem usage:",node.allocatedMem ,"/",node.mem)
}