package objects

import (
	"math"
)

const (
	replicaCalculating       = "calculating"
	replicaCalculateComplete = "calculateComplete"
	replicaTransferring      = "transferring"
	replicaTransferComplete  = "transferComplete"
	replicaComplete          = "complete"

	jobComplete  = "jobComplete"
	jobExecuting = "jobExecuting"
)

type simulator struct {
	bw           *bandwidth
	current      float64
	updateTiming []float64
	allocations  []*allocJob
	nodesUsage   map[*node]*simulateNodeUsage
}

type simulateNodeUsage struct {
	usedCPU    int
	usedMemory int
}

type allocJob struct {
	Job          *Job
	allocReplica []*allocReplica
	state        *jobState
}

type jobState struct {
	actionID int
	status   string
}

type allocReplica struct {
	node    *node
	replica *replica
	state   *state
}

type state struct {
	actionID     int
	status       string
	finishTime   float64
	pivot        float64
	executeRatio float64
	volume       float64
}

func createSimulator(nodes []*node, bw *bandwidth) *simulator {
	return &simulator{
		bw:           bw,
		current:      0.0,
		updateTiming: []float64{},
		allocations:  []*allocJob{},
		nodesUsage:   createSimulateNodes(nodes),
	}
}

func createSimulateNodes(nodes []*node) map[*node]*simulateNodeUsage {
	simulateNodes := map[*node]*simulateNodeUsage{}
	for _, n := range nodes {
		simulateNodes[n] = &simulateNodeUsage{
			usedCPU:    0,
			usedMemory: 0,
		}
	}
	return simulateNodes
}

// Adding Allocation

func (s *simulator) allocate(job *Job) {
	newAllocJob := []*allocJob{}
	newAllocJob = append(newAllocJob, &allocJob{
		Job:          job,
		allocReplica: s.createAllocReplica(job),
		state: &jobState{
			actionID: 0,
			status:   jobExecuting,
		},
	})
	s.initDynamicExecutionState(newAllocJob) // calculate before allocatie resource
	// s.collectFinishTime(newAllocJob)
	s.addingUsage(newAllocJob)
}

func (s *simulator) createAllocReplica(job *Job) []*allocReplica {
	allocReplicas := []*allocReplica{}
	for _, r := range job.replicas {
		allocReplicas = append(allocReplicas, &allocReplica{
			node:    r.node,
			replica: r,
			state:   s.createState(r),
		})
	}
	return allocReplicas
}

func (s *simulator) createState(replica *replica) *state {
	firstAction := replica.actions[0]
	return &state{
		actionID: firstAction.ID,
		status:   replicaCalculating,
		pivot:    s.current,
	}
}

func (s *simulator) addingUsage(newAllocJob []*allocJob) {
	for _, j := range newAllocJob {
		for _, r := range j.allocReplica {
			s.nodesUsage[r.node].usedCPU += j.Job.replicaCpu
			s.nodesUsage[r.node].usedMemory += j.Job.replicaMem
		}
	}
}

func (s *simulator) initDynamicExecutionState(newAllocJob []*allocJob) {
	for _, j := range newAllocJob {
		for _, r := range j.allocReplica {
			actionID := r.state.actionID
			node := r.node
			volume := r.replica.actions[actionID].executionTime
			cpuUsage := float64(s.nodesUsage[node].usedCPU) / float64(node.cpu)
			memUsage := float64(s.nodesUsage[node].usedMemory) / float64(node.mem)
			r.state.executeRatio = dynamicExecutionModel(node.executionRate, cpuUsage, memUsage, j.Job)
			r.state.volume = volume
			r.state.finishTime = s.current + r.state.volume/r.state.executeRatio
		}
	}
}

// func (s *simulator) collectFinishTime(newAllocJob []*allocJob){
// 	for _, j := range newAllocJob {
// 		for _, r := range j.allocReplica {
// 			s.updateTiming = append(s.updateTiming, r.state.finishTime)
// 		}
// 	}
// }

// Update

func (s *simulator) update() {
	s.updateTime()
	s.updateState()
}

func (s *simulator) updateTime() {
	var minEndTime float64 = math.MaxFloat64
	for _, j := range s.allocations {
		for _, r := range j.allocReplica {
			if minEndTime > r.state.finishTime {
				minEndTime = r.state.finishTime
			}
		}
	}
	s.current = minEndTime
}

func (s *simulator) updateState() {
	for _, j := range s.allocations {
		j.updateState(s.current, s)
	}
}

func (j *allocJob) updateState(current float64, s *simulator) {
	isAllCalculateComplete := true
	isAllTransferringComplete := true
	for _, r := range j.allocReplica {
		r.updateDynamicExecutionState(current)
		if r.state.status == replicaCalculating {
			isAllCalculateComplete = false
		}
		if r.state.status == replicaTransferring {
			isAllTransferringComplete = false
		}
	}

	if isAllCalculateComplete == true {
		for _, r := range j.allocReplica {
			r.initTransferTime(current, s)
		}
	} else if isAllTransferringComplete == true {
		if j.isDone(){

		}else{
			j.updateActionID()
			j.initDynamicExecutionState()
		}
		
	}
}

func (j *allocJob) updateActionID(){
	j.state.actionID+=1
	for _, r := range j.allocReplica {
		r.state.actionID=j.state.actionID
	}
}

func (r *allocReplica) updateDynamicExecutionState(current float64) {
	status := r.state.status
	if status == replicaCalculateComplete || status == replicaTransferComplete {
		return
	}

	if status == replicaCalculating {
		period := current - r.state.pivot
		r.state.volume -= period * r.state.executeRatio
		if r.state.volume <= 0 {
			r.state.status = replicaCalculateComplete
			r.state.volume = 0
		}
	} else if status == replicaTransferring {

	}
}

func (r *allocReplica) updateState() {
	if r.state.status == replicaCalculating {
		r.state.status = replicaCalculateComplete
	} else if r.state.status == replicaTransferring {
		r.state.status = replicaTransferComplete
	}
}

func (r *allocReplica) setState(state string) {
	r.state.status = state
}

// Transfer

func (r *allocReplica) initTransferTime(current float64, s *simulator) {
	r.setState(replicaTransferring)
	replica := r.replica
	job := replica.job
	actionID := r.state.actionID
	action := replica.actions[actionID]

	transmissionTime := 0.0
	bandwidth:=0.0
	volume := 0.0
	for i := 0; i < job.replicaNum; i++ {
		from := replica.node
		to := job.replicas[i].node
		datasize := action.datasize[job.replicas[i]]
		var curTransmissionTime float64
		if s.bw.values[from][to] == 0 {
			curTransmissionTime = 0
		} else {
			curTransmissionTime = datasize / s.bw.values[from][to]
		}
		if transmissionTime < curTransmissionTime {
			transmissionTime = curTransmissionTime
			bandwidth = s.bw.values[from][to]
			volume = datasize
		}
	}

	r.state.volume = volume
	r.state.executeRatio = bandwidth
	r.state.finishTime = s.current + volume/bandwidth
	r.state.pivot = s.current
}
