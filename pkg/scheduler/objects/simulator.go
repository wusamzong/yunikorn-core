package objects

import (
	"fmt"
	"math"
)

const (
	replicaCalculating       = "calculating"
	replicaCalculateComplete = "calculateComplete"
	replicaTransferring      = "transferring"
	replicaTransferComplete  = "transferComplete"
	replicaComplete          = "complete"

	jobComplete              = "jobComplete"
	jobExecuting             = "jobExecuting"
	finishParentJobTransfer  = "finishParentJobTransfer"
	waitingParentJobTransfer = "waitingParentJobTransfer"
	waitingParentJobFinish   = "waitingParentJobFinish"
)

type simulator struct {
	bw           *bandwidth
	current      float64
	updateTiming []float64
	pending      []*pendJob
	allocations  []*allocJob
	finished     []*finishJob
	nodesUsage   map[*node]*simulateNodeUsage
}

type pendJob struct {
	Job        *Job
	status     string
	finalState []*finalTransferState
}

type finalTransferState struct {
	status     string
	finishTime float64
	pivot      float64
	volume     float64
	bandwidth  float64
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

type finishJob struct {
	Job          *Job
	finishedTime float64
}

func createSimulator(nodes []*node, bw *bandwidth) *simulator {
	fmt.Println("create simulator!")
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

func (s *simulator) addPendJob(job *Job) {
	pendingJob := &pendJob{
		Job:        job,
		status:     waitingParentJobFinish,
		finalState: []*finalTransferState{},
	}

	if pendingJob.isAllParentFinish(s){
		pendingJob.status=waitingParentJobTransfer
		pendingJob.initFinalTransferState(s)
		if pendingJob.isParentTransferDone(){
			s.allocate(pendingJob.Job)
		}else{
			s.pending = append(s.pending, pendingJob)
		}
	}else{
		s.pending = append(s.pending, pendingJob)
	}
}

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
	s.allocations = append(s.allocations, newAllocJob...)
}

func (s *simulator) addFinishedJob(job *Job){
	s.finished = append(s.finished, &finishJob{
		Job: job,
		finishedTime: s.current,
	})
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
			if minEndTime > r.state.finishTime && s.current < r.state.finishTime {
				minEndTime = r.state.finishTime
			}
		}
	}
	if minEndTime == math.MaxFloat64 {
		return
	}
	s.current = minEndTime
}

func (s *simulator) updateState() {
	for _, pendingJob := range s.pending {
		if pendingJob.status == waitingParentJobTransfer{
			pendingJob.updateParentTransfer(s)
		}
		if pendingJob.isParentTransferDone(){
			s.releasePendJob(pendingJob)
			s.allocate(pendingJob.Job)
		}
	}

	for _, allocJob := range s.allocations {
		allocJob.updateState(s)
		if allocJob.state.status == jobComplete{
			s.releaseAllocJob(allocJob)
			s.addFinishedJob(allocJob.Job)
		}
	}

	for _, pendingJob := range s.pending{
		if pendingJob.status == waitingParentJobFinish{
			if pendingJob.isAllParentFinish(s){
				pendingJob.status=waitingParentJobTransfer
				pendingJob.initFinalTransferState(s)
				if pendingJob.isParentTransferDone(){
					s.allocate(pendingJob.Job)
				}
			}
		}
	}
}

func (s *simulator) releaseAllocJob(job *allocJob) {
	for _, r := range job.allocReplica {
		node := r.replica.node
		s.nodesUsage[node].usedCPU -= job.Job.replicaCpu
		s.nodesUsage[node].usedMemory -= job.Job.replicaMem
	}
	for idx, j := range s.allocations {
		if j == job {
			s.allocations = append(s.allocations[:idx], s.allocations[idx+1:]...)
		}
	}
}

func (s *simulator) releasePendJob(job *pendJob) {
	for idx, j := range s.pending {
		if j == job {
			s.pending = append(s.pending[:idx], s.pending[idx+1:]...)
		}
	}
}

func (j *allocJob) updateState(s *simulator) {

	for _, r := range j.allocReplica {
		r.updateDynamicExecutionState(s.current) // executing to executing/complete
	}

	if j.isCalculateDone() {
		j.initTransferTime(s) // complete to replicaTransfer

	} else if j.isTransferDone() {
		if j.allActionDone() {
			j.state.status = jobComplete
		} else {
			j.updateActionID()
			j.initNextActionState(s) // complete to replicaExecuting
		}
	}

}

func (j *allocJob) isCalculateDone() bool {
	done := true
	for _, r := range j.allocReplica {
		if r.state.status != replicaCalculateComplete {
			done = false
		}
	}
	return done
}

func (j *allocJob) isTransferDone() bool {
	done := true
	for _, r := range j.allocReplica {
		if r.state.status != replicaTransferComplete {
			done = false
		}
	}
	return done
}

func (j *allocJob) updateActionID() {
	j.state.actionID += 1
	for _, r := range j.allocReplica {
		r.state.actionID = j.state.actionID
	}
}

func (j *allocJob) allActionDone() bool {
	if j.state.actionID == j.Job.actionNum-1 {
		return true
	}
	return false
}

func (j *allocJob) initNextActionState(s *simulator) {
	for _, r := range j.allocReplica {
		r.setState(replicaCalculating)
		actionID := r.state.actionID
		node := r.node
		volume := r.replica.actions[actionID].executionTime
		replicaCpuRequest := j.Job.replicaCpu
		replicaMemRequest := j.Job.replicaMem
		cpuUsage := float64(s.nodesUsage[node].usedCPU-replicaCpuRequest) / float64(node.cpu)
		memUsage := float64(s.nodesUsage[node].usedMemory-replicaMemRequest) / float64(node.mem)
		r.state.executeRatio = dynamicExecutionModel(node.executionRate, cpuUsage, memUsage, j.Job)
		r.state.volume = volume
		r.state.finishTime = s.current + r.state.volume/r.state.executeRatio
		r.state.pivot = s.current
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
		if r.state.volume <= 0.000001 {
			r.state.status = replicaCalculateComplete
			r.state.volume = 0
		}
	} else if status == replicaTransferring {
		period := current - r.state.pivot
		r.state.volume -= period * r.state.executeRatio
		if r.state.volume <= 0.000001 { // Avoiding calculation errors that make it impossible to equal 0 due to calculations.
			r.state.status = replicaTransferComplete
			r.state.volume = 0
		}
	}
	r.state.pivot = current
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

func (j *allocJob) initTransferTime(s *simulator) {
	for _, r := range j.allocReplica {
		r.setState(replicaTransferring)
		replica := r.replica
		job := replica.job
		actionID := r.state.actionID
		action := replica.actions[actionID]

		transmissionTime := 0.0
		bandwidth := 1.0
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
}

func (pj *pendJob) initFinalTransferState(s *simulator) {
	job := pj.Job
	for _, parentJob := range job.parent {
		for _, parentReplica := range parentJob.replicas {
			datasize := parentReplica.finalDataSize[job]
			for _, replica := range job.replicas {
				from := parentReplica.node
				to := replica.node

				if from == to {
					pj.finalState = append(pj.finalState, &finalTransferState{
						status: finishParentJobTransfer,
						finishTime: s.current,
						pivot: s.current,
						volume: 0.0,
						bandwidth: s.bw.values[from][to],
					})
				} else {
					transmissionTime := datasize / s.bw.values[from][to]
					pj.finalState = append(pj.finalState, &finalTransferState{
						status: waitingParentJobTransfer,
						finishTime: s.current+transmissionTime,
						pivot: s.current,
						volume: datasize,
						bandwidth: s.bw.values[from][to],
					})					
				}
			}
		}
	}
	if pj.isParentTransferDone(){
		pj.status = finishParentJobTransfer
	}
}

func (pj *pendJob) isParentTransferDone()bool{
	if pj.status == finishParentJobTransfer{
		return true
	}

	for _, state := range pj.finalState{
		if state.status==waitingParentJobTransfer{
			return false
		}
	}
	return true
}

func (s *simulator) removePendingJob(job *pendJob) {
	for idx, j := range s.pending {
		if j == job {
			s.pending = append(s.pending[:idx], s.pending[idx+1:]...)
		}
	}
}

func (pj *pendJob) updateParentTransfer(s *simulator){
	if pj.isParentTransferDone(){
		return 
	}

	for _, finalstate := range pj.finalState{
		if finalstate.status==finishParentJobTransfer{
			continue
		}else{
			period := s.current - finalstate.pivot
			finalstate.volume -= period * finalstate.bandwidth
			if finalstate.volume <= 0.1{
				finalstate.status = finishParentJobTransfer
				finalstate.volume = 0
			}
		}
		finalstate.pivot = s.current
	}
}

func (pj *pendJob) isAllParentFinish(s *simulator)bool{
	if pj.status == waitingParentJobTransfer || pj.status == finishParentJobTransfer{
		return true
	}
	parent:=pj.Job.parent
	for _, parentJob := range parent{
		if !s.isJobFinished(parentJob){
			return false
		}
	}
	return true
}

func (s *simulator) isJobFinished(job *Job)bool{
	for _, finishedJob := range s.finished{
		if job==finishedJob.Job{
			return true
		}
	}
	return false
}

func (s *simulator) isJobAllocated(job *Job)bool{
	for _, allocatedJob := range s.allocations{
		if job==allocatedJob.Job{
			return true
		}
	}
	return false
}

func (s *simulator) isParentJobFinish(j *Job)bool{
	for _, parentJob := range j.parent{
		if !s.isJobFinished(parentJob){
			return false
		}
	}
	return true
}

func (s *simulator) isParentAllocated(j *Job)bool{
	for _, parentJob := range j.parent{
		if !s.isJobAllocated(parentJob){
			return false
		}
	}
	return true
}