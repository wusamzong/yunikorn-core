package objects

import (
	// "fmt"

	"math"
	"sort"
)

type macro struct {
	averageExecutionRate float64
	averageBandwidth     float64
	jobs                 []*Job
	replicas             []*replica
	nodes                []*node
	taskList             map[*node][]*replica
	binding              map[*replica]*node
	// background
	bw  *bandwidth
	w   table                             // computation cost
	c   map[*replica]map[*replica]float64 // communication cost between two replicas
	AFT map[*replica]float64              // the actual finished time of t_i
	EST table                             // the earliest starting time of t_i on p_j
	EFT table                             // the earliest finished time of t_i on p_j
	// MPEFT specific
	bl           map[*replica]float64
	jobBl        map[*Job]float64
	offspringSet map[*replica][]*replica
	DCT          map[*replica]float64
	rankAP       map[*replica]float64
	jobRankAP    map[*Job]float64
	OCT          table
	CPS          map[*replica]map[*node]*replica
	MEFT         table
	k            table
}

func createMacro(jobs []*Job, nodes []*node, bw *bandwidth) *macro {
	// fmt.Println("create MPEFT object")
	aveExecRate, aveBw := calcAve(nodes, bw)

	replicas := []*replica{}
	for _, tj := range jobs {
		for _, r := range tj.replicas {
			replicas = append(replicas, r)
		}
	}
	offspringSet := calcOffSpringSet(replicas)

	taskList := map[*node][]*replica{}
	for _, n := range nodes {
		taskList[n] = []*replica{}
	}

	// init AFT,EST,EFT
	AFT := map[*replica]float64{}
	EST := table{}
	EFT := table{}
	for _, r := range replicas {
		AFT[r] = math.MaxFloat64
		EST[r] = map[*node]float64{}
		EFT[r] = map[*node]float64{}
		for _, n := range nodes {
			EST[r][n] = -1.0
			EFT[r][n] = -1.0
		}
	}

	return &macro{
		averageBandwidth:     aveBw,
		averageExecutionRate: aveExecRate,
		jobs:                 jobs,
		replicas:             replicas,
		offspringSet:         offspringSet,
		nodes:                nodes,
		taskList:             taskList,
		binding:              map[*replica]*node{},
		bw:                   bw,
		w:                    table{},
		c:                    map[*replica]map[*replica]float64{},
		AFT:                  AFT,
		EST:                  EST,
		EFT:                  EFT,
		bl:                   map[*replica]float64{},
		jobBl:                map[*Job]float64{},
		DCT:                  map[*replica]float64{},
		rankAP:               map[*replica]float64{},
		jobRankAP:            map[*Job]float64{},
		OCT:                  table{},
		CPS:                  map[*replica]map[*node]*replica{},
		MEFT:                 table{},
		k:                    table{},
	}
}

func (a *macro) calcBl() {
	for _, r := range a.replicas {
		a.getBl(r)
	}
	for _, j := range a.jobs {
		maxBl := 0.0
		for _, r := range j.replicas {
			if a.bl[r] > maxBl {
				maxBl = a.bl[r]
			}
		}
		a.jobBl[j] = maxBl
	}
}

func (a *macro) getBl(r *replica) {
	if len(r.children) == 0 {
		a.bl[r] = 0
		return
	}
	if _, exist := a.bl[r]; exist {
		return
	}
	max := 0.0
	for _, child := range r.children {
		if _, exist := a.bl[child]; !exist {
			a.getBl(child)
		}

		value := a.c[r][child] + a.bl[child]
		if value > max {
			max = value
		}
	}
	a.bl[r] += max

	for _, action := range r.actions {
		a.bl[r] += action.executionTime / a.averageExecutionRate
	}
}

func (a *macro) allocation() {
	a.calcTime()
	a.calcBl()
	a.calcEFT()

}

func (a *macro) calcTime() {
	for _, r := range a.replicas {
		if _, ok := a.c[r]; !ok {
			a.c[r] = map[*replica]float64{}
		}

		for _, child := range r.job.children {
			for _, childReplica := range child.replicas {
				a.c[r][childReplica] = r.finalDataSize[child] / a.averageBandwidth
			}

		}

		var executionTime float64 = 0
		for _, action := range r.actions {
			executionTime += action.executionTime
		}

		if _, ok := a.w[r]; !ok {
			a.w[r] = map[*node]float64{}
		}
		for _, node := range a.nodes {
			a.w[r][node] = executionTime / node.executionRate
		}
	}
}

func (a *macro) calcEFT() {
	for _, r := range a.replicas {
		a.AFT[r] = math.MaxFloat64
		for _, n := range a.nodes {
			a.getEST(r, n) // calc EST[r][n]
		}
		selectNode := a.binding[r]
		a.taskList[selectNode] = append(a.taskList[selectNode], r)
		a.sortTaskList(selectNode)
	}
}

func (a *macro) getEST(r *replica, n *node) {
	if a.EST[r][n]!=-1.0{
		return
	}

	est := 0.0
	for _, parent := range r.parent {
		if a.binding[parent] == nil {
			for _, n := range a.nodes {
				a.getEST(parent, n)
			}
			selectNode := a.binding[parent]
			a.taskList[selectNode] = append(a.taskList[selectNode], parent)
			a.sortTaskList(selectNode)
		}
		parentNode := a.binding[parent]
		c := 0.0
		if a.binding[parent] != n {
			c = a.c[parent][r] * a.averageBandwidth / a.bw.values[parentNode][n]
		}
		est = math.Max(est, a.AFT[parent]+c)
	}

	freeTimes := [][]float64{}

	if len(a.taskList) == 0 {
		freeTimes = append(freeTimes, []float64{0.0, math.MaxFloat64})
	} else {
		for i, task := range a.taskList[n] {
			start := a.EST[task][n]
			if i == 0 {
				if start != 0 {
					freeTimes = append(freeTimes, []float64{0, start})
				}
			} else {
				lastEndTime := a.EFT[a.taskList[n][i-1]][n]
				freeTimes = append(freeTimes, []float64{lastEndTime, start})
			}
			lastJob := a.taskList[n][len(a.taskList[n])-1]
			lastJobEnd := a.EFT[lastJob][n]
			freeTimes = append(freeTimes, []float64{lastJobEnd, math.MaxFloat64})
		}
	}
	for _, slot := range freeTimes {

		if est < slot[0] && slot[0]+a.w[r][n] <= slot[1] {
			est = slot[0]
			break
		}
		if est >= slot[0] && est+a.w[r][n] <= slot[1] {
			break
		}
	}
	a.EST[r][n] = est
	a.EFT[r][n] = a.EST[r][n] + a.w[r][n]
	if a.EFT[r][n] < a.AFT[r] {
		a.AFT[r] = a.EFT[r][n]
		a.binding[r] = n
	}
}

func (a *macro) sortTaskList(n *node) {
	sort.Slice(a.taskList[n], func(i, j int) bool {
		job1 := a.taskList[n][i]
		job2 := a.taskList[n][i]
		return a.EST[job1][n] < a.EST[job2][n]
	})
}


func (a *macro) simulate() metric {
	a.allocation()
	simulator := createSimulator(a.nodes, a.bw)

	queue := make([]*Job, len(a.jobs))

	copy(queue, a.jobs)


	sort.Slice(queue, func(i, j int) bool {
		return a.jobBl[queue[i]] < a.jobBl[queue[j]]
	})

	scheduledJob := map[*Job]bool{}


	for len(queue) > 0 {
		reserveQueue := []*Job{}
		for len(queue) > 0 {
			job := queue[0]
			queue = queue[1:]
			if _, exist := scheduledJob[job]; exist {
				continue
			}
			a.decideNode(simulator, job)

			if simulator.isParentJobFinish(job) {
				simulator.addPendJob(job)
				scheduledJob[job] = true
			} else {
				reserveQueue = append(reserveQueue, job)
			}
		}
		queue = append(queue, reserveQueue...)

		finishedLength := len(simulator.finished)
		for len(simulator.allocations)+len(simulator.pending) > 0 {
			simulator.update()
			// printJobStatus(simulator)
			if finishedLength < len(simulator.finished) {
				break
			}
		}

	}
	for len(simulator.allocations)+len(simulator.pending) > 0 {
		simulator.update()
		// fmt.Println("update")
		// printJobStatus(simulator)
		if len(simulator.pending) == 0 && len(simulator.allocations) == 0 {
			break
		}
	}

	// simulator.printFinishedJob()
	makespan := simulator.current
	SLR := calSLR(a.nodes, getCriticalPath(a.jobs), makespan)
	speedup := calSpeedup(a.nodes, a.jobs, makespan)
	efficiency := speedup / float64(len(a.nodes))

	return metric{
		makespan:   makespan,
		SLR:        SLR,
		speedup:    speedup,
		efficiency: efficiency,
	}
}

func (a *macro) decideNode(s *simulator, j *Job){
	EFTofNode := map[*node]float64{}
	LastJobOfNode := map[*node]*Job{}

	for _, n := range a.nodes{
		EFTofNode[n]=0.0
	}

	for _, j := range s.allocations {
		for _, r := range j.allocReplica {
			if r.state.finishTime > EFTofNode[r.node] {
				EFTofNode[r.node] = r.state.finishTime
				LastJobOfNode[r.node]=j.Job
			}
		}
	}
	for _, j := range s.pending {
		estimateExecutionTime := 0.0
		onlyReplica := j.Job.replicas[0]
		for _, action := range onlyReplica.actions{
			estimateExecutionTime+=action.executionTime/onlyReplica.node.executionRate
		}

		if s.current+estimateExecutionTime > EFTofNode[onlyReplica.node] {
			EFTofNode[onlyReplica.node] = s.current+estimateExecutionTime
			LastJobOfNode[onlyReplica.node]=j.Job
		}
	}
	
	var nodeSelection *node
	minEFT := math.MaxFloat64
	for _, n := range a.nodes{
		maxStartTime := 0.0

		for _, parent := range j.parent{
			for _, j:= range s.pending{
				if j.Job != parent{
					continue
				}
				onlyReplica:=parent.replicas[0]
				from:= onlyReplica.node
				to := n
				transfertime:=onlyReplica.finalDataSize[j.Job]/a.bw.values[from][to]
				parentEFT:=s.current + j.Job.calcSumOfExecutionTime()
				if transfertime + parentEFT > maxStartTime{
					maxStartTime = transfertime + parentEFT
				}
			}


			for _, j:= range s.allocations{
				if j.Job != parent{
					continue
				}
				onlyReplica:=parent.replicas[0]
				from:= onlyReplica.node
				to := n
				transfertime:=onlyReplica.finalDataSize[j.Job]/a.bw.values[from][to]
				parentEFT:=j.allocatedTime + j.Job.calcSumOfExecutionTime()
				if transfertime + parentEFT > maxStartTime{
					maxStartTime = transfertime + parentEFT
				}
			}

			for _, j:= range s.finished{
				if j.Job != parent{
					continue
				}
				onlyReplica:=parent.replicas[0]
				from:= onlyReplica.node
				to := n
				transfertime:=onlyReplica.finalDataSize[j.Job]/a.bw.values[from][to]
				if j.finishedTime+transfertime > maxStartTime{
					maxStartTime = transfertime + j.finishedTime+transfertime
				}
			}
		}
		if maxStartTime < minEFT{
			minEFT = maxStartTime
			nodeSelection = n
		}
	}
	onlyReplica:=j.replicas[0]
	onlyReplica.node = nodeSelection
	originalCPU := j.replicaCpu
	originalMem := j.replicaMem
	j.replicaCpu = nodeSelection.cpu
	j.replicaMem = nodeSelection.mem
	for _, a := range onlyReplica.actions{
		a.executionTime = a.executionTime * float64(originalCPU+originalMem)/float64(nodeSelection.cpu+nodeSelection.mem) //* 0.75
	}


	var time float64
	for _, r := range j.replicas {

		maxTime := 0.0
		for _, action := range r.actions {
			var transmissionTime, executionTime float64
			executionTime = action.executionTime / r.node.executionRate
			
			transmissionTime = 0.0
			maxTransmissionTime := 0.0
			for _, child := range r.children {
				from := r.node
				to := child.node
				datasize := action.datasize[child]
				if from == to {
					transmissionTime = 0.0
				} else {
					transmissionTime = datasize / a.bw.values[from][to]
				}
				if transmissionTime > maxTransmissionTime {
					maxTransmissionTime = transmissionTime
				}
			}
			if maxTransmissionTime+executionTime > maxTime {
				maxTime = maxTransmissionTime + executionTime
			}
		}
		time += maxTime
	}
	j.makespan = time
}