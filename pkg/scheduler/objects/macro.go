package objects

import (
	// "math/rand"
	"fmt"

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

func (a *macro) sortTaskList(n *node) {
	sort.Slice(a.taskList[n], func(i, j int) bool {
		job1 := a.taskList[n][i]
		job2 := a.taskList[n][i]
		return a.EST[job1][n] < a.EST[job2][n]
	})
}


func (a *macro) simulate() metric {
	fmt.Println("start simulate for macro")
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
			done:=a.decideNode(simulator, job)

			if done && simulator.isParentJobFinish(job) {
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
	// speedup := calSpeedup(a.nodes, a.jobs, makespan)
	// efficiency := speedup / float64(len(a.nodes))

	return metric{
		makespan:   makespan,
		SLR:        SLR,
		// speedup:    speedup,
		// efficiency: efficiency,
	}
}

func (a *macro) decideNode(s *simulator, j *Job)bool{
	
	var nodeSelection *node
	minEFT := math.MaxFloat64
	for _, n := range a.nodes{
		if n.allocatedCpu != 0 && n.allocatedMem != 0{
			continue
		}
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
	if nodeSelection == nil{
		return false
	}
	onlyReplica:=j.replicas[0]
	onlyReplica.node = nodeSelection
	originalCPU := j.replicaCpu
	originalMem := j.replicaMem
	j.replicaCpu = nodeSelection.cpu
	j.replicaMem = nodeSelection.mem
	for _, a := range onlyReplica.actions{
		a.executionTime = a.executionTime * float64(originalCPU+originalMem)/float64(nodeSelection.cpu+nodeSelection.mem) * 1.03
	}
	
	return true
}