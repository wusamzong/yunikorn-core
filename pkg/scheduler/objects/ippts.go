package objects

import (
	"math"
	"sort"
	// "fmt"
)

// type table map[*Job]map[*node]float64

type ippts struct {
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
	c   map[*replica]map[*replica]float64 // communication cost between two jobs
	AFT map[*replica]float64              // the actual finished time of t_i
	EST table                             // the earliest starting time of t_i on p_j
	EFT table                             // the earliest finished time of t_i on p_j
	// IPPTS specific
	PCM      table
	rankPCM  map[*replica]float64
	Prank    map[*replica]float64
	jobPrank map[*Job]float64
	LHET     table
	Lhead    table
}

func createIPPTS(jobs []*Job, nodes []*node, bw *bandwidth) *ippts {
	aveExecRate, aveBw := calcAve(nodes, bw)
	replicas := []*replica{}
	for _, tj := range jobs {
		for _, r := range tj.replicas {
			replicas = append(replicas, r)
		}
	}

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

	return &ippts{
		averageBandwidth:     aveBw,
		averageExecutionRate: aveExecRate,
		jobs:                 jobs,
		replicas:             replicas,
		nodes:                nodes,
		taskList:             taskList,
		binding:              map[*replica]*node{},
		bw:                   bw,
		w:                    table{},
		c:                    map[*replica]map[*replica]float64{},
		AFT:                  AFT,
		EST:                  EST,
		EFT:                  EFT,
		PCM:                  table{},
		rankPCM:              map[*replica]float64{},
		Prank:                map[*replica]float64{},
		jobPrank:             map[*Job]float64{},
		LHET:                 table{},
		Lhead:                table{},
	}
}

func (p *ippts) allocation() {
	p.calcTime()

	p.calcPCM()
	p.calcRankPCMandPrank()
	p.calcLHET()

	p.calcEFT()
	p.calcLhead()

	// p.decideNode()
}

func (p *ippts) simulate() (float64, float64) {
	p.allocation()
	allocManager := intervalAllocManager{
		totalCapacity: []float64{},
		totalAllocte:  []float64{0.0, 0.0},
		totalUsage:    []float64{0.0, 0.0},
		current:       0,
	}
	allocManager.initCapacity(p.nodes)

	queue := make([]*Job, len(p.jobs))
	copy(queue, p.jobs)

	sort.Slice(queue, func(i, j int) bool {
		return p.jobPrank[queue[i]] < p.jobPrank[queue[j]]
	})

	scheduledJob := map[*Job]bool{}
	// scheduledReplica := map[*replica]bool{}

	for len(queue) > 0 {
		reserveQueue := []*Job{}
		for len(queue) > 0 {
			job := queue[0]
			queue = queue[1:]
			if _, exist := scheduledJob[job]; exist {
				continue
			}
			
			done := p.decideNode(job)
			// allParentDone := replica.allParentScheduled(scheduledReplica)
			allParentDone := job.allParentDone()
			// fmt.Print("jobID:", job.ID, ", done:", done, ", allParentDone:", allParentDone, ", makespan:", job.makespan)
			// fmt.Print(", Parent: ")
			// for _, parent := range job.parent {
			// 	fmt.Printf("%d ,", parent.ID)
			// }
			// fmt.Println()
			if done && allParentDone {
				// fmt.Println("allocate: ", job.ID)
				// scheduledReplica[replica] = true
				scheduledJob[job] = true
				allocManager.allocate(job)
				// for _, node := range p.nodes {
				// 	fmt.Printf("nodeId:%d, capacity:{%d, %d}, allocated:{%d, %d}\n", node.ID, node.cpu, node.mem, node.allocatedCpu, node.allocatedMem)
				// }
			} else {
				reserveQueue = append(reserveQueue, job)

			}
		}
		queue = append(queue, reserveQueue...)
		// fmt.Printf("Try Job: (j-%d (%d, %d, %d))\n", job.ID, job.replicaCpu, job.replicaMem, job.replicaNum)
		allocManager.nextInterval()
		// fmt.Printf("updateCurrent time: %.2f\n", allocManager.current)
		_ = allocManager.releaseResource()
		// for _, node := range p.nodes {
		// 	fmt.Printf("nodeId:%d, capacity:{%d, %d}, allocated:{%d, %d}\n", node.ID, node.cpu, node.mem, node.allocatedCpu, node.allocatedMem)
		// }
		if allocManager.current == math.MaxFloat64 {
			return 0.0, 0.0
		}

	}

	// fmt.Printf("makespan = %.2f\n", allocManager.getMakespan())
	return allocManager.getResult()
}

func (p *ippts) tryNode(r *replica) bool {
	node := r.node
	cpuCapacity := node.cpu - node.allocatedCpu
	memCapacity := node.mem - node.allocatedMem
	if r.job.replicaCpu <= cpuCapacity && r.job.replicaMem <= memCapacity {
		return true
	} else {
		return false
	}
}

func (p *ippts) calcTime() {
	for _, r := range p.replicas {
		if _, ok := p.c[r]; !ok {
			p.c[r] = map[*replica]float64{}
		}

		for _, child := range r.job.children {
			for _, childReplica := range child.replicas {
				p.c[r][childReplica] = r.finalDataSize[child] / p.averageBandwidth
			}

		}

		var executionTime float64 = 0
		for _, action := range r.actions {
			executionTime += action.executionTime
		}

		if _, ok := p.w[r]; !ok {
			p.w[r] = map[*node]float64{}
		}
		for _, node := range p.nodes {
			p.w[r][node] = executionTime / node.executionRate
		}
	}
	// for _,j:=range p.jobs{
	// 	fmt.Println("from jobID:",j.ID)
	// 	for _, child:=range j.children{
	// 		fmt.Println("  to jobID:" , child.ID , ",communication Time:",p.c[j][child])
	// 	}
	// }
	// for _,j:=range p.jobs{
	// 	fmt.Println("jobID:",j.ID)
	// 	for _, node:=range p.nodes{
	// 		fmt.Println("  execute on nodeID:" , node.ID , ",execution Time:",p.w[j][node])
	// 	}
	// }

}

func (p *ippts) calcPCM() {
	var dfs func(*replica)
	dfs = func(r *replica) {
		if _, ok := p.PCM[r]; ok {
			return
		} else {
			p.PCM[r] = map[*node]float64{}
		}

		if len(r.children) == 0 {
			for _, n := range p.nodes {
				p.PCM[r][n] = p.w[r][n]
			}
		}

		for _, n := range p.nodes {
			max := 0.0
			for _, child := range r.children {
				min := math.MaxFloat64
				dfs(child)
				for _, cn := range p.nodes {
					sum := p.PCM[child][cn]
					sum += p.w[r][n]
					sum += p.w[child][cn]
					if cn != n {
						sum += p.c[r][child]
					}
					if min > sum {
						min = sum
					}
				}
				if max < min {
					max = min
				}
			}
			p.PCM[r][n] = max
		}
	}
	for _, r := range p.replicas {
		dfs(r)
	}
}

func (p *ippts) calcLHET() {
	for _, r := range p.replicas {
		p.LHET[r] = map[*node]float64{}
		for _, n := range p.nodes {
			p.LHET[r][n] = p.PCM[r][n] - p.w[r][n]
		}
	}
}

func (p *ippts) calcRankPCMandPrank() {
	for _, r := range p.replicas {
		nodeCount := len(p.nodes)
		sum := 0.0
		for _, n := range p.nodes {
			sum += p.PCM[r][n]
		}
		p.rankPCM[r] = sum / float64(nodeCount)
		outd := float64(len(r.children))
		p.Prank[r] = p.rankPCM[r] * outd
	}

	for _, j := range p.jobs {
		max := 0.0
		for _, r := range j.replicas {
			if p.Prank[r] > max {
				max = p.Prank[r]
			}
		}
		p.jobPrank[j] = max
	}
}

func (p *ippts) calcEFT() {
	for _, r := range p.replicas {
		p.AFT[r] = math.MaxFloat64
		for _, n := range p.nodes {
			p.getEST(r, n) // calc EST[r][n]
		}
		selectNode := p.binding[r]
		p.taskList[selectNode] = append(p.taskList[selectNode], r)
		p.sortTaskList(selectNode)
	}
}

func (p *ippts) sortTaskList(n *node) {
	sort.Slice(p.taskList[n], func(i, j int) bool {
		job1 := p.taskList[n][i]
		job2 := p.taskList[n][i]
		return p.EST[job1][n] < p.EST[job2][n]
	})
}

func (p *ippts) getEST(r *replica, n *node) {
	est := 0.0
	for _, parent := range r.parent {
		if p.binding[parent] == nil {
			for _, n := range p.nodes {
				p.getEST(parent, n)
			}
			selectNode := p.binding[parent]
			p.taskList[selectNode] = append(p.taskList[selectNode], parent)
			p.sortTaskList(selectNode)
		}
		parentNode := p.binding[parent]
		c := 0.0
		if p.binding[parent] != n {
			c = p.c[parent][r] * p.averageBandwidth / p.bw.values[parentNode][n]
		}
		est = math.Max(est, p.AFT[parent]+c)
	}

	freeTimes := [][]float64{}

	if len(p.taskList) == 0 {
		freeTimes = append(freeTimes, []float64{0.0, math.MaxFloat64})
	} else {
		for i, task := range p.taskList[n] {
			start := p.EST[task][n]
			if i == 0 {
				if start != 0 {
					freeTimes = append(freeTimes, []float64{0, start})
				}
			} else {
				lastEndTime := p.EFT[p.taskList[n][i-1]][n]
				freeTimes = append(freeTimes, []float64{lastEndTime, start})
			}
			lastJob := p.taskList[n][len(p.taskList[n])-1]
			lastJobEnd := p.EFT[lastJob][n]
			freeTimes = append(freeTimes, []float64{lastJobEnd, math.MaxFloat64})
		}
	}
	for _, slot := range freeTimes {

		if est < slot[0] && slot[0]+p.w[r][n] <= slot[1] {
			est = slot[0]
			break
		}
		if est >= slot[0] && est+p.w[r][n] <= slot[1] {
			break
		}
	}
	p.EST[r][n] = est
	p.EFT[r][n] = p.EST[r][n] + p.w[r][n]
	if p.EFT[r][n] < p.AFT[r] {
		p.AFT[r] = p.EFT[r][n]
		p.binding[r] = n
	}
}

func (p *ippts) calcLhead() {
	for _, r := range p.replicas {
		p.Lhead[r] = map[*node]float64{}
		for _, n := range p.nodes {
			p.Lhead[r][n] = p.EFT[r][n] + p.LHET[r][n]
		}
	}

	// for _, r := range p.replicas {
	// 	fmt.Printf("replica ID:%d\n", r.ID)
	// 	for _, n := range p.nodes {
	// 		fmt.Printf("  Node ID:%d, Lhead: %.1f\n", n.ID, p.Lhead[r][n])
	// 	}
	// }
}

// func (p *ippts) decideNode() {
// 	for _, r := range p.replicas {
// 		j := r.job
// 		min := math.MaxFloat64
// 		for _, n := range p.nodes {
// 			if n.cpu < j.replicaCpu || n.mem < j.replicaMem {
// 				continue
// 			}
// 			if min > p.Lhead[r][n] {
// 				min = p.Lhead[r][n]
// 				r.node = n
// 			}
// 		}
// 	}
// }

func (p *ippts) decideNode(j *Job) bool {
	doneReplica := []*replica{}
	
	for _, r := range j.replicas {
		min := math.MaxFloat64
		var selectNode *node
		for _, node := range p.nodes {
			var currentJobCpuUsage int
			var currentJobMemUsage int
			for _, r := range doneReplica {
				if r.node == node {
					currentJobCpuUsage += j.replicaCpu
					currentJobMemUsage += j.replicaMem
				}
			}

			if node.cpu-node.allocatedCpu-currentJobCpuUsage < j.replicaCpu || node.mem-node.allocatedMem-currentJobMemUsage < j.replicaMem {
				continue
			}
			if min > p.Lhead[r][node] {
				min = p.Lhead[r][node]
				selectNode = node
			}
		}
		if selectNode == nil {
			for _, r := range j.replicas{
				r.node=nil
			}
			return false
		}else{
			r.node=selectNode
		}
		doneReplica = append(doneReplica, r)
	}
	var time float64
	for _, r := range j.replicas {
		maxTime := 0.0
		for _, a := range r.actions {
			var transmissionTime, executionTime float64
			executionTime = a.executionTime / r.node.executionRate
			transmissionTime = 0.0
			maxTransmissionTime := 0.0
			for _, child := range r.children {
				from := r.node
				to := child.node
				datasize := a.datasize[child]
				if from == to {
					transmissionTime = 0.0
				} else {
					transmissionTime = datasize / p.bw.values[from][to]
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

	return true
}
