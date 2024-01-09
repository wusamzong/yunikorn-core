package objects

import (
	"math"
	"sort"
	"fmt"
)

// type table map[*Job]map[*node]float64

type ippts struct {
	averageExecutionRate float64
	averageBandwidth     float64
	jobs                 []*Job
	replicas             []*replica
	nodes                []*node
	taskList             map[*node][]*Job
	binding              map[*Job]*node
	// background
	bw  *bandwidth
	w   table                     // computation cost
	c   map[*Job]map[*Job]float64 // communication cost between two jobs
	AFT map[*Job]float64          // the actual finished time of t_i
	EST table                     // the earliest starting time of t_i on p_j
	EFT table                     // the earliest finished time of t_i on p_j
	// IPPTS specific
	PCM     table
	rankPCM map[*Job]float64
	Prank   map[*Job]float64
	LHET    table
	Lhead   table
}

func createIPPTS(jobs []*Job, nodes []*node, bw *bandwidth) *ippts {
	aveExecRate, aveBw := calcAve(nodes, bw)
	replicas := []*replica{}
	for _, tj := range jobs {
		for _, r := range tj.replicas {
			replicas = append(replicas, r)
		}
	}

	taskList := map[*node][]*Job{}
	for _, n := range nodes {
		taskList[n] = []*Job{}
	}

	// init AFT,EST,EFT
	AFT := map[*Job]float64{}
	EST := table{}
	EFT := table{}
	for _, j := range jobs {
		AFT[j] = math.MaxFloat64
		EST[j] = map[*node]float64{}
		EFT[j] = map[*node]float64{}
		for _, n := range nodes {
			EST[j][n] = -1.0
			EFT[j][n] = -1.0
		}
	}

	return &ippts{
		averageBandwidth:     aveBw,
		averageExecutionRate: aveExecRate,
		jobs:                 jobs,
		replicas:             replicas,
		nodes:                nodes,
		taskList:             taskList,
		binding:              map[*Job]*node{},
		bw:                   bw,
		w:                    table{},
		c:                    map[*Job]map[*Job]float64{},
		AFT:                  AFT,
		EST:                  EST,
		EFT:                  EFT,
		PCM:                  table{},
		rankPCM:              map[*Job]float64{},
		Prank:                map[*Job]float64{},
		LHET:                 table{},
		Lhead:                table{},
	}
}

func (p *ippts) allocation(){
	p.calcTime()
	
	p.calcPCM()
	p.calcRankPCMandPrank()
	p.calcLHET()

	p.calcEFT()
	p.calcLhead()

	p.decideNode()
}

func (p *ippts) simulate() float64{
	p.allocation()
	allocManager := intervalAllocManager{current: 0}
	sort.Slice(p.jobs, func(i, j int) bool {
		return p.Prank[p.jobs[i]] < p.Prank[p.jobs[j]]
	})

	queue := []*replica{}
	scheduledReplica := map[*replica]bool{}
	for _, j := range p.jobs {
		j.predictTime(0.0)
		if len(j.parent) == 0 {
			queue = append(queue, j.replicas...)
		}
	}

	for len(queue) > 0 {
		replica := queue[0]

		done := p.tryNode(replica)
		allParentDone := replica.job.allParentDone()
		if done && allParentDone {

			fmt.Println("Replica ID:", replica.job.ID, ",Select Node ID:", replica.node.ID)
			scheduledReplica[replica] = true
			queue = queue[1:]
			allocManager.allocate(replica)

			// is child need to been consider??
			for _, childReplica := range replica.children {
				_, exist := scheduledReplica[childReplica]
				if childReplica.allParentScheduled(scheduledReplica) && !exist {
					queue = append(queue, childReplica)
				}
			}
		} else {
			allocManager.nextInterval()
			fmt.Printf("updateCurrent time: %.2f\n", allocManager.current)
			_ = allocManager.releaseResource()
			if allocManager.current==math.MaxFloat64{
				return 0.0
			}
		}
		
	}
	fmt.Printf("makespan = %.2f\n", allocManager.getMakespan())
	return allocManager.getMakespan()
}

func (p *ippts) tryNode(r *replica)bool{
	node := r.node
	cpuCapacity := node.cpu-node.allocatedCpu
	memCapacity := node.mem-node.allocatedMem
	if r.job.replicaCpu<=cpuCapacity && r.job.replicaMem<=memCapacity{
		return true
	}else{
		return false
	}
}


func (p *ippts) calcTime(){
	for _, j := range p.jobs {
		if _, ok := p.c[j]; !ok {
			p.c[j] = map[*Job]float64{}
		}
		for _, child := range j.children {
			dataSize := 0.0
			for _, r := range j.replicas {
				dataSize += r.finalDataSize[child]
			}
			dataSize /= float64(j.replicaNum)
			p.c[j][child] = dataSize / p.averageBandwidth
		}

		maxExecutionTime := 0.0
		for _, replica := range j.replicas {
			var executionTime float64 = 0
			for _, action := range replica.actions {
				executionTime += action.executionTime
			}
			if executionTime > maxExecutionTime {
				maxExecutionTime = executionTime
			}
		}
		if _, ok := p.w[j]; !ok {
			p.w[j] = map[*node]float64{}
		}
		for _, node := range p.nodes {
			p.w[j][node] = maxExecutionTime * node.executionRate
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

func (p *ippts) calcPCM(){
	var dfs func(*Job)
	dfs = func(j *Job) {
		if _, ok := p.PCM[j]; ok {
			return
		}else{
			p.PCM[j] = map[*node]float64{}
		}

		if len(j.children)==0{
			for _, n:=range p.nodes{
				p.PCM[j][n]=p.w[j][n]
			}
		}

		for _, n:=range p.nodes{
			max:=0.0
			for _, child := range j.children{
				min:=math.MaxFloat64
				dfs(child)
				for _, cn := range p.nodes{
					sum := p.PCM[child][cn]
					sum += p.w[j][n]
					sum += p.w[child][cn]
					if cn!=n{
						sum += p.c[j][child]
					}
					if min>sum{
						min=sum
					}
				}
				if max<min{
					max=min
				}
			}
			p.PCM[j][n]=max
		}
	}
	for _, j := range p.jobs {
		dfs(j)
	}
}

func (p *ippts) calcLHET(){
	for _, j:=range p.jobs{
		p.LHET[j] = map[*node]float64{}
		for _, n:=range p.nodes{
			p.LHET[j][n]=p.PCM[j][n]-p.w[j][n]
		}
	}
}

func (p *ippts) calcRankPCMandPrank(){
	for _, j:=range p.jobs{
		nodeCount := len(p.nodes)
		sum :=0.0
		for _, n:=range p.nodes{
			sum += p.PCM[j][n]
		}
		p.rankPCM[j]=sum/float64(nodeCount)
		outd:=float64(len(j.children))
		p.Prank[j] = p.rankPCM[j]*outd
	}
}		

func (p *ippts) calcEFT(){
	for _, j := range p.jobs {
		p.AFT[j] = math.MaxFloat64
		for _, n := range p.nodes {
			p.getEST(j, n) // calc EST[j][n]
		}
		selectNode := p.binding[j]
		p.taskList[selectNode] = append(p.taskList[selectNode], j)
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

func (p *ippts) getEST(j *Job, n *node) {
	est := 0.0
	for _, parent := range j.parent {
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
			c = p.c[parent][j] * p.averageBandwidth / p.bw.values[parentNode][n]
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

		if est < slot[0] && slot[0]+p.w[j][n] <= slot[1] {
			est = slot[0]
			break
		}
		if est >= slot[0] && est+p.w[j][n] <= slot[1] {
			break
		}
	}
	p.EST[j][n] = est
	p.EFT[j][n] = p.EST[j][n] + p.w[j][n]
	if p.EFT[j][n] < p.AFT[j] {
		p.AFT[j] = p.EFT[j][n]
		p.binding[j] = n
	}
}

func (p *ippts) calcLhead(){
	for _, j := range p.jobs{
		p.Lhead[j] = map[*node]float64{}
		for _, n := range p.nodes{
			p.Lhead[j][n] = p.EFT[j][n] + p.LHET[j][n]
		}
	}

	for _, j := range p.jobs {
		fmt.Printf("job ID:%d\n", j.ID)
		for _, n := range p.nodes {
			fmt.Printf("  Node ID:%d, Lhead: %.1f\n", n.ID, p.Lhead[j][n])
		}
	}
}

func (p *ippts) decideNode(){
	for _, j :=range p.jobs{
		var selectNode *node
		min := math.MaxFloat64
		for _, n :=range p.nodes{
			if n.cpu < j.replicaCpu || n.mem < j.replicaMem{
				continue
			}
			if min>p.Lhead[j][n]{
				min = p.Lhead[j][n]
				selectNode = n
			}
		}
		for _, r := range j.replicas{
			r.node = selectNode
		}
	}
}