package objects

import (
	// "fmt"
	"math"
	"sort"
	// "os"
)

type table map[*replica]map[*node]float64

type mpeft struct {
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
	offspringSet map[*replica][]*replica
	DCT          map[*replica]float64
	rankAP       map[*replica]float64
	jobRankAP    map[*Job]float64
	OCT          table
	CPS          map[*replica]map[*node]*replica
	MEFT         table
	k            table
}

func createMPEFT(jobs []*Job, nodes []*node, bw *bandwidth) *mpeft {
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

	return &mpeft{
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
		DCT:                  map[*replica]float64{},
		rankAP:               map[*replica]float64{},
		jobRankAP:            map[*Job]float64{},
		OCT:                  table{},
		CPS:                  map[*replica]map[*node]*replica{},
		MEFT:                 table{},
		k:                    table{},
	}
}

func calcOffSpringSet(replicas []*replica) map[*replica][]*replica {
	result := map[*replica][]*replica{}
	var dfs func(*replica) []*replica
	dfs = func(j *replica) []*replica {
		if _, ok := result[j]; ok {
			return result[j]
		}

		currentState := []*replica{}
		for _, child := range j.children {
			currentState = append(currentState, child)
			currentState = append(currentState, dfs(child)...)
		}
		currentState = removeDuplicates(currentState)

		result[j] = currentState
		return currentState
	}
	for _, j := range replicas {
		dfs(j)
	}
	// fmt.Println("--Show offspring set--")
	// for j, r := range result {
	// 	fmt.Println("jobID:",j.ID)
	// 	fmt.Print("offset: ")
	// 	for _, offset := range r{
	// 		fmt.Printf("%d ",offset.ID)
	// 	}
	// 	fmt.Println()
	// }
	return result
}

func removeDuplicates(arr []*replica) []*replica {
	seen := make(map[*replica]bool)
	result := []*replica{}
	for _, value := range arr {
		if _, ok := seen[value]; !ok {
			seen[value] = true
			result = append(result, value)
		}
	}
	return result
}

func (m *mpeft) allocation() {
	m.calcTime()
	m.calcEFT()
	m.calcDCT()
	m.calcRankAP()

	m.calcOCTandCPS()
	m.calcK()
	m.calcMEFT()
	// m.decideNode()
}

func (m *mpeft) simulate() (float64, float64) {
	m.allocation()
	simulator := createSimulator(m.nodes, m.bw)

	queue := make([]*Job, len(m.jobs))

	copy(queue, m.jobs)

	sort.Slice(queue, func(i, j int) bool {
		return m.jobRankAP[queue[i]] < m.jobRankAP[queue[j]]
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
			done := m.decideNode(job)

			if done && simulator.isParentJobFinish(job) {
				simulator.addPendJob(job)
				scheduledJob[job] = true
			} else {
				reserveQueue = append(reserveQueue, job)
			}
		}
		queue = append(queue, reserveQueue...)

		finishedLength:=len(simulator.finished)
		for len(simulator.allocations)+len(simulator.pending)>0{
			simulator.update()
			// printJobStatus(simulator)
			if finishedLength < len(simulator.finished){
				break
			}
		}


	}
	for len(simulator.allocations)+len(simulator.pending)>0{
		simulator.update()
		// printJobStatus(simulator)
		if len(simulator.pending) ==0 && len(simulator.allocations)==0{
			break
		}
	}
	
	makespan:= simulator.current
	SLR:=calSLR(m.nodes, getCriticalPath(m.jobs), makespan)

	return makespan, SLR
}

func (m *mpeft) tryNode(r *replica) bool {
	node := r.node
	cpuCapacity := node.cpu - node.allocatedCpu
	memCapacity := node.mem - node.allocatedMem
	if r.job.replicaCpu <= cpuCapacity && r.job.replicaMem <= memCapacity {
		return true
	} else {
		return false
	}
}

func (m *mpeft) calcDCT() {
	for _, r := range m.replicas {
		executionTime := 0.0
		// execution time
		for _, a := range r.actions {
			executionTime += a.executionTime
		}
		// transmission time

		sumData := 0.0

		for _, data := range r.finalDataSize {
			sumData += data
		}
		if _, ok := m.DCT[r]; !ok {
			m.DCT[r] = 0.0
		}
		m.DCT[r] += (executionTime/m.averageExecutionRate + sumData/m.averageBandwidth)

	}

	// print result
	// for _, j := range m.jobs{
	// 	fmt.Println("jobID: ",j.ID)
	// 	for idx, r := range j.replicas{
	// 		fmt.Printf("replica.ID:%d, DCT:%.2f\n", idx, m.DCT[r])
	// 	}
	// }
}

func (m *mpeft) calcTime() {
	for _, r := range m.replicas {
		if _, ok := m.c[r]; !ok {
			m.c[r] = map[*replica]float64{}
		}

		for _, child := range r.job.children {
			for _, childReplica := range child.replicas {
				m.c[r][childReplica] = r.finalDataSize[child] / m.averageBandwidth
			}

		}

		var executionTime float64 = 0
		for _, action := range r.actions {
			executionTime += action.executionTime
		}

		if _, ok := m.w[r]; !ok {
			m.w[r] = map[*node]float64{}
		}
		for _, node := range m.nodes {
			m.w[r][node] = executionTime / node.executionRate
		}
	}
	// for _,j:=range m.jobs{
	// 	fmt.Println("from jobID:",j.ID)
	// 	for _, r:=range j.replicas{
	// 		fmt.Println("from replica ID:",r.ID)
	// 		for _, child:=range r.children{
	// 			fmt.Println(" JobID: ",child.job.ID," to replica ID:" , child.ID , ",communication Time:",m.c[r][child])
	// 		}
	// 	}

	// }
	// for _,j:=range m.jobs{
	// 	fmt.Println("jobID:",j.ID)
	// 	for _, r:=range j.replicas{
	// 		fmt.Println("from replica ID:",r.ID)
	// 		for _, node:=range m.nodes{
	// 			fmt.Println("  execute on nodeID:" , node.ID , ",execution Time:",m.w[r][node])
	// 		}
	// 	}
	// }

}

func (m *mpeft) calcRankAP() {

	for _, r := range m.replicas {
		sum := m.DCT[r]
		for _, offsetJob := range m.offspringSet[r] {
			sum += m.DCT[offsetJob]
		}
		m.rankAP[r] = sum
	}

	for _, j := range m.jobs {
		max := 0.0
		for _, r := range j.replicas {
			if m.rankAP[r] > max {
				max = m.rankAP[r]
			}
		}
		m.jobRankAP[j] = max
	}

	// print result
	// for _, j := range m.jobs{
	// 	fmt.Println("jobID: ",j.ID)
	// 	for idx, r := range j.replicas{
	// 		fmt.Printf("replica.ID:%d, RankAP:%.2f\n", idx, m.rankAP[r])
	// 	}
	// }
}

func (m *mpeft) calcOCTandCPS() {
	var dfs func(*replica)
	dfs = func(r *replica) {
		if _, ok := m.OCT[r]; ok {
			return
		}

		if len(r.children) == 0 {
			for _, n := range m.nodes {
				if _, ok := m.OCT[r]; !ok {
					m.OCT[r] = map[*node]float64{}
					m.CPS[r] = map[*node]*replica{}
				}

				m.OCT[r][n] = 0
				m.CPS[r][n] = nil
			}
			return
		}

		maxMakespan := 0.0
		var criticalPath *replica
		for _, n := range m.nodes {
			for _, succ := range r.children {
				min := math.MaxFloat64
				for _, cn := range m.nodes {
					if _, ok := m.OCT[succ][cn]; !ok {
						dfs(succ)
					}
					current := 0.0
					current += m.OCT[succ][cn]
					current += m.w[succ][cn]

					if n != cn {
						current += m.c[r][succ]
					}

					if min > current {
						min = current
					}
				}
				if min > maxMakespan {
					maxMakespan = min
					criticalPath = succ
				}
			}
			if _, ok := m.OCT[r]; !ok {
				m.OCT[r] = map[*node]float64{}
				m.CPS[r] = map[*node]*replica{}
			}
			m.OCT[r][n] = maxMakespan
			m.CPS[r][n] = criticalPath
		}
	}

	for _, r := range m.replicas {
		dfs(r)
	}

	// for _, j:=range m.jobs{
	// 	fmt.Println("job ID:", j.ID)
	// 	for _, r := range j.replicas{
	// 		fmt.Println("replica ID:", r.ID)
	// 		for _, n:= range m.nodes{
	// 			if m.CPS[r][n]==nil{
	// 				fmt.Printf("  Node ID:%d, OCT: %.1f, CPS: %d\n",n.ID, m.OCT[r][n], -1)
	// 			}else{
	// 				fmt.Printf("  Node ID:%d, OCT: %.1f, CPS: %d\n",n.ID, m.OCT[r][n], m.CPS[r][n].ID)
	// 			}
	// 		}
	// 	}

	// }
}

func (m *mpeft) calcEFT() {
	// top := []*Job{}
	// tail := []*Job{}
	// for _, j := range m.jobs {
	// 	if len(j.parent) == 0 {
	// 		top = append(top, j)
	// 	} else {
	// 		tail = append(tail, j)
	// 	}
	// }
	// binpacking for top jobs
	// for i := 0; i < len(top); i++ {
	// 	j := top[i]
	// 	n := m.nodes[i%len(m.nodes)]
	// 	m.binding[j] = n
	// 	m.taskList[n] = append(m.taskList[n], j)
	// 	// m.EST[j][n] = math.Max(0, m.avail(n))
	// 	m.getEST(j, n)
	// 	m.EFT[j][n] = m.EST[j][n] + m.w[j][n]
	// 	m.AFT[j] = m.EFT[j][n]
	// 	fmt.Println(m.EST[j][n])
	// 	m.sortTaskList(n)
	// }
	// // allocate for tail jobs, which is the jobs have parent
	// for i := 0; i < len(tail); i++ {
	// 	j := tail[i]
	// 	m.AFT[j] = math.MaxFloat64
	// 	for _, n := range m.nodes {
	// 		m.getEST(j, n) // calc EST[j][n]
	// 	}
	// 	selectNode := m.binding[j]
	// 	m.taskList[selectNode] = append(m.taskList[selectNode], j)
	// 	m.sortTaskList(selectNode)
	// }

	for _, r := range m.replicas {
		m.AFT[r] = math.MaxFloat64
		for _, n := range m.nodes {
			m.getEST(r, n) // calc EST[r][n]
		}
		selectNode := m.binding[r]
		m.taskList[selectNode] = append(m.taskList[selectNode], r)
		m.sortTaskList(selectNode)
	}

	// for _, r := range m.replicas {
	// 	fmt.Printf("job ID:%d, Node ID: %d, AFT: %.1f\n", r.ID, m.binding[r].ID, m.AFT[r])

	// 	for _, n := range m.nodes {
	// 		fmt.Printf("  Node ID:%d, EST: %.1f, EFT: %.1f\n", n.ID, m.EST[r][n], m.EFT[r][n])
	// 	}
	// }

}

func (m *mpeft) sortTaskList(n *node) {
	sort.Slice(m.taskList[n], func(i, j int) bool {
		job1 := m.taskList[n][i]
		job2 := m.taskList[n][i]
		return m.EST[job1][n] < m.EST[job2][n]
	})
}

func (m *mpeft) avail(n *node) float64 {
	sort.Slice(m.taskList[n], func(i, j int) bool {
		job1 := m.taskList[n][i]
		job2 := m.taskList[n][i]
		return m.AFT[job1] > m.AFT[job2]
	})
	j := m.taskList[n][0]
	return m.AFT[j]
}

func (m *mpeft) getEST(r *replica, n *node) {
	est := 0.0
	for _, parent := range r.parent {
		if m.binding[parent] == nil {
			for _, n := range m.nodes {
				m.getEST(parent, n)
			}
			selectNode := m.binding[parent]
			m.taskList[selectNode] = append(m.taskList[selectNode], parent)
			m.sortTaskList(selectNode)
		}
		parentNode := m.binding[parent]
		c := 0.0
		if m.binding[parent] != n {
			c = m.c[parent][r] * m.averageBandwidth / m.bw.values[parentNode][n]
		}
		est = math.Max(est, m.AFT[parent]+c)
	}

	freeTimes := [][]float64{}

	if len(m.taskList) == 0 {
		freeTimes = append(freeTimes, []float64{0.0, math.MaxFloat64})
	} else {
		for i, task := range m.taskList[n] {
			start := m.EST[task][n]
			if i == 0 {
				if start != 0 {
					freeTimes = append(freeTimes, []float64{0, start})
				}
			} else {
				lastEndTime := m.EFT[m.taskList[n][i-1]][n]
				freeTimes = append(freeTimes, []float64{lastEndTime, start})
			}
			lastJob := m.taskList[n][len(m.taskList[n])-1]
			lastJobEnd := m.EFT[lastJob][n]
			freeTimes = append(freeTimes, []float64{lastJobEnd, math.MaxFloat64})
		}
	}
	for _, slot := range freeTimes {

		if est < slot[0] && slot[0]+m.w[r][n] <= slot[1] {
			est = slot[0]
			break
		}
		if est >= slot[0] && est+m.w[r][n] <= slot[1] {
			break
		}
	}
	m.EST[r][n] = est
	m.EFT[r][n] = m.EST[r][n] + m.w[r][n]
	if m.EFT[r][n] < m.AFT[r] {
		m.AFT[r] = m.EFT[r][n]
		m.binding[r] = n
	}

}

func (m *mpeft) calcK() {
	for _, r := range m.replicas {
		if _, ok := m.k[r]; !ok {
			m.k[r] = map[*node]float64{}
		}
		for _, n := range m.nodes {
			if len(r.children) <= len(m.nodes)+1 {
				m.k[r][n] = 1
				continue
			}
			sum := 0.0
			for _, child := range r.children {
				sum += m.rankAP[child] + m.c[r][child]
			}
			k := m.rankAP[m.CPS[r][n]] / sum
			m.k[r][n] = k
		}
	}
}

func (m *mpeft) calcMEFT() {
	for _, r := range m.replicas {
		if _, ok := m.MEFT[r]; !ok {
			m.MEFT[r] = map[*node]float64{}
		}
		for _, n := range m.nodes {
			m.MEFT[r][n] = m.EFT[r][n] + m.OCT[r][n]*m.k[r][n]
		}
	}
	// for _, r := range m.replicas {
	// 	fmt.Printf("replica ID:%d\n", r.ID)
	// 	for _, n := range m.nodes {
	// 		fmt.Printf("  Node ID:%d, MEFT: %.1f\n", n.ID, m.MEFT[r][n])
	// 	}
	// }
}

// func (m *mpeft) decideNode() {
// 	for _, r := range m.replicas {
// 		j := r.job
// 		min := math.MaxFloat64
// 		for _, n := range m.nodes {
// 			if n.cpu < j.replicaCpu || n.mem < j.replicaMem {
// 				continue
// 			}
// 			if min > m.MEFT[r][n] {
// 				min = m.MEFT[r][n]
// 				r.node = n
// 			}
// 		}
// 	}
// }

func (m *mpeft) decideNode(j *Job) bool {
	doneReplica := []*replica{}

	for _, r := range j.replicas {
		min := math.MaxFloat64
		var selectNode *node
		for _, node := range m.nodes {
			var currentJobCpuUsage int
			var currentJobMemUsage int
			for _, r := range doneReplica {
				if r.node == node {
					currentJobCpuUsage += j.replicaCpu
					currentJobMemUsage += j.replicaMem
				}
			}

			if node.cpu-node.allocatedCpu < currentJobCpuUsage+j.replicaCpu {
				continue
			}

			if node.mem-node.allocatedMem < currentJobMemUsage+j.replicaMem {
				continue
			}
			
			cpuUsage := float64(currentJobCpuUsage+node.allocatedCpu)/float64(node.cpu)
			memUsage := float64(currentJobMemUsage+node.allocatedMem)/float64(node.mem)
			dynamicValue:=m.MEFT[r][node] * math.Pow(dynamicExecutionModel(node.executionRate, cpuUsage, memUsage, j), 2)
			if min > dynamicValue {
				min = dynamicValue
				selectNode = node
			}
		}
		if selectNode == nil {
			for _, r := range j.replicas {
				r.node = nil
			}
			return false
		} else {
			r.node = selectNode
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
					transmissionTime = datasize / m.bw.values[from][to]
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
