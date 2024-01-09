package objects

import (
	"fmt"
	"math"
	"sort"
)

type table map[*Job]map[*node]float64

type mpeft struct {
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
	// MPEFT specific
	offspringSet map[*Job][]*Job
	DCT          map[*Job]float64
	rankAP       map[*Job]float64
	OCT          table
	CPS          map[*Job]map[*node]*Job
	MEFT         table
	k            table
}

func createMPEFT(jobs []*Job, nodes []*node, bw *bandwidth) *mpeft {
	fmt.Println("create MPEFT object")
	aveExecRate, aveBw := calcAve(nodes, bw)
	offspringSet := calcOffSpringSet(jobs)

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

	return &mpeft{
		averageBandwidth:     aveBw,
		averageExecutionRate: aveExecRate,
		jobs:                 jobs,
		replicas:             replicas,
		offspringSet:         offspringSet,
		nodes:                nodes,
		taskList:             taskList,
		binding:              map[*Job]*node{},
		bw:                   bw,
		w:                    table{},
		c:                    map[*Job]map[*Job]float64{},
		AFT:                  AFT,
		EST:                  EST,
		EFT:                  EFT,
		DCT:                  map[*Job]float64{},
		rankAP:               map[*Job]float64{},
		OCT:                  table{},
		CPS:                  map[*Job]map[*node]*Job{},
		MEFT:                 table{},
		k:                    table{},
	}
}

func calcOffSpringSet(jobs []*Job) map[*Job][]*Job {
	result := map[*Job][]*Job{}
	var dfs func(*Job) []*Job
	dfs = func(j *Job) []*Job {
		if _, ok := result[j]; ok {
			return result[j]
		}

		currentState := []*Job{}
		for _, child := range j.children {
			currentState = append(currentState, child)
			currentState = append(currentState, dfs(child)...)
		}
		currentState = removeDuplicates(currentState)

		result[j] = currentState
		return currentState
	}
	for _, j := range jobs {
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

func removeDuplicates(arr []*Job) []*Job {
	seen := make(map[*Job]bool)
	result := []*Job{}
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
	m.decideNode()
}

func (m *mpeft) simulate() float64 {
	m.allocation()
	allocManager := intervalAllocManager{current: 0}
	sort.Slice(m.jobs, func(i, j int) bool {
		return m.rankAP[m.jobs[i]] < m.rankAP[m.jobs[j]]
	})
	for _, node := range m.nodes {
		fmt.Printf("nodeId:%d, capacity:{%d, %d}, allocated:{%d, %d}\n", node.ID, node.cpu, node.mem, node.allocatedCpu, node.allocatedMem)
	}

	queue := []*replica{}
	scheduledReplica := map[*replica]bool{}
	for _, j := range m.jobs {
		j.predictTime(0.0)
		if len(j.parent) == 0 {
			queue = append(queue, j.replicas...)
		}
	}

	for len(queue) > 0 {
		replica := queue[0]

		done := m.tryNode(replica)
		allParentDone := replica.job.allParentDone()
		if done && allParentDone {

			fmt.Println("Replica ID:", replica.job.ID, ",Select Node ID:", replica.node.ID)
			scheduledReplica[replica] = true
			queue = queue[1:]
			allocManager.allocate(replica)
			for _, node := range m.nodes {
				fmt.Printf("nodeId:%d, capacity:{%d, %d}, allocated:{%d, %d}\n", node.ID, node.cpu, node.mem, node.allocatedCpu, node.allocatedMem)
			}
			// is child need to been consider??
			for _, childReplica := range replica.children {
				_, exist := scheduledReplica[childReplica]
				if childReplica.allParentScheduled(scheduledReplica) && !exist {
					queue = append(queue, childReplica)
				}
			}
		} else {

			fmt.Printf("Try replica: (j-%d r-%d (%d, %d)), On node: %d (%d, %d)\n", replica.ID, replica.job.ID, replica.job.replicaCpu, replica.job.replicaMem, replica.node.ID, replica.node.cpu, replica.node.mem)
			allocManager.nextInterval()
			fmt.Printf("updateCurrent time: %.2f\n", allocManager.current)
			_ = allocManager.releaseResource()
			for _, node := range m.nodes {
				fmt.Printf("nodeId:%d, capacity:{%d, %d}, allocated:{%d, %d}\n", node.ID, node.cpu, node.mem, node.allocatedCpu, node.allocatedMem)
			}
			if allocManager.current==math.MaxFloat64{
				return 0.0
			}
		}
	}
	fmt.Printf("makespan = %.2f\n", allocManager.getMakespan())
	return allocManager.getMakespan()
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
	for _, j := range m.jobs {
		executionTime := 0.0
		// execution time
		for _, a := range j.replicas[0].actions {
			executionTime += a.executionTime
		}
		// transmission time
		for _, r := range j.replicas {
			sumData := 0.0
			for _, data := range r.finalDataSize {
				sumData += data
			}
			if _, ok := m.DCT[r.job]; !ok {
				m.DCT[r.job] = 0.0
			}
			m.DCT[r.job] += (executionTime*m.averageExecutionRate + sumData/m.averageBandwidth)
		}
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
	for _, j := range m.jobs {
		if _, ok := m.c[j]; !ok {
			m.c[j] = map[*Job]float64{}
		}
		for _, child := range j.children {
			dataSize := 0.0
			for _, r := range j.replicas {
				dataSize += r.finalDataSize[child]
			}
			dataSize /= float64(j.replicaNum)
			m.c[j][child] = dataSize / m.averageBandwidth
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
		if _, ok := m.w[j]; !ok {
			m.w[j] = map[*node]float64{}
		}
		for _, node := range m.nodes {
			m.w[j][node] = maxExecutionTime * node.executionRate
		}
	}
	// for _,j:=range m.jobs{
	// 	fmt.Println("from jobID:",j.ID)
	// 	for _, child:=range j.children{
	// 		fmt.Println("  to jobID:" , child.ID , ",communication Time:",m.c[j][child])
	// 	}
	// }
	// for _,j:=range m.jobs{
	// 	fmt.Println("jobID:",j.ID)
	// 	for _, node:=range m.nodes{
	// 		fmt.Println("  execute on nodeID:" , node.ID , ",execution Time:",m.w[j][node])
	// 	}
	// }

}

func (m *mpeft) calcRankAP() {
	for _, j := range m.jobs {
		for _, r := range j.replicas {
			sum := m.DCT[r.job]
			for _, offsetJob := range m.offspringSet[j] {
				sum += m.DCT[offsetJob]
			}
			m.rankAP[r.job] = sum
		}
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
	var dfs func(*Job)
	dfs = func(j *Job) {
		if _, ok := m.OCT[j]; ok {
			return
		}

		if len(j.children) == 0 {
			for _, n := range m.nodes {
				if _, ok := m.OCT[j]; !ok {
					m.OCT[j] = map[*node]float64{}
					m.CPS[j] = map[*node]*Job{}
				}

				m.OCT[j][n] = 0
				m.CPS[j][n] = nil
			}
			return
		}

		maxMakespan := 0.0
		var criticalPath *Job
		for _, n := range m.nodes {
			for _, succ := range j.children {
				min := math.MaxFloat64
				for _, cn := range m.nodes {
					if _, ok := m.OCT[succ][cn]; !ok {
						dfs(succ)
					}
					current := 0.0
					current += m.OCT[succ][cn]
					current += m.w[succ][cn]

					if n != cn {
						current += m.c[j][succ]
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
			if _, ok := m.OCT[j]; !ok {
				m.OCT[j] = map[*node]float64{}
				m.CPS[j] = map[*node]*Job{}
			}
			m.OCT[j][n] = maxMakespan
			m.CPS[j][n] = criticalPath
		}
	}

	for _, j := range m.jobs {
		dfs(j)
	}

	// for _, j:=range m.jobs{
	// 	fmt.Println("job ID:", j.ID)
	// 	for _, n:= range m.nodes{
	// 		if m.CPS[j][n]==nil{
	// 			fmt.Printf("  Node ID:%d, OCT: %.1f, CPS: %d\n",n.ID, m.OCT[j][n], -1)
	// 		}else{
	// 			fmt.Printf("  Node ID:%d, OCT: %.1f, CPS: %d\n",n.ID, m.OCT[j][n], m.CPS[j][n].ID)
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

	for _, j := range m.jobs {
		m.AFT[j] = math.MaxFloat64
		for _, n := range m.nodes {
			m.getEST(j, n) // calc EST[j][n]
		}
		selectNode := m.binding[j]
		m.taskList[selectNode] = append(m.taskList[selectNode], j)
		m.sortTaskList(selectNode)
	}

	// for _, j := range m.jobs {
	// 	fmt.Printf("job ID:%d, Node ID: %d, AFT: %.1f\n", j.ID, m.binding[j].ID, m.AFT[j])

	// 	for _, n := range m.nodes {
	// 		fmt.Printf("  Node ID:%d, EST: %.1f, EFT: %.1f\n", n.ID, m.EST[j][n], m.EFT[j][n])
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

func (m *mpeft) getEST(j *Job, n *node) {
	est := 0.0
	for _, parent := range j.parent {
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
			c = m.c[parent][j] * m.averageBandwidth / m.bw.values[parentNode][n]
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

		if est < slot[0] && slot[0]+m.w[j][n] <= slot[1] {
			est = slot[0]
			break
		}
		if est >= slot[0] && est+m.w[j][n] <= slot[1] {
			break
		}
	}
	m.EST[j][n] = est
	m.EFT[j][n] = m.EST[j][n] + m.w[j][n]
	if m.EFT[j][n] < m.AFT[j] {
		m.AFT[j] = m.EFT[j][n]
		m.binding[j] = n
	}

}

func (m *mpeft) calcK() {
	for _, j := range m.jobs {
		if _, ok := m.k[j]; !ok {
			m.k[j] = map[*node]float64{}
		}
		for _, n := range m.nodes {
			if len(j.children) <= len(m.nodes)+1 {
				m.k[j][n] = 1
				continue
			}
			sum := 0.0
			for _, child := range j.children {
				sum += m.rankAP[child] + m.c[j][child]
			}
			k := m.rankAP[m.CPS[j][n]] / sum
			m.k[j][n] = k
		}
	}
}

func (m *mpeft) calcMEFT() {
	for _, j := range m.jobs {
		if _, ok := m.MEFT[j]; !ok {
			m.MEFT[j] = map[*node]float64{}
		}
		for _, n := range m.nodes {
			m.MEFT[j][n] = m.EFT[j][n] + m.OCT[j][n]*m.k[j][n]
		}
	}
	for _, j := range m.jobs {
		fmt.Printf("job ID:%d\n", j.ID)
		for _, n := range m.nodes {
			fmt.Printf("  Node ID:%d, MEFT: %.1f\n", n.ID, m.MEFT[j][n])
		}
	}
}

func (m *mpeft) decideNode() {
	for _, j := range m.jobs {
		var selectNode *node
		min := math.MaxFloat64
		for _, n := range m.nodes {
			if n.cpu < j.replicaCpu || n.mem < j.replicaMem {
				continue
			}
			if min > m.MEFT[j][n] {
				min = m.MEFT[j][n]
				selectNode = n
			}
		}
		for _, r := range j.replicas {
			r.node = selectNode
		}
	}
}
