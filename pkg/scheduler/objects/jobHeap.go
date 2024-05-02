package objects

type JobHeap struct {
	averageExecutionRate float64
	averageBandwidth     float64
	jobs                 []*Job
}

func (h JobHeap) Len() int { return len(h.jobs) }
func (h JobHeap) Less(i, j int) bool {return h.jobs[i].priority(h.averageExecutionRate, h.averageExecutionRate) > h.jobs[j].priority(h.averageExecutionRate, h.averageExecutionRate)}

// func (h JobHeap) Less(i, j int) bool { return h.jobs[i].predictExecutionTime > h.jobs[j].predictExecutionTime }
// func (h JobHeap) Less(i, j int) bool { return h.jobs[i].ID < h.jobs[j].ID }
func (h JobHeap) Swap(i, j int) { 
	h.jobs[i], h.jobs[j]= h.jobs[j], h.jobs[i]
}

func (h *JobHeap) Push(x interface{}) {
	(*h).jobs = append((*h).jobs, x.(*Job))
}

func (h *JobHeap) Pop() interface{} {
	old := (*h).jobs
	n := len(old)
	x := old[n-1]
	(*h).jobs = old[0 : n-1]
	return x
}
