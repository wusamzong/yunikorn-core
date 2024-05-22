package objects

import (
	"testing"
	"fmt"
	"math/rand"
)

func TestAllocateJob(t *testing.T) {
	rand.Seed(0)
	nodes, bw := createSampleNode()  // cpu:1000/ mem:1024
	job := &Job{
		ID:         0,
		replicaNum: 3,
		replicaCpu: 200,
		replicaMem: 256,
		actionNum:  2,
		parent:     []*Job{},
		children:   []*Job{},
		finish:     0,
	}
	createRandReplica(job)
	job.replicas[0].node=nodes[0]
	job.replicas[1].node=nodes[1]
	job.replicas[2].node=nodes[2]
	
	simulator := createSimulator(nodes, bw)
	simulator.allocate(job)

	for i:=0; i<30;i++{
		simulator.update()
		printStatus(simulator)
	}
}

func TestAddTwoPendJob(t *testing.T){
	rand.Seed(0)
	nodes, bw := createSampleNode()
	var job0 *Job
	var job1 *Job
	job0 = &Job{
		ID:         0,
		replicaNum: 3,
		replicaCpu: 200,
		replicaMem: 256,
		actionNum:  2,
		parent:     []*Job{},
		children:   []*Job{job1},
		finish:     0,
	}

	job1 = &Job{
		ID:         1,
		replicaNum: 3,
		replicaCpu: 200,
		replicaMem: 256,
		actionNum:  2,
		parent:     []*Job{job0},
		children:   []*Job{},
		finish:     0,
	}

	createRandReplica(job0)
	job0.replicas[0].node=nodes[0]
	job0.replicas[1].node=nodes[1]
	job0.replicas[2].node=nodes[2]

	createRandReplica(job1)
	job1.replicas[0].node=nodes[0]
	job1.replicas[1].node=nodes[1]
	job1.replicas[2].node=nodes[2]

	simulator := createSimulator(nodes, bw)
	simulator.addPendJob(job0)
	simulator.addPendJob(job1)

	for i:=0; i<30;i++{
		simulator.update()
		printStatus(simulator)
		if len(simulator.finished)==2{
			break
		}
	}
}

func printStatus(s *simulator){
	fmt.Println("Update:")
	fmt.Println("  current:", s.current)
	for _, j := range s.pending{
		fmt.Println("  Job:", j.Job.ID,"  Action:",j.status)
	}

	for _, j := range s.allocations{
		fmt.Println("  Job:", j.Job.ID,"  Action:",j.state.actionID, " Status:", j.state.status)
		for _, r := range j.allocReplica{
			fmt.Println("  ",r.state.status, r.state.finishTime)
		}
	}
}

func printJobStatus(s *simulator){
	fmt.Println("Update--  current:", s.current)

	fmt.Println("  Pending Jobs")
	for _, j := range s.pending{
		fmt.Println("    Job:", j.Job.ID,"  Status:",j.status)
	}

	fmt.Println("  Allocated Jobs")
	for _, j := range s.allocations{
		fmt.Println("    Job:", j.Job.ID,"  Action:",j.state.actionID, " Status:", j.state.status)
	}

	fmt.Println("  Finished Jobs")
	for _, j := range s.finished{
		fmt.Println("    Job:", j.Job.ID)
	}
}