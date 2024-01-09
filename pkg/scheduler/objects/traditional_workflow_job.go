package objects

type tradProcessor struct {
	nodes []*node
}

func calcAve(nodes []*node, bw *bandwidth) (float64, float64) {
	sum := 0.0
	count := len(nodes)
	for _, node := range nodes {
		sum += node.executionRate
	}
	avgExecutionRage := sum / float64(count)

	edgeCount := 0.0
	edgeSum := 0.0
	for i := 0; i < len(nodes)-1; i++ {
		for j := i + 1; j < len(nodes); j++ {
			from := nodes[i]
			to := nodes[j]
			edgeCount += 1.0
			edgeSum += (*bw).values[from][to]
		}
	}
	avgBandwidth := edgeSum / edgeCount

	return avgExecutionRage, avgBandwidth
}

// Complement the relationship at the replica level?

// func childrenTJ(jobs []*Job) []*tradJob {
// 	result := []*tradJob{}
// 	for _, job := range jobs {
// 		result = append(result, &tradJob{
// 			mapping: job,
// 		})
// 	}
// 	return result
// }
