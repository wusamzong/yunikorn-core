// go run ./createDAG.go > ./simple.dot && dot -Tsvg simple.dot -o simple.svg
package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"strconv"

	"gopkg.in/yaml.v3"
	// "time"
)

const (
	minPerRank      = 2 // Nodes/Rank: How 'fat' the DAG should be.
	maxPerRank      = 5
	minRanks        = 5 // Ranks: How 'tall' the DAG should be.
	maxRanks        = 15
	percent         = 10 // Chance of having an Edge.
	filePath        = "dag02.yaml"
	appConfigPath   = "../workflow-config.yaml"
	JobTemplate     = "job-template.yaml"
	KwokPodTemplate = "kwok-pod-template.yaml"
)

type Job struct {
	APIVersion string `yaml:"apiVersion"`
	Kind       string `yaml:"kind"`
	Metadata   struct {
		Name string `yaml:"name"`
	} `yaml:"metadata"`
	Spec struct {
		Completions int `yaml:"completions"`
		Parallelism int `yaml:"parallelism"`
		Template    struct {
			Metadata struct {
				Labels struct {
					App           string `yaml:"app"`
					ApplicationID string `yaml:"applicationId"`
					Queue         string `yaml:"queue"`
					Children      string `yaml:"children"`
					ExecutionTime string `yaml:"executionTime"`
				} `yaml:"labels"`
				Name string `yaml:"name"`
			} `yaml:"metadata"`
			Spec struct {
				SchedulerName string `yaml:"schedulerName"`
				RestartPolicy string `yaml:"restartPolicy"`
				Containers    []struct {
					Name      string   `yaml:"name"`
					Image     string   `yaml:"image"`
					Command   []string `yaml:"command"`
					Resources struct {
						Requests struct {
							CPU    string `yaml:"cpu"`
							Memory string `yaml:"memory"`
						} `yaml:"requests"`
					} `yaml:"resources"`
				} `yaml:"containers"`
			} `yaml:"spec"`
		} `yaml:"template"`
	} `yaml:"spec"`
}

type KwokPod struct {
	APIVersion string `yaml:"apiVersion"`
	Kind       string `yaml:"kind"`
	Metadata   struct {
		Name   string `yaml:"name"`
		Labels struct {
			App           string `yaml:"app"`
			ApplicationID string `yaml:"applicationId"`
			Queue         string `yaml:"queue"`
			Children      string `yaml:"children"`
			ExecutionTime string `yaml:"executionTime"`
		} `yaml:"labels"`
	} `yaml:"metadata"`
	Spec struct {
		Containers []struct {
			Name      string   `yaml:"name"`
			Image     string   `yaml:"image"`
			Command   []string `yaml:"command"`
			Resources struct {
				Requests struct {
					CPU    string `yaml:"cpu"`
					Memory string `yaml:"memory"`
				} `yaml:"requests"`
			} `yaml:"resources"`
		} `yaml:"containers"`
		Tolerations []struct {
			Key      string `yaml:"key"`
			Operator string `yaml:"operator"`
			Effect   string `yaml:"effect"`
		} `yaml:"tolerations"`
	} `yaml:"spec"`
}

type AppConfig struct {
	ApplicationID string `yaml:"applicationId"`
	Podcount      int    `yaml:"podcount"`
	Edges         []Edge `yaml:"dependency"`
}

type Edge struct {
	Idx    string `yaml:"idx"`
	Weight int    `yaml:"weight"`
}

func main() {
	truncYAML()
	// rand.Seed(time.Now().UnixNano())
	rand.Seed(50)

	ranks := minRanks + rand.Intn(maxRanks-minRanks+1)
	nodes := 0
	DependencyStruct := map[int][]int{}
	appConfig := AppConfig{
		ApplicationID: "dag01",
		Podcount:      0,
		Edges:         []Edge{},
	}
	// WeightStruct := map[string]int{}

	fmt.Println("digraph {")
	for i := 0; i < ranks; i++ {
		// New nodes of 'higher' rank than all nodes generated till now.
		newNodes := minPerRank + rand.Intn(maxPerRank-minPerRank+1)

		// Edges from old nodes ('nodes') to new ones ('newNodes').
		for j := 0; j < nodes; j++ {
			for k := 0; k < newNodes; k++ {
				if rand.Intn(100) < percent {
					edge := Edge{
						Idx:    fmt.Sprintf("%d-%d", j, k+nodes),
						Weight: (rand.Intn(10) + 1) * 100,
					}
					appConfig.Edges = append(appConfig.Edges, edge)

					DependencyStruct[j] = append(DependencyStruct[j], k+nodes)
					fmt.Printf("  %d -> %d [label=\"%d\"];\n", j, k+nodes, edge.Weight) // An Edge.
				}
			}
		}

		nodes += newNodes // Accumulate into old node set.
	}
	fmt.Println("}")

	appConfig.Podcount = len(DependencyStruct)
	appConfig.createYAML()

	for idx, children := range DependencyStruct {
		formatedIdx := strconv.FormatInt(int64(idx), 10)
		formatedChildren := formantChildren(children)
		executionTime := strconv.FormatInt(int64((rand.Intn(10)+1)*30), 10) // 30~300
		cpu := strconv.FormatInt(int64(100+(rand.Intn(10)+1)*100), 10) + "m"
		memory := strconv.FormatInt(int64(100+(rand.Intn(10)+1)*100), 10) + "M"

		// job:= Job{}
		// job.createYAML(formatedIdx, formatedChildren, executionTime, cpu, memory)

		kwokPod := KwokPod{}
		kwokPod.createYAML(formatedIdx, formatedChildren, executionTime, cpu, memory)
	}

}

func truncYAML() {
	// Open the file with write permissions and the os.O_TRUNC flag to truncate the content
	file, err := os.OpenFile(filePath, os.O_WRONLY|os.O_TRUNC, 0666)
	if err != nil {
		fmt.Println("Error opening the file:", err)
		return
	}
	defer file.Close()

	// Truncate the file by setting its size to 0
	err = file.Truncate(0)
	if err != nil {
		fmt.Println("Error truncating the file:", err)
		return
	}

	// fmt.Println("File content cleared.")
}

func (j Job) getTemplate() string {
	return JobTemplate
}

func (j Job) createYAML(idx, children, executionTime, cpu, memory string) {
	file, err := ioutil.ReadFile(j.getTemplate())
	if err != nil {
		log.Fatalf("error: %v", err)
	}
	err = yaml.Unmarshal([]byte(file), &j)
	if err != nil {
		log.Fatalf("error: %v", err)
	}

	j.Metadata.Name = j.Metadata.Name + idx
	j.Spec.Template.Metadata.Name = j.Spec.Template.Metadata.Name + idx
	j.Spec.Template.Metadata.Labels.Children = children
	j.Spec.Template.Metadata.Labels.ExecutionTime = executionTime
	j.Spec.Template.Spec.Containers[0].Resources.Requests.CPU = cpu
	j.Spec.Template.Spec.Containers[0].Resources.Requests.Memory = memory

	d, err := yaml.Marshal(&j)
	if err != nil {
		log.Fatalf("error: %v", err)
	}
	// fmt.Println(string(d))

	f, err := os.OpenFile(filePath, os.O_APPEND|os.O_WRONLY, os.ModeAppend) // For read access.
	check(err)
	defer f.Close()

	_, err = f.WriteString("---\n")
	check(err)
	_, err = f.WriteString(string(d))
	check(err)

	f.Sync()

}

func (k KwokPod) getTemplate() string {
	return KwokPodTemplate
}

func (k KwokPod) createYAML(idx, children, executionTime, cpu, memory string) {
	file, err := ioutil.ReadFile(k.getTemplate())
	if err != nil {
		log.Fatalf("error: %v", err)
	}
	err = yaml.Unmarshal([]byte(file), &k)
	if err != nil {
		log.Fatalf("error: %v", err)
	}

	k.Metadata.Name = k.Metadata.Name + idx
	k.Metadata.Labels.ExecutionTime = executionTime
	k.Metadata.Labels.Children = children
	k.Spec.Containers[0].Resources.Requests.CPU = cpu
	k.Spec.Containers[0].Resources.Requests.Memory = memory

	d, err := yaml.Marshal(&k)
	if err != nil {
		log.Fatalf("error: %v", err)
	}
	// fmt.Println(string(d))

	f, err := os.OpenFile(filePath, os.O_APPEND|os.O_WRONLY, os.ModeAppend) // For read access.
	check(err)
	defer f.Close()

	_, err = f.WriteString("---\n")
	check(err)
	_, err = f.WriteString(string(d))
	check(err)

	f.Sync()

}

func (a AppConfig) createYAML() {
	d, err := yaml.Marshal(&a)
	if err != nil {
		log.Fatalf("error: %v", err)
	}

	_ = os.WriteFile(appConfigPath, d, 0644) // For read access.
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func formantChildren(children []int) string {
	result := ""
	for idx, child := range children {
		if idx != 0 {
			result += "-"
		}
		result += strconv.FormatInt(int64(child), 10)
	}
	return result
}
