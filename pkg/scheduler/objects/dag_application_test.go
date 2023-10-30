package objects

import (
	"github.com/apache/yunikorn-core/pkg/scheduler/objects/custom"
	// "github.com/apache/yunikorn-scheduler-interface/lib/go/si"
	"fmt"
	"gotest.tools/v3/assert"
	"strconv"
	"testing"
)

func TestIsDagApp(t *testing.T) {
	config := custom.LoadAppConfig()
	app1 := newApplicationWithTags(config.ApplicationID, "default", "root.default", nil)
	assert.Assert(t, isDagApp(app1))

	app2 := newApplicationWithTags("yunikorn-autogen", "default", "root.default", nil)
	assert.Assert(t, !isDagApp(app2))
}

func TestCreateDagManager(t *testing.T) {
	config := custom.LoadAppConfig()
	app1 := newApplicationWithTags(config.ApplicationID, "default", "root.default", nil)
	dependency := []string{"", "10", "9", "8", "7", "6", "5", "4", "3", "2"} // 10, 9, 8....1
	for i := 0; i < 10; i++ {
		saa := &AllocationAsk{
			tags: map[string]string{
				"kubernetes.io/label/job-name":      "task" + strconv.Itoa(10-i),
				"kubernetes.io/label/children":      dependency[i],
				"kubernetes.io/label/executionTime": "300",
			},
		}

		app1.requests["task"+strconv.Itoa(10-i)] = saa
	}
	app1.dag = CreateDagManager(app1, true)
}

func TestParseChildren(t *testing.T) {
	// fmt.Println(parseParent("9-4-7-10"))
	fmt.Println(parseChildren(""))

	fmt.Println(parseChildren("9"))
}
