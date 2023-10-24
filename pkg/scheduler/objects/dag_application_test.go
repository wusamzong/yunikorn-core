package objects

import (
	"github.com/apache/yunikorn-core/pkg/scheduler/objects/custom"
	// "github.com/apache/yunikorn-scheduler-interface/lib/go/si"
	"gotest.tools/v3/assert"
	"testing"
	"strconv"
	"fmt"
)

func TestIsDagApp(t *testing.T) {
	config := custom.LoadAppConfig()
	app1 := newApplicationWithTags(config.ApplicationID, "default", "root.default", nil)
	assert.Assert(t, isDagApp(app1))

	app2 := newApplicationWithTags("yunikorn-autogen", "default", "root.default", nil)
	assert.Assert(t, !isDagApp(app2))
}

func TestCreateDagManager(t *testing.T){
	config := custom.LoadAppConfig()
	app1 := newApplicationWithTags(config.ApplicationID, "default", "root.default", nil)
	dependency := []string{"8-2", "10-3", "4-2-1", "8-4-9-5", "2-8", "7-8", "2-7-10", "2-7-1-9", "9-5", ""}  // 10, 9, 8....1
	for i:=0;i<10;i++{
		saa:= &AllocationAsk{
			tags:  map[string]string{
				"kubernetes.io/label/job-name": "task"+strconv.Itoa(10-i),
				"kubernetes.io/label/parent": dependency[i],
			},
		}
		
		app1.requests["task"+strconv.Itoa(10-i)]= saa
	}
	app1.dag=CreateDagManager(app1, true)
}

func TestParseParent(t *testing.T){
	// fmt.Println(parseParent("9-4-7-10"))
	fmt.Println(parseParent(""))
	
	fmt.Println(parseParent("9"))
}