package objects

import (
	"github.com/apache/yunikorn-core/pkg/scheduler/drheft"
	"github.com/apache/yunikorn-core/pkg/common/security"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
	"gotest.tools/v3/assert"
	"testing"
)

func TestIsDagApp(t *testing.T) {
	config := loadAppConfig()
	app1 := newApplicationWithTags(config.ApplicationID, "default", nil, "")
	assert.Assert(t, isDagApp(app1.ApplicationID))

	app2 := newApplicationWithTags("yunikorn-autogen", "default", nil, "")
	assert.Assert(t, isDagApp(app1.ApplicationID))
}

func newApplication(appID, partition, queueName string) *Application {
	tags := make(map[string]string)
	return newApplicationWithTags(appID, partition, queueName, tags)
}

func newApplicationWithTags(appID, partition, queueName string, tags map[string]string) *Application {
	siApp := &si.AddApplicationRequest{
		ApplicationID: appID,
		QueueName:     queueName,
		PartitionName: partition,
	}

	return NewApplication(siApp, getTestUserGroup(), nil, "")
}

func getTestUserGroup() security.UserGroup {
	return security.UserGroup{User: "testuser", Groups: []string{"testgroup"}}
}
