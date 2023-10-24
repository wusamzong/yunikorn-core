package objects

import (
	"github.com/apache/yunikorn-core/pkg/log"
)

func isDagApp(appID string) bool {
	config := loadAppConfig()
	if appID == config.ApplicationID {
		log.Log(log.SchedApplication).Info("try dag")
		return true
	}
	return false
}

func allRequestWaiting() bool {

}
