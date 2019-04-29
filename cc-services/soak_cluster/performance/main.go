package performance

import (
	logutil "github.com/confluentinc/cc-utils/log"
	"github.com/confluentinc/ce-kafka/cc-services/soak_cluster/common"
	"github.com/confluentinc/ce-kafka/cc-services/soak_cluster/trogdor"
	"github.com/go-kit/kit/log"
	"os"
	"time"
)

var logger log.Logger

var (
	adminConfig   = trogdor.AdminConf{}
	adminConfPath = os.Getenv("TROGDOR_ADMIN_CONF")
)

func Run(testConfigPath string, trogdorCoordinatorHost string, trogdorAgentsCount int, bootstrapServers string) {
	logger = common.InitLogger("performance-tests")
	err := adminConfig.ParseConfig(adminConfPath)
	if err != nil {
		logutil.Error(logger, "error while parsing admin config - %s", err)
		panic(err)
	}

	testConfig := newScenarioTestConfig()
	err = testConfig.ParseConfig(testConfigPath)
	if err != nil {
		logutil.Error(logger, "error while parsing scenario test config - %s", err)
		panic(err)
	}
	err = testConfig.CreateSchedules(time.Now())
	if err != nil {
		logutil.Error(logger, "error while scheduling tests - %s", err)
		panic(err)
	}

	tasks, err := testConfig.CreateTests(trogdorAgentsCount, bootstrapServers)
	if err != nil {
		logutil.Error(logger, "error while creating tests - %s", err)
		panic(err)
	}

	logutil.Info(logger, "Parsed and scheduled tests successfully")
	common.ScheduleTrogdorTasks(logger, tasks, trogdorCoordinatorHost)
}
