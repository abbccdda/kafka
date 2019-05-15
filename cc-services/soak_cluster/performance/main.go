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
	adminConfig = trogdor.AdminConf{
		AutoOffsetReset: "latest", // default value, will be overridden if specified
	}
	adminConfPath = os.Getenv("TROGDOR_ADMIN_CONF")
)

// ScenarioContext holds the tests that are parsed for this scenario run
type ScenarioContext struct {
	TestsWithTopics  map[string]TestWithTopics
	SchedulableTests map[string]SchedulableTest
}

func newScenarioContext() *ScenarioContext {
	return &ScenarioContext{
		TestsWithTopics:  make(map[string]TestWithTopics),
		SchedulableTests: make(map[string]SchedulableTest),
	}
}

func (sc *ScenarioContext) addTest(test interface{}) {
	if st, ok := test.(SchedulableTest); ok {
		sc.AddSchedulableTest(st)
	}
	if twt, ok := test.(TestWithTopics); ok {
		sc.AddTestWithTopics(twt)
	}
}

// the NotEnoughContextError error indicates that a test needs more than the provided scenario context to be parsed correctly
type NotEnoughContextError struct {
	msg string
}

func newNotEnoughContextError(msg string) error {
	return &NotEnoughContextError{msg}
}

func (nec *NotEnoughContextError) Error() string {
	return nec.msg
}

func (sc *ScenarioContext) AddTestWithTopics(twt TestWithTopics) {
	sc.TestsWithTopics[twt.GetName()] = twt
}

func (sc *ScenarioContext) AddSchedulableTest(st SchedulableTest) {
	sc.SchedulableTests[st.GetName()] = st
}

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

	err = testConfig.parseTests()
	if err != nil {
		logutil.Error(logger, "error while parsing tests - %s", err)
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
