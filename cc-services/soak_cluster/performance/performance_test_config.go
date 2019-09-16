package performance

import (
	"encoding/json"
	"fmt"
	"github.com/confluentinc/ce-kafka/cc-services/soak_cluster/common"
	"github.com/confluentinc/ce-kafka/cc-services/soak_cluster/trogdor"
	"github.com/pkg/errors"
	"time"
)

// PerformanceTestConfig is a generic definition of a test.
// It it meant to support different types of tests, each of which define their own set of test_parameters
// Each test should implement the SchedulableTest interface
type PerformanceTestConfig struct {
	Type       string          `json:"test_type"`
	Name       string          `json:"test_name"`
	Parameters json.RawMessage `json:"test_parameters"`

	schedulableTest SchedulableTest
	job             *common.SchedulableJob

	// determines whether ParseTest() was called successfully
	parsed bool
}

// SchedulableTest is an interface for a test that is schedulable.
// To be eligible for scheduling, the test should have a known duration time or be scheduled to run until a test with a known duration time.
// After the scheduling is determined, the start and end times of the test
// 	will be set via the appropriate methods
type SchedulableTest interface {
	// CreateTest() should return Trogdor task specifications that compose the whole test.
	// Said tasks should start no earlier than GetStartTime(),
	// should have at least one tasks that ends at GetEndTime() and none ending later than that.
	CreateTest(trogdorAgentsCount int, bootstrapServers string) ([]trogdor.TaskSpec, error)

	GetName() string

	// returns the duration of the test. If the test is scheduled to run until another test, this method should return 0
	GetDuration() time.Duration
	// returns an error if StartTime is not set
	GetStartTime() (time.Time, error)
	// returns an error if EndTime is not set
	GetEndTime() (time.Time, error)

	SetStartTime(time.Time)
	SetEndTime(time.Time)
}

// TestWithTopics is an interface for a test that makes use of topics.
type TestWithTopics interface {
	GetName() string
	// TopicNames() should return all the topics this test will use
	TopicNames() []string
}

const PROGRESSIVE_WORKLOAD_TEST_TYPE = "ProgressiveWorkload"
const TAIL_CONSUMER_TEST_TYPE = "TailConsume"
const CONNECTION_STRESS_TEST_TYPE = "ConnectionStress"
const SUSTAINED_CONNECTION_TEST_TYPE = "SustainedConnection"

func (ptc *PerformanceTestConfig) CreateTest(trogdorAgentsCount int, bootstrapServers string) ([]trogdor.TaskSpec, error) {
	return ptc.schedulableTest.CreateTest(trogdorAgentsCount, bootstrapServers)
}

// ParseTest() parses the configuration into the concrete test struct
// it can return a retriable error of type NotEnoughContext which means that we should try parsing this test
// again when we have more context from the scenario
// Parsing will be done only once, if successful
func (ptc *PerformanceTestConfig) ParseTest(context *ScenarioContext) error {
	if ptc.parsed {
		return nil
	}

	switch ptc.Type {
	case PROGRESSIVE_WORKLOAD_TEST_TYPE:
		progressiveWorkload := newWorkload()
		err := json.Unmarshal(ptc.Parameters, progressiveWorkload)
		if err != nil {
			return errors.Wrapf(err, "error while trying to parse progressive workload test parameters")
		}
		progressiveWorkload.Name = ptc.Name
		err = progressiveWorkload.validate()
		if err != nil {
			return errors.Wrapf(err, "error while validating progressive workload")
		}
		ptc.schedulableTest = progressiveWorkload

	case TAIL_CONSUMER_TEST_TYPE:
		tailConsumer := newTailConsumer()
		err := json.Unmarshal(ptc.Parameters, tailConsumer)
		if err != nil {
			return errors.Wrapf(err, "error while trying to parse tail consumer %s test parameters", tailConsumer.Name)
		}
		tailConsumer.Name = ptc.Name
		err = tailConsumer.validate()
		if err != nil {
			return errors.Wrapf(err, "error while validating tail consumer workload")
		}

		if tailConsumer.ProduceTestName != "" {
			produceTest := context.TestsWithTopics[tailConsumer.ProduceTestName]
			if produceTest == nil {
				return newNotEnoughContextError(fmt.Sprintf("could not find test %s to get topics from", tailConsumer.ProduceTestName))
			}

			tailConsumer.Topics = produceTest.TopicNames()
		}

		ptc.schedulableTest = tailConsumer

	case CONNECTION_STRESS_TEST_TYPE:
		connectionStressWorkload := newConnectionStress()
		err := json.Unmarshal(ptc.Parameters, connectionStressWorkload)
		if err != nil {
			return errors.Wrapf(err, "error while trying to parse connection stress test parameters")
		}
		connectionStressWorkload.Name = ptc.Name
		err = connectionStressWorkload.validate()
		if err != nil {
			return errors.Wrapf(err, "error while validating connection stress workload")
		}
		ptc.schedulableTest = connectionStressWorkload

	case SUSTAINED_CONNECTION_TEST_TYPE:
		sustainedConnectionWorkload := newSustainedConnection()
		err := json.Unmarshal(ptc.Parameters, sustainedConnectionWorkload)
		if err != nil {
			return errors.Wrapf(err, "error while trying to parse sustained connection test parameters")
		}
		sustainedConnectionWorkload.Name = ptc.Name
		err = sustainedConnectionWorkload.validate()
		if err != nil {
			return errors.Wrapf(err, "error while validating sustained connection workload")
		}
		ptc.schedulableTest = sustainedConnectionWorkload

	default:
		return fmt.Errorf("test type %s is not supported", ptc.Type)
	}
	ptc.parsed = true
	return nil
}

// prepareForScheduling() parses the test to get the necessary parameters from it needed for scheduling - namely, duration
func (ptc *PerformanceTestConfig) prepareForScheduling(context *ScenarioContext) error {
	err := ptc.ParseTest(context)
	if err != nil {
		return err
	}
	err = ptc.createSchedulableJob()
	if err != nil {
		return err
	}
	return nil
}

func (ptc *PerformanceTestConfig) getSchedulableJob() (*common.SchedulableJob, error) {
	var err error
	if ptc.job == nil {
		err = ptc.createSchedulableJob()
	}

	return ptc.job, err
}

func (ptc *PerformanceTestConfig) createSchedulableJob() error {
	if ptc.schedulableTest == nil {
		return errors.New("the test needs to be parsed first before creating its job")
	}
	if ptc.job == nil {
		ptc.job = &common.SchedulableJob{
			Name:     ptc.Name,
			Duration: ptc.schedulableTest.GetDuration(),
		}
	}

	return nil
}
