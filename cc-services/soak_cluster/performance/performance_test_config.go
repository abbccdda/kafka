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
}

// SchedulableTest is an interface for a test that is schedulable.
// To be eligible for scheduling, the test should have a known duration time.
// After the scheduling is determined, the start and end times of the test
// 	will be set via the appropriate methods
type SchedulableTest interface {
	// CreateTest() should return Trogdor task specifications that compose the whole test.
	// Said tasks should start no earlier than GetStartTime(),
	// should have at least one tasks that ends at GetEndTime() and none ending later than that.
	CreateTest(trogdorAgentsCount int, bootstrapServers string) ([]trogdor.TaskSpec, error)

	GetName() string

	GetDuration() time.Duration
	// returns an error if StartTime is not set
	GetStartTime() (time.Time, error)
	// returns an error if EndTime is not set
	GetEndTime() (time.Time, error)

	SetStartTime(time.Time)
	SetEndTime(time.Time)
}

const PROGRESSIVE_WORKLOAD_TEST_TYPE = "ProgressiveWorkload"

func (ptc *PerformanceTestConfig) CreateTest(trogdorAgentsCount int, bootstrapServers string) ([]trogdor.TaskSpec, error) {
	return ptc.schedulableTest.CreateTest(trogdorAgentsCount, bootstrapServers)
}

// parseTest() parses the configuration into the concrete test struct
func (ptc *PerformanceTestConfig) parseTest() error {
	switch ptc.Type {
	case PROGRESSIVE_WORKLOAD_TEST_TYPE:
		progressiveWorkload := newWorkload()
		err := json.Unmarshal(ptc.Parameters, progressiveWorkload)
		if err != nil {
			return errors.Wrapf(err, "error while trying to parse progressive workload test parameters")
		}
		progressiveWorkload.Name = ptc.Name
		ptc.schedulableTest = progressiveWorkload
	default:
		return fmt.Errorf("test type %s is not supported", ptc.Type)
	}
	return nil
}

// prepareForScheduling() parses the test to get the necessary parameters from it needed for scheduling - namely, duration
func (ptc *PerformanceTestConfig) prepareForScheduling() error {
	err := ptc.parseTest()
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
