package performance

import (
	"github.com/pkg/errors"

	"fmt"
	"github.com/confluentinc/ce-kafka/cc-services/soak_cluster/common"
	"github.com/confluentinc/ce-kafka/cc-services/soak_cluster/trogdor"
	"math"
	"time"
)

type TailConsumer struct {
	Name string

	// the ProduceTestName must refer to a test that implements the TopicWithTests interface
	ProduceTestName string        `json:"topics_from_test"`
	Fanout          int           `json:"fanout"`
	Topics          []string      `json:"topics"`
	Duration        time.Duration `json:"duration"`

	startTime time.Time
	endTime   time.Time
}

func (tc *TailConsumer) testName() string {
	return tc.Name + "-tail-consumer"
}

func (tc *TailConsumer) CreateTest(trogdorAgentsCount int, bootstrapServers string) (tasks []trogdor.TaskSpec, err error) {
	if len(tc.Topics) != 1 {
		return tasks, errors.Errorf("tail consumer %s must define one topic exactly. it had %d", tc.Name, len(tc.Topics))
	}
	topic := trogdor.TopicSpec{
		TopicName: tc.Topics[0],
	}
	clientNodes := common.TrogdorAgentPodNames(trogdorAgentsCount)

	taskCount := len(clientNodes)
	spec := trogdor.ScenarioSpec{
		UsedNames: map[string]bool{},
	}

	startTime, err := tc.GetStartTime()
	if err != nil {
		return tasks, err
	}
	endTime, err := tc.GetEndTime()
	if err != nil {
		return tasks, err
	}

	for i := 1; i <= tc.Fanout; i++ {
		consumerOptions := trogdor.ConsumerOptions{
			ConsumerGroup: fmt.Sprintf("%s-%d", tc.testName(), i),
		}
		scenConfig := trogdor.ScenarioConfig{
			ScenarioID: trogdor.TaskId{
				TaskType: tc.testName(),
				Desc:     fmt.Sprintf("topic-%s-fanout-%d", topic.TopicName, i),
			},
			Class:            trogdor.CONSUME_BENCH_SPEC_CLASS,
			TaskCount:        taskCount,
			TopicSpec:        topic,
			DurationMs:       uint64(endTime.Sub(startTime) / time.Millisecond),
			StartMs:          common.TimeToUnixMilli(startTime),
			BootstrapServers: bootstrapServers,
			MessagesPerSec:   math.MaxInt32,
			AdminConf:        adminConfig,
			ConsumerOptions:  consumerOptions,
			ClientNodes:      clientNodes,
		}
		spec.CreateScenario(scenConfig)
		tasks = append(tasks, spec.TaskSpecs...)
	}
	return tasks, nil

}

func (tc *TailConsumer) GetName() string {
	return tc.Name
}

func newTailConsumer() *TailConsumer {
	return &TailConsumer{}
}

func (tc *TailConsumer) validate() error {
	if (tc.Duration) < 0 {
		return errors.Errorf("tail consumer %s cannot have a negative duration %s", tc.Name, tc.Duration)
	}
	if tc.ProduceTestName != "" {
		if len(tc.Topics) != 0 {
			return errors.Errorf("tail consumer %s cannot define both topics and a produce test name", tc.Name)
		}
	} else if len(tc.Topics) != 1 {
		return errors.Errorf("tail consumer %s must define one topic exactly. it had %d", tc.Name, len(tc.Topics))
	}

	return nil
}

func (tc *TailConsumer) GetDuration() time.Duration {
	return tc.Duration
}

func (tc *TailConsumer) GetStartTime() (time.Time, error) {
	if tc.startTime.IsZero() {
		return tc.startTime, errors.New("startTime not set")
	}
	return tc.startTime, nil
}

func (tc *TailConsumer) GetEndTime() (time.Time, error) {
	if tc.endTime.IsZero() {
		return tc.endTime, errors.New("endTime not set")
	}
	return tc.endTime, nil
}

func (tc *TailConsumer) SetStartTime(startTime time.Time) {
	tc.startTime = startTime
}

func (tc *TailConsumer) SetEndTime(endTime time.Time) {
	tc.endTime = endTime
}
