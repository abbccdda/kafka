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
	ProduceTestName       string          `json:"topics_from_test"`
	Fanout                int             `json:"fanout"`
	Topics                []string        `json:"topics"`
	Duration              common.Duration `json:"duration"`
	StepMessagesPerSecond uint64          `json:"step_messages_per_second"`
	TasksPerStep          int             `json:"tasks_per_step"`
	SlowStartPerStepMs    uint64          `json:"slow_start_per_step_ms"`
	ConsumerGroup         string          `json:"consumer_group"`

	startTime time.Time
	endTime   time.Time
}

func (tc *TailConsumer) testName() string {
	return tc.Name + "-tail-consumer"
}

func (tc *TailConsumer) consumerGroup(i int) string {
	if tc.ConsumerGroup == "" {
		return fmt.Sprintf("%s-%d", tc.testName(), i)
	}
	return tc.ConsumerGroup
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
	if tc.TasksPerStep > 0 {
		taskCount = tc.TasksPerStep
	}
	spec := trogdor.ScenarioSpec{
		UsedNames: map[string]bool{},
	}

	messagesPerSecond := tc.StepMessagesPerSecond
	if messagesPerSecond == 0 {
		messagesPerSecond = math.MaxInt32
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
			ConsumerGroup: tc.consumerGroup(i),
		}
		scenConfig := trogdor.ScenarioConfig{
			ScenarioID: trogdor.TaskId{
				TaskType: tc.testName(),
				StartMs:  common.TimeToUnixMilli(startTime),
				Desc:     fmt.Sprintf("topic-%s-fanout-%d-start-%d", topic.TopicName, i, common.TimeToUnixMilli(startTime)),
			},
			Class:              trogdor.CONSUME_BENCH_SPEC_CLASS,
			TaskCount:          taskCount,
			DurationMs:         uint64(endTime.Sub(startTime) / time.Millisecond),
			StartMs:            common.TimeToUnixMilli(startTime),
			BootstrapServers:   bootstrapServers,
			AdminConf:          adminConfig,
			ClientNodes:        clientNodes,
			SlowStartPerStepMs: tc.SlowStartPerStepMs,
			ConsumerTestConfig: trogdor.ConsumerTestConfig{
				TopicSpec:       topic,
				MessagesPerSec:  messagesPerSecond,
				ConsumerOptions: consumerOptions,
			},
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
		return errors.Errorf("tail consumer %s cannot have a negative duration %s", tc.Name, tc.GetDuration())
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
	return time.Duration(tc.Duration)
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
