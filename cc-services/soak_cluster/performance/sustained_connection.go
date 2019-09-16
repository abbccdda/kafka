package performance

import (
	"fmt"
	"time"

	"github.com/confluentinc/ce-kafka/cc-services/soak_cluster/common"
	"github.com/confluentinc/ce-kafka/cc-services/soak_cluster/trogdor"
	"github.com/pkg/errors"
)

type SustainedConnection struct {
	Name string

	Duration                common.Duration `json:"duration"`
	ProducerConnectionCount uint64          `json:"producer_connection_count"`
	ConsumerConnectionCount uint64          `json:"consumer_connection_count"`
	MetadataConnectionCount uint64          `json:"metadata_connection_count"`
	NumThreads              uint64          `json:"num_threads"`
	Fanout                  int             `json:"fanout"`
	TasksPerStep            int             `json:"tasks_per_step"`
	RefreshRateMs           uint64          `json:"refresh_rate_ms"`
	TopicName               string          `json:"topic_name"`
	MessageSizeBytes        uint64          `json:"message_size_bytes"`
	SlowStartPerStepMs      uint64          `json:"slow_start_per_step_ms"`

	startTime time.Time
	endTime   time.Time
}

func (sc *SustainedConnection) testName() string {
	return sc.Name + "-sustained-connection"
}

func (sc *SustainedConnection) CreateTest(trogdorAgentsCount int, bootstrapServers string) (tasks []trogdor.TaskSpec, err error) {
	clientNodes := common.TrogdorAgentPodNames(trogdorAgentsCount)
	common.ShuffleSlice(clientNodes)
	taskCount := len(clientNodes)
	if sc.TasksPerStep > 0 {
		taskCount = sc.TasksPerStep
	}
	startTime, err := sc.GetStartTime()
	if err != nil {
		return tasks, err
	}
	endTime, err := sc.GetEndTime()
	if err != nil {
		return tasks, err
	}
	messageSizeBytes := sc.MessageSizeBytes
	if messageSizeBytes == 0 {
		messageSizeBytes = 512
	}

	spec := trogdor.ScenarioSpec{
		UsedNames: map[string]bool{},
	}
	valueGenerator := trogdor.ValueGeneratorSpec{
		ValueType: "uniformRandom", Size: sc.MessageSizeBytes, Padding: 0,
	}

	for i := 1; i <= sc.Fanout; i++ {
		scenarioConfig := trogdor.ScenarioConfig{
			ScenarioID: trogdor.TaskId{
				TaskType: sc.testName(),
				StartMs:  common.TimeToUnixMilli(startTime),
				Desc: fmt.Sprintf(
					"%dThreads-%dProducers-%dConsumers-%dMetadata-%sDuration-%dmsRefresh",
					sc.NumThreads, sc.ProducerConnectionCount, sc.ConsumerConnectionCount, sc.MetadataConnectionCount,
					sc.GetDuration().String(), sc.RefreshRateMs,
				),
			},
			Class:              trogdor.SUSTAINED_CONNECTION_SPEC_CLASS,
			TaskCount:          taskCount,
			DurationMs:         uint64(endTime.Sub(startTime) / time.Millisecond),
			StartMs:            common.TimeToUnixMilli(startTime),
			BootstrapServers:   bootstrapServers,
			AdminConf:          adminConfig,
			ClientNodes:        clientNodes,
			SlowStartPerStepMs: sc.SlowStartPerStepMs,
			SustainedConnectionTestConfig: trogdor.SustainedConnectionTestConfig{
				ProducerConnectionCount: sc.ProducerConnectionCount,
				ConsumerConnectionCount: sc.ConsumerConnectionCount,
				MetadataConnectionCount: sc.MetadataConnectionCount,
				NumThreads:              sc.NumThreads,
				RefreshRateMs:           sc.RefreshRateMs,
				KeyGenerator:            trogdor.DefaultKeyGeneratorSpec,
				ValueGenerator:          valueGenerator,
				TopicName:               sc.TopicName,
			},
		}
		spec.CreateScenario(scenarioConfig)
		tasks = append(tasks, spec.TaskSpecs...)
	}
	return tasks, nil
}

func (sc *SustainedConnection) GetName() string {
	return sc.Name
}

func newSustainedConnection() *SustainedConnection {
	return &SustainedConnection{}
}

func (sc *SustainedConnection) validate() error {
	if (sc.Duration) < 0 {
		return errors.Errorf("%s: Duration %d cannot be negative.", sc.Name, sc.GetDuration())
	}
	if sc.ProducerConnectionCount > 0 {
		if sc.TopicName == "" {
			return errors.Errorf("%s: TopicName must be set for producer workload.", sc.Name)
		}
	}
	if sc.ConsumerConnectionCount > 0 {
		if sc.TopicName == "" {
			return errors.Errorf("%s: TopicName must be set for consumer workload.", sc.Name)
		}
	}

	return nil
}

func (sc *SustainedConnection) GetDuration() time.Duration {
	return time.Duration(sc.Duration)
}

func (sc *SustainedConnection) GetStartTime() (time.Time, error) {
	if sc.startTime.IsZero() {
		return sc.startTime, errors.New("startTime not set")
	}
	return sc.startTime, nil
}

func (sc *SustainedConnection) GetEndTime() (time.Time, error) {
	if sc.endTime.IsZero() {
		return sc.endTime, errors.New("endTime not set")
	}
	return sc.endTime, nil
}

func (sc *SustainedConnection) SetStartTime(startTime time.Time) {
	sc.startTime = startTime
}

func (sc *SustainedConnection) SetEndTime(endTime time.Time) {
	sc.endTime = endTime
}
