package performance

import (
	"fmt"
	logutil "github.com/confluentinc/cc-utils/log"
	"github.com/confluentinc/ce-kafka/cc-services/soak_cluster/common"
	"github.com/confluentinc/ce-kafka/cc-services/soak_cluster/trogdor"
	"github.com/pkg/errors"
	"time"
)

type Workload struct {
	Name                  string
	Type                  string  `json:"workload_type"`
	PartitionCount        uint64  `json:"partition_count"`
	StepDurationMs        uint64  `json:"step_duration_ms"`
	StepCooldownMs        uint64  `json:"step_cooldown_ms"`
	StartThroughputMbs    float32 `json:"start_throughput_mbs"`
	EndThroughputMbs      float32 `json:"end_throughput_mbs"`
	ThroughputIncreaseMbs float32 `json:"throughput_increase_per_step_mbs"`
	MessageSizeBytes      uint64  `json:"message_size_bytes"`
	MessagePaddingBytes   uint64  `json:"message_padding_bytes"`
	TasksPerStep          int     `json:"tasks_per_step"`
	SlowStartPerStepMs    uint64  `json:"slow_start_per_step_ms"`
	TopicName             string  `json:"topic_name"`

	steps     []*Step
	duration  time.Duration
	startTime time.Time
	endTime   time.Time
}

const PRODUCE_WORKLOAD_TYPE = "Produce"

var defaultProducerOptions = trogdor.ProducerOptions{
	ValueGenerator: trogdor.ValueGeneratorSpec{ValueType: "uniformRandom", Size: 900, Padding: 100},
	KeyGenerator:   trogdor.DefaultKeyGeneratorSpec,
}

func (workload *Workload) GetName() string {
	return workload.Name
}
func (workload *Workload) GetDuration() time.Duration {
	if workload.duration == 0 {
		_, duration := workload.getSteps(time.Now())
		workload.duration = duration
	}

	return workload.duration
}

func (workload *Workload) TopicNames() []string {
	return []string{workload.topicName()}
}

func (workload *Workload) topicName() string {
	if workload.TopicName == "" {
		return workload.Name + "-topic"
	}
	return workload.TopicName
}

// creates all the Steps that this Workload will consist of
func (workload *Workload) populateSteps(startTime time.Time) {
	steps, duration := workload.getSteps(startTime)
	workload.steps = steps
	workload.duration = duration
}

// returns all the steps that make up this workload and their total duration ms
func (workload *Workload) getSteps(startTime time.Time) ([]*Step, time.Duration) {
	var step uint32
	steps := make([]*Step, 0)
	totalDuration := time.Duration(0)
	cooldownDuration := time.Duration(workload.StepCooldownMs) * time.Millisecond
	stepDuration := time.Duration(workload.StepDurationMs) * time.Millisecond

	lastStep := &Step{
		endTime: startTime.Add(-cooldownDuration),
	}
	throughputMbs := workload.StartThroughputMbs
	for {
		if step != 0 {
			throughputMbs += workload.ThroughputIncreaseMbs
			totalDuration += cooldownDuration
		}
		startTime := lastStep.endTime.Add(cooldownDuration)
		currentStep := &Step{
			number:        step,
			throughputMbs: throughputMbs,
			startTime:     startTime,
			endTime:       startTime.Add(stepDuration),
			workload:      workload,
		}
		totalDuration += stepDuration
		steps = append(steps, currentStep)

		lastStep = currentStep
		step += 1
		if throughputMbs >= workload.EndThroughputMbs {
			break
		}
	}

	return steps, totalDuration
}

// Returns all the Trogdor tasks that should be ran as part of this workload
func (workload *Workload) CreateTest(trogdorAgentsCount int, bootstrapServers string) (tasks []trogdor.TaskSpec, err error) {
	topicSpec := &trogdor.TopicSpec{
		TopicName: workload.topicName(),
		PartitionsSpec: &trogdor.PartitionsSpec{
			NumPartitions:        uint64(workload.PartitionCount),
			ReplicationFactor:    3,
			PartitionsSpecConfig: adminConfig.ToPartitionSpecConfig(),
		},
	}
	clientNodes := common.TrogdorAgentPodNames(trogdorAgentsCount)

	startTime, err := workload.GetStartTime()
	if err != nil {
		return tasks, errors.Wrapf(err, "%s", workload.Name)
	}
	endTime, err := workload.GetEndTime()
	if err != nil {
		return tasks, errors.Wrapf(err, "%s", workload.Name)
	}

	workload.populateSteps(startTime)
	workloadEndTime := workload.steps[len(workload.steps)-1].endTime
	if workloadEndTime != endTime {
		return tasks, fmt.Errorf("the last step of workload %s should end at %s but it ends at %s", workload.Name, endTime, workloadEndTime)
	}

	for _, step := range workload.steps {
		newTasks, err := step.tasks(topicSpec, clientNodes, bootstrapServers)
		if err != nil {
			return tasks, errors.Wrapf(err, "%s", workload.Name)
		}
		tasks = append(tasks, newTasks...)
	}
	logutil.Info(logger, "Created %d steps for a progressive workload starting at %f MB/s, ending at %f MB/s",
		len(workload.steps), workload.StartThroughputMbs, workload.EndThroughputMbs)

	return tasks, nil
}

// a Step is a part of a Workload. It is to be converted to multiple Trogdor tasks which in combination achieve the desired throughput
type Step struct {
	throughputMbs float32
	startTime     time.Time
	endTime       time.Time
	number        uint32
	workload      *Workload
}

// Returns all the Trogdor tasks that should be ran as part of this workload step
func (step *Step) tasks(topicSpec *trogdor.TopicSpec, clientNodes []string, bootstrapServers string) ([]trogdor.TaskSpec, error) {
	var err error
	taskCount := len(clientNodes)
	if step.workload.TasksPerStep > 0 {
		taskCount = step.workload.TasksPerStep
	}
	spec := trogdor.ScenarioSpec{
		UsedNames: map[string]bool{},
	}

	workloadType := step.workload.Type
	switch workloadType {
	case PRODUCE_WORKLOAD_TYPE:
		{
			messageSize := step.workload.MessageSizeBytes
			messagePadding := step.workload.MessagePaddingBytes
			var producerOptions trogdor.ProducerOptions
			if messageSize == 0 {
				producerOptions = defaultProducerOptions
			} else {
				producerOptions = trogdor.ProducerOptions{
					ValueGenerator: trogdor.ValueGeneratorSpec{ValueType: "uniformRandom", Size: messageSize, Padding: messagePadding},
					KeyGenerator:   trogdor.DefaultKeyGeneratorSpec,
				}
			}

			stepScenario := trogdor.ScenarioConfig{
				ScenarioID: trogdor.TaskId{
					TaskType: step.workload.Name + "-produce-workload",
					StartMs:  common.TimeToUnixMilli(step.startTime),
					Desc:     fmt.Sprintf("topic-%s-step-%d-start-%d", topicSpec.TopicName, step.number, common.TimeToUnixMilli(step.startTime)),
				},
				Class:              trogdor.PRODUCE_BENCH_SPEC_CLASS,
				TaskCount:          taskCount,
				TopicSpec:          *topicSpec,
				DurationMs:         uint64(step.endTime.Sub(step.startTime) / time.Millisecond),
				StartMs:            common.TimeToUnixMilli(step.startTime),
				BootstrapServers:   bootstrapServers,
				MessagesPerSec:     producerOptions.MessagesPerSec(step.throughputMbs),
				AdminConf:          adminConfig,
				ProducerOptions:    producerOptions,
				ClientNodes:        clientNodes,
				SlowStartPerStepMs: step.workload.SlowStartPerStepMs,
			}
			spec.CreateScenario(stepScenario)
		}
	default:
		err = fmt.Errorf("workload type %s is not supported", workloadType)
	}

	return spec.TaskSpecs, err
}

func newWorkload() *Workload {
	return &Workload{
		MessagePaddingBytes: trogdor.DefaultValueGeneratorSpec.Padding,
	}
}

func (workload *Workload) validate() error {
	if workload.StepDurationMs < 0 {
		return errors.Errorf(
			"progressive workload %s cannot have a negative step duration %d",
			workload.Name, workload.StepDurationMs,
		)
	}
	if workload.MessagePaddingBytes > 0 && workload.MessageSizeBytes == 0 {
		return errors.Errorf(
			"progressive workload %s cannot have message_padding_bytes specified when message_size_bytes is %d",
			workload.Name, workload.MessageSizeBytes,
		)
	}
	return nil
}

func (workload *Workload) GetStartTime() (time.Time, error) {
	if workload.startTime.IsZero() {
		return workload.startTime, errors.New("startTime not set")
	}
	return workload.startTime, nil
}

func (workload *Workload) GetEndTime() (time.Time, error) {
	if workload.endTime.IsZero() {
		return workload.endTime, errors.New("endTime not set")
	}
	return workload.endTime, nil
}

func (workload *Workload) SetStartTime(startTime time.Time) {
	workload.startTime = startTime
}

func (workload *Workload) SetEndTime(endTime time.Time) {
	workload.endTime = endTime
}
