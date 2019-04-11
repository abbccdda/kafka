package performance

import (
	"errors"
	"fmt"
	logutil "github.com/confluentinc/cc-utils/log"
	"github.com/confluentinc/ce-kafka/cc-services/soak_cluster/common"
	"github.com/confluentinc/ce-kafka/cc-services/soak_cluster/trogdor"
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
}

const PRODUCE_WORKLOAD_TYPE = "Produce"

var defaultProducerOptions = trogdor.ProducerOptions{
	ValueGenerator: trogdor.ValueGeneratorSpec{ValueType: "uniformRandom", Size: 900, Padding: 100},
	KeyGenerator:   trogdor.DefaultKeyGeneratorSpec,
}

// Returns all the Trogdor tasks that should be ran as part of this workload
func (workload *Workload) CreateWorkload(trogdorAgentsCount int, bootstrapServers string) ([]trogdor.TaskSpec, error) {
	tasks := []trogdor.TaskSpec{}
	topicSpec := &trogdor.TopicSpec{
		NumPartitions:     workload.PartitionCount,
		ReplicationFactor: 3,
		TopicName:         workload.Name + "-topic",
	}
	clientNodes := common.TrogdorAgentPodNames(trogdorAgentsCount)

	var step uint32
	lastStep := &Step{
		endMs: uint64(time.Now().UnixNano()/int64(time.Millisecond)) - workload.StepCooldownMs,
	}
	throughputMbs := workload.StartThroughputMbs
	for {
		if step != 0 {
			throughputMbs += workload.ThroughputIncreaseMbs
		}
		startMs := workload.StepCooldownMs + lastStep.endMs
		currentStep := &Step{
			number:        step,
			throughputMbs: throughputMbs,
			startMs:       startMs,
			endMs:         startMs + workload.StepDurationMs,
			workload:      workload,
		}
		newTasks, err := currentStep.tasks(topicSpec, clientNodes, bootstrapServers)
		if err != nil {
			return []trogdor.TaskSpec{}, err
		}
		tasks = append(tasks, newTasks...)
		lastStep = currentStep
		step += 1
		if throughputMbs >= workload.EndThroughputMbs {
			break
		}
	}
	logutil.Info(logger, "Created %d steps for a progressive workload starting at %f MB/s, ending at %f MB/s",
		step, workload.StartThroughputMbs, workload.EndThroughputMbs)

	return tasks, nil
}

// a Step is a part of a Workload. It is to be converted to multiple Trogdor tasks which in combination achieve the desired throughput
type Step struct {
	throughputMbs float32
	startMs       uint64
	endMs         uint64
	number        uint32
	workload      *Workload
}

// Returns all the Trogdor tasks that should be ran as part of this workload step
func (step *Step) tasks(topicSpec *trogdor.TopicSpec, clientNodes []string, bootstrapServers string) ([]trogdor.TaskSpec, error) {
	var err error
	taskCount := len(clientNodes)
	spec := trogdor.ScenarioSpec{
		UsedNames: map[string]bool{},
	}

	workloadType := step.workload.Type
	switch workloadType {
	case PRODUCE_WORKLOAD_TYPE:
		{
			messageSize := step.workload.MessageSizeBytes
			var producerOptions trogdor.ProducerOptions
			if messageSize == 0 {
				producerOptions = defaultProducerOptions
			} else {
				producerOptions = trogdor.ProducerOptions{
					ValueGenerator: trogdor.ValueGeneratorSpec{ValueType: "uniformRandom", Size: messageSize, Padding: 100},
					KeyGenerator:   trogdor.DefaultKeyGeneratorSpec,
				}
			}

			stepScenario := trogdor.ScenarioConfig{
				ScenarioID: trogdor.TaskId{
					TaskType: "Produce Workload",
					Desc:     fmt.Sprintf("Step %d", step.number),
				},
				Class:            trogdor.PRODUCE_BENCH_SPEC_CLASS,
				TaskCount:        taskCount,
				TopicSpec:        *topicSpec,
				DurationMs:       step.endMs - step.startMs,
				StartMs:          step.startMs,
				BootstrapServers: bootstrapServers,
				MessagesPerSec:   producerOptions.MessagesPerSec(step.throughputMbs),
				AdminConf:        adminConfig,
				ProducerOptions:  producerOptions,
				ClientNodes:      clientNodes,
			}
			spec.CreateScenario(stepScenario)
		}
	default:
		err = errors.New(fmt.Sprintf("workload type %s is not supported", workloadType))
	}

	return spec.TaskSpecs, err
}
