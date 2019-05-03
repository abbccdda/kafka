package soak_clients

import (
	"encoding/json"
	"fmt"
	logutil "github.com/confluentinc/cc-utils/log"
	"github.com/confluentinc/ce-kafka/cc-services/soak_cluster/common"
	"github.com/confluentinc/ce-kafka/cc-services/soak_cluster/trogdor"
	"strings"

	"github.com/dariubs/percent"
	"github.com/jinzhu/copier"
	"github.com/pkg/errors"
	"io/ioutil"
	"math/rand"
	"time"
)

type SoakTestConfig struct {
	Topics                          []TopicConfiguration `json:"topics"`
	LongLivedTaskDurationMs         uint64               `json:"long_lived_task_duration_ms"`
	ShortLivedTaskDurationMs        uint64               `json:"short_lived_task_duration_ms"`
	ShortLivedTaskRescheduleDelayMs uint64               `json:"short_lived_task_reschedule_delay_ms"`
}

func (t *SoakTestConfig) parseConfig(configPath string) error {
	raw, err := ioutil.ReadFile(configPath)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("failed reading the soak test configuration from %s", configPath))
	}

	err = json.Unmarshal(raw, &t)
	if err != nil {
		return err
	}

	topicNames := make([]string, len(t.Topics))
	for i, topicConfig := range t.Topics {
		topicNames[i] = topicConfig.Name
	}
	logutil.Info(logger, fmt.Sprintf("Loaded configuration for topics %s", strings.Join(topicNames, ",")))
	return nil
}

type TopicConfiguration struct {
	Name                   string  `json:"name"`
	PartitionsCount        int     `json:"partitions_count"`
	ProduceMBsThroughput   float32 `json:"produce_mbs_throughput"`
	ConsumeMBsThroughput   float32 `json:"consume_mbs_throughput"`
	LongLivedProduceCount  int     `json:"long_lived_producer_count"`
	ShortLivedProduceCount int     `json:"short_lived_producer_count"`
	LongLivedConsumeCount  int     `json:"long_lived_consumer_count"`
	ShortLivedConsumeCount int     `json:"short_lived_consumer_count"`
	TransactionsEnabled    bool    `json:"transactions_enabled"`
	IdempotenceEnabled     bool    `json:"idempotence_enabled"`
	ShortLivedRandomConsumerGroup bool `json:"short_lived_random_consumer_group"`
	LongLivedRandomConsumerGroup bool `json:"long_lived_random_consumer_group"`
}

func (topicConfig *TopicConfiguration) totalProduceCount() int {
	return topicConfig.ShortLivedProduceCount + topicConfig.LongLivedProduceCount
}

func (topicConfig *TopicConfiguration) totalConsumeCount() int {
	return topicConfig.ShortLivedConsumeCount + topicConfig.LongLivedConsumeCount
}

func (topicConfig *TopicConfiguration) longLivedProduceTaskThroughput() float32 {
	return topicConfig.ProduceMBsThroughput * percentOf(topicConfig.LongLivedProduceCount, topicConfig.totalProduceCount())
}

func (topicConfig *TopicConfiguration) shortLivedProduceTaskThroughput() float32 {
	return topicConfig.ProduceMBsThroughput * percentOf(topicConfig.ShortLivedProduceCount, topicConfig.totalProduceCount())
}

func (topicConfig *TopicConfiguration) longLivedConsumeTaskThroughput() float32 {
	return topicConfig.ConsumeMBsThroughput * percentOf(topicConfig.LongLivedConsumeCount, topicConfig.totalConsumeCount())
}

func (topicConfig *TopicConfiguration) shortLivedConsumeTaskThroughput() float32 {
	return topicConfig.ConsumeMBsThroughput * percentOf(topicConfig.ShortLivedConsumeCount, topicConfig.totalConsumeCount())
}

var baseProducerOptions = trogdor.ProducerOptions{
	ValueGenerator: trogdor.DefaultValueGeneratorSpec, // 1000 bytes size
	KeyGenerator:   trogdor.DefaultKeyGeneratorSpec,
}
var transactionalProducerOptions = trogdor.ProducerOptions{
	ValueGenerator:       trogdor.DefaultValueGeneratorSpec, // 1000 bytes size
	KeyGenerator:         trogdor.DefaultKeyGeneratorSpec,
	TransactionGenerator: trogdor.DefaultTransactionGeneratorSpec,
}

// Returns all the baseline tasks that should be ran on the Soak Cluster at all times
// trogdorAgentsCount - the number of trogdor agents,
// 	this should be the same as the replicas field in agentStatefulSet.yaml
func baselineTasks(soakConfigPath string, trogdorAgentsCount int, bootstrapServers string) ([]trogdor.TaskSpec, error) {
	var tasks []trogdor.TaskSpec
	clientNodes := common.TrogdorAgentPodNames(trogdorAgentsCount)

	configuration := SoakTestConfig{}
	err := configuration.parseConfig(soakConfigPath)
	if err != nil {
		return []trogdor.TaskSpec{}, err
	}
	existingIDs := make(map[string]bool)
	for _, topicConfig := range configuration.Topics {
		newTasks := createTopicTasks(topicConfig, clientNodes, existingIDs,
			configuration.LongLivedTaskDurationMs,
			configuration.ShortLivedTaskDurationMs,
			configuration.ShortLivedTaskRescheduleDelayMs,
			bootstrapServers)
		tasks = append(tasks, newTasks...)
	}

	return tasks, nil
}

// consumerOptions will return Trogdor ConsumerOptions for the given topicName.
// if randomGroup is true, we will leave the group name blank, as the Trogdor agent
// will assign a random groupID in this case.
func consumerOptions(topicName string, randomGroup bool) trogdor.ConsumerOptions {
	consumerGroupName := fmt.Sprintf("Consume%sTestGroup", topicName)
	if randomGroup {
		consumerGroupName = ""
	}
	return trogdor.ConsumerOptions{
		ConsumerGroup: consumerGroupName,
	}
}

// Creates Trogdor Produce and Consume Bench Tasks from a TopicConfiguration
// short-lived tasks are scheduled to run up until the long-lived tasks finish, taking into account a delay in re-scheduling
func createTopicTasks(topicConfig TopicConfiguration, clientNodes []string, existingTaskIDs map[string]bool,
	longLivedMs uint64, shortLivedMs uint64, shortLivedReschedDelayMs uint64, bootstrapServers string) []trogdor.TaskSpec {
	var tasks []trogdor.TaskSpec
	topic := trogdor.TopicSpec{
		NumPartitions:     uint64(topicConfig.PartitionsCount),
		ReplicationFactor: 3,
		TopicName:         topicConfig.Name,
	}
	logutil.Debug(logger, "Creating tasks for topic configuration: %+v", topicConfig)
	var producerOptions trogdor.ProducerOptions
	if topicConfig.TransactionsEnabled {
		producerOptions = transactionalProducerOptions
	} else {
		producerOptions = baseProducerOptions
	}

	var producerAdminConfig = trogdor.AdminConf{}
	copier.Copy(&producerAdminConfig, &adminConfig)
	if topicConfig.IdempotenceEnabled {
		producerAdminConfig.EnableIdempotence = "true"
	}


	longLivedConsumerOptions := consumerOptions(topicConfig.Name, topicConfig.LongLivedRandomConsumerGroup)
	shortLivedConsumerOptions := consumerOptions(topicConfig.Name, topicConfig.ShortLivedRandomConsumerGroup)

	nowMs := uint64(time.Now().UnixNano() / int64(time.Millisecond))

	longLivingProducersScenarioConfig := trogdor.ScenarioConfig{
		ScenarioID: trogdor.TaskId{
			TaskType: "LongLivedProduce",
			Desc:     topic.TopicName,
			StartMs:  nowMs,
		},
		Class:            trogdor.PRODUCE_BENCH_SPEC_CLASS,
		TaskCount:        topicConfig.LongLivedProduceCount,
		TopicSpec:        topic,
		DurationMs:       longLivedMs,
		StartMs:          0, // start immediately
		BootstrapServers: bootstrapServers,
		MessagesPerSec:   messagesPerSec(topicConfig.longLivedProduceTaskThroughput(), producerOptions),
		AdminConf:        producerAdminConfig,
		ProducerOptions:  producerOptions,
		ClientNodes:      shuffleSlice(clientNodes),
	}
	logutil.Debug(logger, "longLivingProducersScenarioConfig: %+v", longLivingProducersScenarioConfig)
	longLivingProducersScenario := &trogdor.ScenarioSpec{
		UsedNames: existingTaskIDs,
	}
	longLivingProducersScenario.CreateScenario(longLivingProducersScenarioConfig)

	longLivingConsumersScenarioConfig := trogdor.ScenarioConfig{
		ScenarioID: trogdor.TaskId{
			TaskType: "LongLivedConsume",
			Desc:     topic.TopicName,
			StartMs:  nowMs,
		},
		Class:            trogdor.CONSUME_BENCH_SPEC_CLASS,
		TaskCount:        topicConfig.LongLivedConsumeCount,
		TopicSpec:        topic,
		DurationMs:       longLivedMs,
		StartMs:          0, // start immediately
		BootstrapServers: bootstrapServers,
		MessagesPerSec:   messagesPerSec(topicConfig.longLivedConsumeTaskThroughput(), producerOptions),
		AdminConf:        adminConfig,
		ConsumerOptions:  longLivedConsumerOptions,
		ClientNodes:      shuffleSlice(clientNodes),
	}

	logutil.Debug(logger, "longLivingConsumersScenarioConfig: %+v", longLivingConsumersScenarioConfig)
	longLivingConsumersScenario := &trogdor.ScenarioSpec{
		UsedNames: existingTaskIDs,
	}
	longLivingConsumersScenario.CreateScenario(longLivingConsumersScenarioConfig)
	tasks = append(tasks, longLivingProducersScenario.TaskSpecs...)
	tasks = append(tasks, longLivingConsumersScenario.TaskSpecs...)

	// schedule short-lived produce/consume tasks for one week in advance
	shortLivedProducersScenarioConfig := trogdor.ScenarioConfig{
		ScenarioID: trogdor.TaskId{
			TaskType: "ShortLivedProduce",
			Desc:     topic.TopicName,
			StartMs:  nowMs,
		},
		Class:            trogdor.PRODUCE_BENCH_SPEC_CLASS,
		DurationMs:       shortLivedMs,
		TaskCount:        topicConfig.ShortLivedProduceCount,
		TopicSpec:        topic,
		StartMs:          nowMs,
		BootstrapServers: bootstrapServers,
		MessagesPerSec:   messagesPerSec(topicConfig.shortLivedProduceTaskThroughput(), producerOptions),
		AdminConf:        producerAdminConfig,
		ProducerOptions:  producerOptions,
		ClientNodes:      shuffleSlice(clientNodes),
	}

	logutil.Debug(logger, "initial shortLivedProducersScenarioConfig: %+v", shortLivedProducersScenarioConfig)
	producerTasks, err := consecutiveTasks(shortLivedProducersScenarioConfig, nowMs+longLivedMs, shortLivedReschedDelayMs)
	if err != nil {
		panic(err)
	}
	for _, config := range producerTasks {
		shortLivedProducersScenario := &trogdor.ScenarioSpec{
			UsedNames: existingTaskIDs,
		}
		shortLivedProducersScenario.CreateScenario(config)
		tasks = append(tasks, shortLivedProducersScenario.TaskSpecs...)
	}

	shortLivedConsumersScenarioConfig := trogdor.ScenarioConfig{
		ScenarioID: trogdor.TaskId{
			TaskType: "ShortLivedConsume",
			Desc:     topic.TopicName,
			StartMs:  nowMs,
		},
		Class:            trogdor.CONSUME_BENCH_SPEC_CLASS,
		DurationMs:       shortLivedMs,
		TaskCount:        topicConfig.ShortLivedConsumeCount,
		TopicSpec:        topic,
		StartMs:          nowMs,
		BootstrapServers: bootstrapServers,
		MessagesPerSec:   messagesPerSec(topicConfig.shortLivedConsumeTaskThroughput(), producerOptions),
		AdminConf:        adminConfig,
		ConsumerOptions:  shortLivedConsumerOptions,
		ClientNodes:      shuffleSlice(clientNodes),
	}

	logutil.Debug(logger, "initial shortLivedConsumersScenarioConfig: %+v", shortLivedConsumersScenarioConfig)
	consumerTasks, err := consecutiveTasks(shortLivedConsumersScenarioConfig, nowMs+longLivedMs, shortLivedReschedDelayMs)
	if err != nil {
		panic(err)
	}
	for _, config := range consumerTasks {
		shortLivedProducersScenario := &trogdor.ScenarioSpec{
			UsedNames: existingTaskIDs,
		}
		shortLivedProducersScenario.CreateScenario(config)
		tasks = append(tasks, shortLivedProducersScenario.TaskSpecs...)
	}

	return tasks
}

func consecutiveTasks(initialScenario trogdor.ScenarioConfig, endMs uint64, rescheduleDelayMs uint64) ([]trogdor.ScenarioConfig, error) {
	var configs []trogdor.ScenarioConfig
	if initialScenario.StartMs == 0 {
		return configs, errors.New("StartMs cannot be 0")
	}
	originalId := initialScenario.ScenarioID
	durationMs := initialScenario.DurationMs
	nextScenario := trogdor.ScenarioConfig{
		StartMs: initialScenario.StartMs,
	}
	copier.Copy(&nextScenario, &initialScenario)

	for {
		if nextScenario.StartMs+durationMs > endMs {
			break
		}
		configs = append(configs, nextScenario)

		taskId := trogdor.TaskId{}
		newStartMs := nextScenario.StartMs + durationMs + rescheduleDelayMs

		copier.Copy(&taskId, &originalId)
		copier.Copy(&nextScenario, &initialScenario)

		taskId.StartMs = newStartMs
		nextScenario.StartMs = newStartMs
		nextScenario.ScenarioID = taskId
	}
	return configs, nil
}

// Returns the number of messages per second we would need in order to achieve the desired throughput in MBs
func messagesPerSec(throughputMbPerSec float32, producerOptions trogdor.ProducerOptions) uint64 {
	messageSizeBytes := producerOptions.KeyGenerator.Size + producerOptions.ValueGenerator.Size
	throughputBytesPerSec := float64(throughputMbPerSec * 1000000)
	return uint64(throughputBytesPerSec / float64(messageSizeBytes))
}

func percentOf(part int, all int) float32 {
	return float32(percent.PercentOf(part, all)) / 100
}

// Fisher-Yates shuffle
func shuffleSlice(vals []string) []string {
	n := len(vals)
	cpy := make([]string, n)
	copy(cpy, vals)
	rand.Seed(time.Now().UnixNano())
	for i := n - 1; i > 0; i-- {
		j := rand.Intn(i + 1)
		cpy[i], cpy[j] = cpy[j], cpy[i]
	}
	return cpy
}
