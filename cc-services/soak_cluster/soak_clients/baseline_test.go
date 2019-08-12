package soak_clients

import (
	"encoding/json"
	"fmt"
	"github.com/confluentinc/ce-kafka/cc-services/soak_cluster/common"
	"github.com/confluentinc/ce-kafka/cc-services/soak_cluster/trogdor"
	"github.com/jinzhu/copier"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"math"
	"strings"
	"testing"
)

const (
	oneWeekDurationMs               = 604800000
	fifteenMinutesDurationMs        = 900000
	shortLivedTaskRescheduleDelayMs = 0
)

var clientNodes = []string{"node1", "node2", "node3"}

func TestBaselineTasks(t *testing.T) {
	logger = common.InitLogger("soak-clients-test")
	topics := soakTestConfig([]string{"a", "b", "c"})
	expectedTasks := calculateExpectedTasksCount(topics.Topics, topics.LongLivedTaskDurationMs, topics.ShortLivedTaskDurationMs, topics.ShortLivedTaskRescheduleDelayMs)
	configPath := writeSoakTestConfigFile(t, topics)
	tasks, err := baselineTasks(configPath, 10, "")
	if err != nil {
		fmt.Println(err)
		assert.Fail(t, "error while getting baseline tasks")
	}

	assertTaskNamesUnique(t, tasks)
	assertTaskCount(t, tasks, expectedTasks, topics.LongLivedTaskDurationMs, topics.ShortLivedTaskDurationMs)
}

func assertCreateTopicTasks(t *testing.T, topicSpecification TopicConfiguration,
	expectedCount *expectedTasksCount, longDuration uint64, shortDuration uint64, reschedDelay uint64) {

	calculatedExpected := calculateExpectedTasksCount([]TopicConfiguration{topicSpecification}, longDuration, shortDuration, reschedDelay)
	if expectedCount == nil {
		expectedCount = &calculatedExpected
	}
	assert.Equal(t, *expectedCount, calculatedExpected) // verify calculateExpectedTasksCount output when expectedCount specified

	tasks := createTopicTasks(topicSpecification, clientNodes, make(map[string]bool), longDuration, shortDuration, reschedDelay, "")
	assertTaskCount(t, tasks, *expectedCount, longDuration, shortDuration)
}

func TestCreateTopicTasksVariousDurations(t *testing.T) {
	logger = common.InitLogger("soak-clients-test")
	topicSpecification := TopicConfiguration{
		Name:                   "testTest",
		PartitionsCount:        1000,
		ProduceMBsThroughput:   60,
		ConsumeMBsThroughput:   60,
		ShortLivedProduceCount: 1,
		LongLivedProduceCount:  1,
		ShortLivedConsumeCount: 1,
		LongLivedConsumeCount:  1,
	}

	longDuration, shortDuration, reschedDelay := uint64(10), uint64(5), uint64(2)
	expected := &expectedTasksCount{
		ExpectedShortLivedProducerTasks: 1,
		ExpectedLongLivedProducerTasks:  1,
		ExpectedShortLivedConsumerTasks: 1,
		ExpectedLongLivedConsumerTasks:  1,

		ExpectedShortLivedTasksCount: 2,
		ExpectedLongLivedTasksCount:  2,
		ExpectedTotalTasksCount:      4,
	}
	assertCreateTopicTasks(t, topicSpecification, expected, longDuration, shortDuration, reschedDelay)

	longDuration, shortDuration, reschedDelay = uint64(10), uint64(5), uint64(5)
	expected2 := &expectedTasksCount{
		ExpectedShortLivedProducerTasks: 1,
		ExpectedLongLivedProducerTasks:  1,
		ExpectedShortLivedConsumerTasks: 1,
		ExpectedLongLivedConsumerTasks:  1,

		ExpectedShortLivedTasksCount: 2,
		ExpectedLongLivedTasksCount:  2,
		ExpectedTotalTasksCount:      4,
	}
	assertCreateTopicTasks(t, topicSpecification, expected2, longDuration, shortDuration, reschedDelay)

	longDuration, shortDuration, reschedDelay = uint64(9), uint64(3), uint64(1)
	expected3 := &expectedTasksCount{
		ExpectedShortLivedProducerTasks: 2,
		ExpectedLongLivedProducerTasks:  1,
		ExpectedShortLivedConsumerTasks: 2,
		ExpectedLongLivedConsumerTasks:  1,

		ExpectedShortLivedTasksCount: 4,
		ExpectedLongLivedTasksCount:  2,
		ExpectedTotalTasksCount:      6,
	}
	assertCreateTopicTasks(t, topicSpecification, expected3, longDuration, shortDuration, reschedDelay)

	longDuration, shortDuration, reschedDelay = uint64(9), uint64(3), uint64(0)
	expected4 := &expectedTasksCount{
		ExpectedShortLivedProducerTasks: 3,
		ExpectedLongLivedProducerTasks:  1,
		ExpectedShortLivedConsumerTasks: 3,
		ExpectedLongLivedConsumerTasks:  1,

		ExpectedShortLivedTasksCount: 6,
		ExpectedLongLivedTasksCount:  2,
		ExpectedTotalTasksCount:      8,
	}
	assertCreateTopicTasks(t, topicSpecification, expected4, longDuration, shortDuration, reschedDelay)

	assertCreateTopicTasks(t, topicSpecification, nil, oneWeekDurationMs, fifteenMinutesDurationMs/2, shortLivedTaskRescheduleDelayMs)
}

func assertTaskNamesUnique(t *testing.T, tasks []trogdor.TaskSpec) {
	existingIDs := make(map[string]bool)
	for _, newTask := range tasks {
		if existingIDs[newTask.ID] {
			assert.Fail(t, fmt.Sprintf("Task name %s is duplicate", newTask.ID))
			return
		}
		existingIDs[newTask.ID] = true
	}
}

func assertTaskCount(t *testing.T, tasks []trogdor.TaskSpec, expected expectedTasksCount,
	longLivedMs uint64, shortLivedMs uint64) {
	isShortLivedTask := func(v string) bool {
		return strings.Contains(v, fmt.Sprintf(`"durationMs":%d,`, shortLivedMs))
	}
	isLongLivedTask := func(v string) bool {
		return strings.Contains(v, fmt.Sprintf(`"durationMs":%d,`, longLivedMs))
	}
	isConsumerTask := func(v string) bool {
		return strings.Contains(v, trogdor.CONSUME_BENCH_SPEC_CLASS)
	}
	isProducerTask := func(v string) bool {
		return strings.Contains(v, trogdor.PRODUCE_BENCH_SPEC_CLASS)
	}
	taskJsons := make([]string, len(tasks))
	for i, v := range tasks {
		buffer, err := v.Spec.MarshalJSON()
		if err != nil {
			assert.Fail(t, "received error while unmarshalling JSON", err)
		}
		taskJsons[i] = string(buffer)
	}

	shortLivedTasks := filter(taskJsons, isShortLivedTask)
	longLivedTasks := filter(taskJsons, isLongLivedTask)
	assert.Equal(t, expected.ExpectedShortLivedTasksCount, len(shortLivedTasks))
	assert.Equal(t, expected.ExpectedLongLivedTasksCount, len(longLivedTasks))

	shortLivedConsumerTasks := filter(taskJsons, func(v string) bool {
		return isShortLivedTask(v) && isConsumerTask(v)
	})
	shortLivedProducerTasks := filter(taskJsons, func(v string) bool {
		return isShortLivedTask(v) && isProducerTask(v)
	})
	longLivedConsumerTasks := filter(taskJsons, func(v string) bool {
		return isLongLivedTask(v) && isConsumerTask(v)
	})
	longLivedProducerTasks := filter(taskJsons, func(v string) bool {
		return isLongLivedTask(v) && isProducerTask(v)
	})
	assert.Equal(t, expected.ExpectedShortLivedConsumerTasks, len(shortLivedConsumerTasks))
	assert.Equal(t, expected.ExpectedShortLivedProducerTasks, len(shortLivedProducerTasks))
	assert.Equal(t, expected.ExpectedLongLivedProducerTasks, len(longLivedProducerTasks))
	assert.Equal(t, expected.ExpectedLongLivedConsumerTasks, len(longLivedConsumerTasks))
	assert.Equal(t, expected.ExpectedTotalTasksCount, len(tasks))
}

func TestParseConfigParsesCorrectly(t *testing.T) {
	originalTopics := soakTestConfig([]string{"a", "b", "c"})
	fileName := writeSoakTestConfigFile(t, originalTopics)

	topics := SoakTestConfig{}
	err := topics.parseConfig(fileName)

	assert.NoError(t, err)
	assert.Equal(t, originalTopics, topics)
}

func writeSoakTestConfigFile(t *testing.T, topics SoakTestConfig) string {
	file, err := ioutil.TempFile("/tmp", "config")
	data, err := json.Marshal(topics)
	if err != nil {
		assert.Fail(t, fmt.Sprintf("error while marshalling topics %s", err))
	}
	_, err = file.Write(data)
	if err != nil {
		assert.Fail(t, fmt.Sprintf("error while creating temporary file %s", err))
	}

	return file.Name()
}

func TestConsecutiveTasks(t *testing.T) {
	mediumTopic := trogdor.TopicSpec{
		PartitionsSpec: &trogdor.PartitionsSpec{
			NumPartitions:     uint64(16),
			ReplicationFactor: 3,
		},
	}
	startConfig := trogdor.ScenarioConfig{
		ScenarioID: trogdor.TaskId{
			TaskType: "ShortLivedConsume",
			Desc:     "MediumTopic",
		},
		Class:            trogdor.CONSUME_BENCH_SPEC_CLASS,
		DurationMs:       5,
		TaskCount:        3,
		TopicSpec:        mediumTopic,
		BootstrapServers: "",
		StartMs:          10,
		MessagesPerSec:   750,
		AdminConf:        adminConfig,
		ConsumerOptions: trogdor.ConsumerOptions{
			ConsumerGroup: "cg-1",
		},
		ClientNodes: shuffleSlice(clientNodes),
	}
	firstConfig := trogdor.ScenarioConfig{}
	copier.Copy(&firstConfig, &startConfig)
	secondConfig := trogdor.ScenarioConfig{}
	copier.Copy(&secondConfig, &startConfig)
	secondConfig.StartMs = 15
	secondConfig.ScenarioID = trogdor.TaskId{
		TaskType: "ShortLivedConsume",
		Desc:     "MediumTopic",
		StartMs:  15,
	}
	thirdConfig := trogdor.ScenarioConfig{}
	copier.Copy(&thirdConfig, &startConfig)
	thirdConfig.StartMs = 20
	thirdConfig.ScenarioID = trogdor.TaskId{
		TaskType: "ShortLivedConsume",
		Desc:     "MediumTopic",
		StartMs:  20,
	}

	// should return 3 scenario configs
	expectedConfigs := []trogdor.ScenarioConfig{
		firstConfig,
		secondConfig,
		thirdConfig,
	}

	actualConfigs, err := consecutiveTasks(startConfig, 25, shortLivedTaskRescheduleDelayMs)
	if err != nil {
		assert.Fail(t, err.Error())
	}
	assert.Equal(t, expectedConfigs, actualConfigs)
}

func TestConsecutiveTasksFailsIfStartMsIsZero(t *testing.T) {
	mediumTopic := trogdor.TopicSpec{
		PartitionsSpec: &trogdor.PartitionsSpec{
			NumPartitions:     uint64(16),
			ReplicationFactor: 3,
		},
	}
	startConfig := &trogdor.ScenarioConfig{
		ScenarioID: trogdor.TaskId{
			TaskType: "ShortLivedConsume",
			Desc:     "MediumTopic",
		},
		Class:          trogdor.CONSUME_BENCH_SPEC_CLASS,
		TaskCount:      3,
		TopicSpec:      mediumTopic,
		MessagesPerSec: 75,
		ConsumerOptions: trogdor.ConsumerOptions{
			ConsumerGroup: "cg-1",
		},
		DurationMs:       5,
		BootstrapServers: "",
	}
	_, err := consecutiveTasks(*startConfig, 25, shortLivedTaskRescheduleDelayMs)
	assert.Error(t, err)
}

func TestMessagesPerSec(t *testing.T) {
	keyGen, valueGen := trogdor.KeyGeneratorSpec{Size: 100}, trogdor.ValueGeneratorSpec{Size: 900}
	expectedMessagesPerSec := uint64(1000) // 1000 messages of size 0.01mb per second equal 1mb/s throughput

	assert.Equal(t,
		expectedMessagesPerSec,
		messagesPerSec(1.00, trogdor.ProducerOptions{
			ValueGenerator:       valueGen,
			TransactionGenerator: trogdor.DefaultTransactionGeneratorSpec,
			KeyGenerator:         keyGen,
		}))
}

func soakTestConfig(topicNames []string) SoakTestConfig {
	topics := SoakTestConfig{
		LongLivedTaskDurationMs:         oneWeekDurationMs,
		ShortLivedTaskDurationMs:        fifteenMinutesDurationMs,
		ShortLivedTaskRescheduleDelayMs: shortLivedTaskRescheduleDelayMs,
	}
	for _, topicName := range topicNames {
		topics.Topics = append(topics.Topics, TopicConfiguration{
			Name:                   topicName,
			PartitionsCount:        1000,
			ProduceMBsThroughput:   60,
			ConsumeMBsThroughput:   60,
			ShortLivedProduceCount: 5,
			LongLivedProduceCount:  5,
			LongLivedConsumeCount:  5,
			ShortLivedConsumeCount: 5,
		})
	}

	return topics
}

func filter(vs []string, f func(string) bool) []string {
	vsf := make([]string, 0)
	for _, v := range vs {
		if f(v) {
			vsf = append(vsf, v)
		}
	}
	return vsf
}

type expectedTasksCount struct {
	ExpectedShortLivedProducerTasks int
	ExpectedLongLivedProducerTasks  int
	ExpectedShortLivedConsumerTasks int
	ExpectedLongLivedConsumerTasks  int
	ExpectedShortLivedTasksCount    int
	ExpectedLongLivedTasksCount     int
	ExpectedTotalTasksCount         int
}

func calculateExpectedTasksCount(topics []TopicConfiguration, longLivedMs uint64, shortLivedMs uint64, shortLivedRescheduleDelayMs uint64) expectedTasksCount {
	llProduceCount, slProduceCount, llConsumeCount, slConsumeCount := 0, 0, 0, 0
	for _, topic := range topics {
		llProduceCount += topic.LongLivedProduceCount
		slProduceCount += topic.ShortLivedProduceCount

		llConsumeCount += topic.LongLivedConsumeCount
		slConsumeCount += topic.ShortLivedConsumeCount
	}
	shortLivedCount := int(math.Max(float64(longLivedMs/(shortLivedMs+shortLivedRescheduleDelayMs)), 1))
	slTaskCount := (slConsumeCount + slProduceCount) * shortLivedCount
	llTaskCount := llConsumeCount + llProduceCount
	return expectedTasksCount{
		ExpectedShortLivedProducerTasks: slProduceCount * shortLivedCount,
		ExpectedLongLivedProducerTasks:  llProduceCount,
		ExpectedShortLivedConsumerTasks: slConsumeCount * shortLivedCount,
		ExpectedLongLivedConsumerTasks:  llConsumeCount,
		ExpectedShortLivedTasksCount:    slTaskCount,
		ExpectedLongLivedTasksCount:     llTaskCount,
		ExpectedTotalTasksCount:         llTaskCount + slTaskCount,
	}
}
