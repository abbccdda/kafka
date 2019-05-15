package trogdor

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"net/http"
	"time"
)

type AdminConf struct {
	CompressionType                    string `json:"compression.type,omitempty"`
	Acks                               string `json:"acks,omitempty"`
	AutoOffsetReset                    string `json:"auto.offset.reset,omitempty"`
	EnableIdempotence                  string `json:"enable.idempotence,omitempty"`
	LingerMs                           int64  `json:"linger.ms,omitempty"`
	RetentionMs                        int64  `json:"retention.ms,omitempty"`
	MaxInFlightRequestsPerConnection   uint64 `json:"max.in.flight.requests.per.connection,omitempty"`
	RetryBackoffMs                     int64  `json:"retry.backoff.ms,omitempty"`
	SaslJaasConfig                     string `json:"sasl.jaas.config,omitempty"`
	SecurityProtocol                   string `json:"security.protocol,omitempty"`
	SslEndpointIdentificationAlgorithm string `json:"ssl.endpoint.identification.algorithm,omitempty"`
	SaslMechanism                      string `json:"sasl.mechanism,omitempty"`
	SslProtocol                        string `json:"ssl.protocol,omitempty"`
	SslKeyPassword                     string `json:"ssl.key.password,omitempty"`
	SslKeystoreLocation                string `json:"ssl.keystore.location,omitempty"`
	SslKeystorePassword                string `json:"ssl.keystore.password,omitempty"`
	SslKeystoreType                    string `json:"ssl.keystore.type,omitempty"`
	SslTruststoreLocation              string `json:"ssl.truststore.location,omitempty"`
	SslTruststorePassword              string `json:"ssl.truststore.password,omitempty"`
	SslTruststoreType                  string `json:"ssl.truststore.type,omitempty"`
}

type producerSpec struct {
	Class                string                    `json:"class"`
	StartMs              uint64                    `json:"startMs"`
	DurationMs           uint64                    `json:"durationMs"`
	ProducerNode         string                    `json:"producerNode,omitempty"`
	BootstrapServers     string                    `json:"bootstrapServers"`
	TargetMessagesPerSec uint64                    `json:"targetMessagesPerSec"`
	MaxMessages          uint64                    `json:"maxMessages"`
	KeyGenerator         KeyGeneratorSpec          `json:"keyGenerator"`
	ValueGenerator       ValueGeneratorSpec        `json:"valueGenerator"`
	TransactionGenerator *TransactionGeneratorSpec `json:"transactionGenerator,omitempty"`
	ProducerConf         *AdminConf                `json:"producerConf,omitempty"`
	CommonClientConf     *AdminConf                `json:"commonClientConf,omitempty"`
	AdminClientConf      *AdminConf                `json:"adminClientConf,omitempty"`
	ActiveTopics         json.RawMessage           `json:"activeTopics"`
}

type consumerSpec struct {
	Class                string     `json:"class"`
	StartMs              uint64     `json:"startMs"`
	DurationMs           uint64     `json:"durationMs"`
	ConsumerNode         string     `json:"consumerNode,omitempty"`
	ConsumerGroup        string     `json:"consumerGroup,omitempty"`
	BootstrapServers     string     `json:"bootstrapServers"`
	TargetMessagesPerSec uint64     `json:"targetMessagesPerSec"`
	MaxMessages          uint64     `json:"maxMessages"`
	ConsumerConf         *AdminConf `json:"consumerConf,omitempty"`
	CommonClientConf     *AdminConf `json:"commonClientConf,omitempty"`
	AdminClientConf      *AdminConf `json:"adminClientConf,omitempty"`
	ActiveTopics         []string   `json:"activeTopics"`
}

type roundTripSpec struct {
	Class                string             `json:"class"`
	StartMs              uint64             `json:"startMs"`
	DurationMs           uint64             `json:"durationMs"`
	ClientNode           string             `json:"clientNode,omitempty"`
	BootstrapServers     string             `json:"bootstrapServers"`
	TargetMessagesPerSec uint64             `json:"targetMessagesPerSec"`
	MaxMessages          uint64             `json:"maxMessages"`
	ValueGenerator       ValueGeneratorSpec `json:"valueGenerator"`
	ProducerConf         *AdminConf         `json:"producerConf,omitempty"`
	CommonClientConf     *AdminConf         `json:"commonClientConf,omitempty"`
	AdminClientConf      *AdminConf         `json:"adminClientConf,omitempty"`
	ActiveTopics         json.RawMessage    `json:"activeTopics"`
}

// a structured name of a Trogdor task
type TaskId struct {
	TaskType    string
	StartMs     uint64
	Desc        string // arbitrary task identifier
	agentId     string
	duplicateId int // increasing number to avoid duplicate names
}

// Returns the name of this Task in the following form:
// {TASK_TYPE}.{START_TIMESTAMP}.{AGENT_ID}.{OPTIONAL_TASK_DESCRIPTION}.{OPTIONAL_DUPLICATE_ID}
// e.g ShortLivedConsume.2014-07-16T20:55:46Z.cc-trogdor-service-agent-0.topic-1.2L.1
func (t *TaskId) Name() string {
	var description string
	if t.Desc != "" {
		description = fmt.Sprintf(".%s", t.Desc)
	}
	var duplicateId string
	if t.duplicateId != 0 {
		duplicateId = fmt.Sprintf(".%d", t.duplicateId)
	}
	optionalSuffix := fmt.Sprintf("%s%s", description, duplicateId)

	return fmt.Sprintf("%s.%s.%s%s", t.TaskType,
		time.Unix(int64(t.StartMs/1000), 0).UTC().Format(time.RFC3339), t.agentId, optionalSuffix)
}

type TaskSpec struct {
	ID   string          `json:"id"`
	Spec json.RawMessage `json:"spec"`
}

type taskStatusSpec struct {
	Class string `json:"class"`
}

type TaskStatus struct {
	State     string          `json:"state"`
	Spec      taskStatusSpec  `json:"spec"`
	StartedMs int64           `json:"startedMs"`
	DoneMs    int64           `json:"doneMs"`
	Cancelled bool            `json:"cancelled"`
	Error     string          `json:"error"`
	Status    json.RawMessage `json:"status"`
}

type produceTaskStatus struct {
	TotalSent        int     `json:"totalSent"`
	AverageLatencyMs float64 `json:"averageLatencyMs"`
	P50LatencyMs     int     `json:"p50LatencyMs"`
	P95LatencyMs     int     `json:"p95LatencyMs"`
	P99LatencyMs     int     `json:"p99LatencyMs"`
}

type consumeTaskStatus struct {
	AssignedPartitions      []string `json:"assignedPartitions"`
	TotalMessagesReceived   int      `json:"totalMessagesReceived"`
	TotalBytesReceived      int      `json:"totalBytesReceived"`
	AverageMessageSizeBytes int      `json:"averageMessageSizeBytes"`
	AverageLatencyMs        float64  `json:"averageLatencyMs"`
	P50LatencyMs            int      `json:"p50LatencyMs"`
	P95LatencyMs            int      `json:"p95LatencyMs"`
	P99LatencyMs            int      `json:"p99LatencyMs"`
}

const (
	PENDING_TASK_STATE  = "PENDING"
	RUNNING_TASK_STATE  = "RUNNING"
	STOPPING_TASK_STATE = "STOPPING"
	DONE_TASK_STATE     = "DONE"
)

type ValueGeneratorSpec struct {
	ValueType string `json:"type"`
	Size      uint64 `json:"size"`
	Padding   uint64 `json:"padding"`
}

type TransactionGeneratorSpec struct {
	Type                   string `json:"type"`
	MessagesPerTransaction uint64 `json:"messagesPerTransaction"`
}

// use mostly random values to best simulate real-life data compression
var DefaultValueGeneratorSpec = ValueGeneratorSpec{ValueType: "uniformRandom", Size: 900, Padding: 100}
var DefaultKeyGeneratorSpec = KeyGeneratorSpec{Type: "sequential", Size: 4, StartOffset: 0}
var DefaultTransactionGeneratorSpec = TransactionGeneratorSpec{Type: "uniform", MessagesPerTransaction: 100}

type KeyGeneratorSpec struct {
	Type        string `json:"type"`
	Size        uint64 `json:"size"`
	StartOffset uint64 `json:"startOffset"`
}

type TopicSpec struct {
	NumPartitions     uint64 `json:"numPartitions"`
	ReplicationFactor uint64 `json:"replicationFactor"`
	TopicName         string
}

type ScenarioSpec struct {
	TaskSpecs []TaskSpec
	UsedNames map[string]bool
}

type ConsumerOptions struct {
	ConsumerGroup string
}

type ProducerOptions struct {
	ValueGenerator       ValueGeneratorSpec
	TransactionGenerator TransactionGeneratorSpec
	KeyGenerator         KeyGeneratorSpec
}

// Returns the number of messages per second we would need in order to achieve the desired throughput in MBs
func (po *ProducerOptions) MessagesPerSec(throughputMbPerSec float32) uint64 {
	messageSizeBytes := po.KeyGenerator.Size + po.ValueGenerator.Size
	throughputBytesPerSec := float64(throughputMbPerSec) * 1024 * 1024
	return uint64(math.Round(throughputBytesPerSec / float64(messageSizeBytes)))
}

// a Scenario is a composition of multiple identical Trogdor tasks split across multiple Trogdor agents
type ScenarioConfig struct {
	ScenarioID       TaskId
	Class            string
	TaskCount        int
	TopicSpec        TopicSpec
	DurationMs       uint64
	StartMs          uint64
	BootstrapServers string
	MessagesPerSec   uint64 // the total messages per second we want this scenario to have
	AdminConf        AdminConf
	ProducerOptions  ProducerOptions
	ConsumerOptions  ConsumerOptions
	ClientNodes      []string // all the configured trogdor nodes
}

const PRODUCE_BENCH_SPEC_CLASS = "org.apache.kafka.trogdor.workload.ProduceBenchSpec"
const CONSUME_BENCH_SPEC_CLASS = "org.apache.kafka.trogdor.workload.ConsumeBenchSpec"

func (r *AdminConf) ParseConfig(adminConfFile string) error {
	raw, err := ioutil.ReadFile(adminConfFile)
	if err != nil {
		panic(err)
	}

	err = json.Unmarshal(raw, &r)

	return err

}

func (r *TopicSpec) ReturnTopicSpecRawJson() json.RawMessage {
	raw := json.RawMessage{}
	jsonString := fmt.Sprintf(`{"%s": { "numPartitions": %d, "replicationFactor": %d}}`, r.TopicName, r.NumPartitions, r.ReplicationFactor)
	json.Unmarshal([]byte(jsonString), &raw)

	return raw
}

func (r *ScenarioSpec) createAgentTasks(scenarioConfig ScenarioConfig, scenario []TaskSpec) []TaskSpec {
	rawSpec := json.RawMessage{}

	clientNodesCount := len(scenarioConfig.ClientNodes)
	for agentIdx := 0; agentIdx < scenarioConfig.TaskCount; agentIdx++ {
		durationSeconds := uint64(scenarioConfig.DurationMs) / 1000
		messagesPerSec := scenarioConfig.MessagesPerSec / uint64(scenarioConfig.TaskCount)
		maxMessages := durationSeconds * messagesPerSec

		clientNode := scenarioConfig.ClientNodes[agentIdx%clientNodesCount]
		switch class := scenarioConfig.Class; class {

		case PRODUCE_BENCH_SPEC_CLASS:
			{
				activeTopics := scenarioConfig.TopicSpec.ReturnTopicSpecRawJson()

				produceBenchSpecData := producerSpec{
					Class:                scenarioConfig.Class,
					StartMs:              scenarioConfig.StartMs,
					DurationMs:           scenarioConfig.DurationMs,
					ProducerNode:         clientNode,
					BootstrapServers:     scenarioConfig.BootstrapServers,
					TargetMessagesPerSec: messagesPerSec,
					MaxMessages:          maxMessages,
					ValueGenerator:       scenarioConfig.ProducerOptions.ValueGenerator,
					KeyGenerator:         scenarioConfig.ProducerOptions.KeyGenerator,
					CommonClientConf:     &scenarioConfig.AdminConf,
					ActiveTopics:         activeTopics,
				}
				if scenarioConfig.ProducerOptions.TransactionGenerator.Type != "" {
					produceBenchSpecData.TransactionGenerator = &scenarioConfig.ProducerOptions.TransactionGenerator
				}
				rawSpec, _ = json.Marshal(produceBenchSpecData)
			}

		case CONSUME_BENCH_SPEC_CLASS:
			{
				consumeBenchSpecData := consumerSpec{
					Class:                scenarioConfig.Class,
					StartMs:              scenarioConfig.StartMs,
					DurationMs:           scenarioConfig.DurationMs,
					ConsumerNode:         clientNode,
					BootstrapServers:     scenarioConfig.BootstrapServers,
					TargetMessagesPerSec: messagesPerSec,
					MaxMessages:          maxMessages,
					CommonClientConf:     &scenarioConfig.AdminConf,
					ConsumerGroup:        scenarioConfig.ConsumerOptions.ConsumerGroup, // trogdor will generate a random group if given an empty one
					ActiveTopics:         []string{scenarioConfig.TopicSpec.TopicName},
				}
				rawSpec, _ = json.Marshal(consumeBenchSpecData)
			}
		}

		scenarioConfig.ScenarioID.agentId = clientNode
		SpecData := TaskSpec{
			ID:   r.generateUniqueName(&scenarioConfig.ScenarioID),
			Spec: rawSpec,
		}
		scenario = append(scenario, SpecData)
	}

	return scenario
}

func (r *ScenarioSpec) generateUniqueName(taskName *TaskId) string {
	duplicateId := taskName.duplicateId
	nameToUse := taskName.Name()
	for r.UsedNames[nameToUse] {
		duplicateId += 1
		taskName.duplicateId = duplicateId
		nameToUse = taskName.Name()
	}
	r.UsedNames[nameToUse] = true

	return nameToUse
}

func (r *ScenarioSpec) CreateScenario(scenarioConfig ScenarioConfig) {
	r.TaskSpecs = r.createAgentTasks(scenarioConfig, []TaskSpec{})
}

// Returns all the Trogdor Tasks from the coordinator that match the filter
// Note that earliestStartMs and latestStartMs denote the time
// when the task was actually started, not scheduled to start
func TaskStatuses(coordinatorURL string, earliestStartMs int64, latestStartMs int64, state string) (map[string]*TaskStatus, error) {
	req, err := http.Get(fmt.Sprintf("%s/coordinator/tasks?firstStartMs=%d&lastStartMs=%d&state=%s",
		coordinatorURL, earliestStartMs, latestStartMs, state))
	if err != nil {
		return nil, err
	}
	if req.StatusCode > 299 {
		return nil, fmt.Errorf(
			`Request for the status of tasks (earliestStartMs: %d, latestStartMs: %d) returned status code %s`,
			earliestStartMs, latestStartMs, req.Status)
	}

	return parseStatuses(req.Body)
}

type taskStatuses struct {
	Tasks map[string]*TaskStatus `json:"tasks"`
}

// parses statuses returned from the /coordinator/tasks endpoint
func parseStatuses(reader io.Reader) (map[string]*TaskStatus, error) {
	var statuses taskStatuses
	decoder := json.NewDecoder(reader)
	err := decoder.Decode(&statuses)
	if err != nil {
		return nil, err
	}

	return statuses.Tasks, nil
}

func (r *TaskSpec) Status(coordinatorURL string) (*TaskStatus, error) {
	req, err := http.Get(fmt.Sprintf("%s/coordinator/tasks/%s", coordinatorURL, r.ID))
	if err != nil {
		return nil, err
	}
	if req.StatusCode > 299 {
		return nil, fmt.Errorf(`request for the status of task "%s" returned status code %s`, r.ID, req.Status)
	}

	return r.parseStatus(req.Body)
}

func (r *TaskSpec) parseStatus(reader io.Reader) (*TaskStatus, error) {
	var taskStatus TaskStatus
	decoder := json.NewDecoder(reader)
	err := decoder.Decode(&taskStatus)
	if err != nil {
		return nil, err
	}

	return &taskStatus, err
}

func (r *TaskSpec) CreateTask(coordinatorURL string) (*http.Response, []byte, error) {

	out, err := json.Marshal(r)

	if err != nil {
		return nil, nil, err
	}

	req, err := http.NewRequest("POST", fmt.Sprintf("%s/coordinator/task/create", coordinatorURL), bytes.NewBuffer(out))
	if err != nil {
		return nil, nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, nil, err
	}

	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)

	return resp, body, nil
}

func DeleteTask(coordinatorURL string, taskId string) (*http.Response, []byte, error) {
	req, err := http.NewRequest("DELETE", fmt.Sprintf("%s/coordinator/tasks?taskId=%s", coordinatorURL, taskId), nil)
	if err != nil {
		return nil, nil, err
	}
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, nil, err
	}

	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)

	return resp, body, nil
}

func (r *TaskStatus) isConsumeTask() bool {
	return r.Spec.Class == CONSUME_BENCH_SPEC_CLASS
}

func (r *TaskStatus) isProduceTask() bool {
	return r.Spec.Class == PRODUCE_BENCH_SPEC_CLASS
}

func (r *TaskStatus) produceTaskStatus() (*produceTaskStatus, error) {
	if !r.isProduceTask() {
		return nil, fmt.Errorf("the task is a %s, not a produce task", r.Spec.Class)
	}
	var taskStatus produceTaskStatus
	err := json.Unmarshal(r.Status, &taskStatus)
	if err != nil {
		return nil, err
	}

	return &taskStatus, err
}

func (r *TaskStatus) consumeTaskStatus() (map[string]*consumeTaskStatus, error) {
	if !r.isConsumeTask() {
		return nil, fmt.Errorf("the task is a %s, not a consume task", r.Spec.Class)
	}
	var taskStatuses map[string]*consumeTaskStatus
	err := json.Unmarshal(r.Status, &taskStatuses)
	if err != nil {
		return nil, err
	}

	return taskStatuses, err
}
