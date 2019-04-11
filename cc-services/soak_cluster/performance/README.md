# Performance Tests

The performance_test package contains functionality to orchestrate configurable tests that stress a Kafka cluster.
We use Kafka's Trogdor framework to achieve this and plan on supporting multiple test scenarios (throughput tests, connection/authentication/rebalance storms, etc).

Each kind of test has a different type, specified in a JSON format.
Currently supported types:   
* `ProgressiveWorkload`


#### Progressive Workload
We define a progressive workload as a continuous series of test scenarios where each step progressively issues more load on the cluster.
Each step essentially consists of multiple Trogdor tasks. We schedule exactly one Trogdor task per Trogdor agent at any one given time.

* SingleDurationMs - the duration of a single iteration
* StepCooldownMs - a configurable amount of time in between each iteration
* MessagesIncreasePerStep - the amount of messages we bump this up by on each step
* StartThroughputMbs - the throughput, in MB/s, we want to start at
* EndThroughputMbs - the throughput, in MB/s, we want this test to end at (inclusive)
* ThroughputIncreaseMbs - the amount of throughput we want to progressively increase each step by
* MessageSizeBytes - an approximation of the message size. We always add 100 of the same bytes as padding to the end of the message to simulate a partly-compressible workload

##### Example
See [example.json](example.json) for a sample configuration.
A configuration like
```json
{
  "test_type": "ProgressiveWorkload",
  "test_name": "test",
  "test_parameters": {
    "workload_type": "Produce",
    "step_duration_ms": 60000,
    "partition_count": 10,
    "step_cooldown_ms": 60000,
    "start_throughput_mbs": 1,
    "end_throughput_mbs": 5,
    "throughput_increase_per_step_mbs": 3,
    "message_size_bytes": 1000
  }
}
```
would result in 3 steps, consisting of the following throughputs (1 MB/s, 4 MB/s, 7 MB/s). Each step would last one minute and there would be one minute of downtime in between each step.

## How to Run
Pre-requisite: Have Trogdor and the soak clients helm charts deployed. (see [cc-services/README.md](../../README.md))

```bash
# open a shell into the running soak-clients pod
$ kubectl get pods --all-namespaces | grep clients-cli
soak-tests          cc-soak-clients-clients-cli-76568867b5-bcmdh                 0/1     Running              0          1h
$ kubectl exec -it -n soak-tests cc-soak-clients-clients-cli-76568867b5-bcmdh sh
```
Once inside the pod, create a JSON test configuration and run the tests with it:
```bash
vi /mnt/test/test_config.json
export PERFORMANCE_TEST_CONFIG_PATH=/mnt/test/test_config.json
./soak-clients performance-tests
```
