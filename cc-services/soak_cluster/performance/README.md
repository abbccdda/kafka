# Performance Tests

The performance_test package contains functionality to orchestrate configurable tests that stress a Kafka cluster. It allows you to schedule _scenarios_, which consist of multiple tests.
 
We use Kafka's Trogdor framework to achieve this and plan on supporting multiple test types (throughput tests, connection/authentication/rebalance storms, etc).

Each kind of test has a different type, specified in a JSON format.
Currently supported types:   
* `ProgressiveWorkload`

The framework allows you to schedule multiple tests at once with flexible schedules (one after the other, overlapping, etc)

### General JSON Parameters
* scenario_name
* schedule - defines when each test in the scenario should run in relation to one another
    * start_after_begin - a list of tasks that should all begin before the given task starts
    * start_after_end - a list of tasks that should all end before the given task starts
        * we do not support configuring both start_after_begin and start_after_end at once
    * start_delay - defines a duration that should pass before we start the task
* test_definitions - defines the different tests this scenario will consist of
    * test_name - name of the test
    * test_type - defines the type of test we will run. Each test supports different test_parameters
    * test_parameters - custom parameters for this specific test type

#### Progressive Workload
We define a progressive workload as a continuous series of test scenarios where each step progressively issues more load on the cluster.
Each step essentially consists of multiple Trogdor tasks. We schedule exactly one Trogdor task per Trogdor agent at any one given time.

* single_duration_ms - the duration of a single iteration
* step_cooldown_ms - a configurable amount of time in between each iteration
* start_throughput_mbs - the throughput, in MB/s, we want to start at
* end_throughput_mbs - the throughput, in MB/s, we want this test to end at (inclusive)
* throughput_increase_mbs - the amount of throughput we want to progressively increase each step by
* message_size_bytes - an approximation of the message size. We always add 100 of the same bytes as padding to the end of the message to simulate a partly-compressible workload

##### Single Test Example
See [example.json](example.json) for a sample configuration.
A configuration like
```json
{
  "scenario_name": "ExampleTest",
  "test_definitions": {
    "test_type": "ProgressiveWorkload",
    "test_name": "test",
    "test_parameters": {
      "workload_type": "Produce",
      "step_duration_ms": 100,
      "partition_count": 10,
      "step_cooldown_ms": 60000,
      "start_throughput_mbs": 10,
      "end_throughput_mbs": 20,
      "throughput_increase_per_step_mbs": 5,
      "message_size_bytes": 1000
    }
  }
}
```
would result in 3 steps, consisting of the following throughputs (1 MB/s, 4 MB/s, 7 MB/s). Each step would last one minute and there would be one minute of downtime in between each step.
Note that the _schedule_ field is optional. If omitted, all tasks get scheduled at once.

##### Multi-Test Example
```json
{
  "scenario_name": "TestCPKAFKA",
  "schedule": {
    "B": {
      "start_delay": "1m",
      "start_after_begin": ["A"]
    },
    "A": {}
  },
  "test_definitions": [
    {
      "test_type": "ProgressiveWorkload",
      "test_name": "A",
      "test_parameters": {
        "workload_type": "Produce",
        "step_duration_ms": 100,
        "partition_count": 10,
        "step_cooldown_ms": 60000,
        "start_throughput_mbs": 10,
        "end_throughput_mbs": 20,
        "throughput_increase_per_step_mbs": 5,
        "message_size_bytes": 1000
      }
    },
    {
      "test_type": "ProgressiveWorkload",
      "test_name": "B",
      "test_parameters": {
        "workload_type": "Produce",
        "step_duration_ms": 300,
        "partition_count": 15,
        "step_cooldown_ms": 1000,
        "start_throughput_mbs": 10,
        "end_throughput_mbs": 20,
        "throughput_increase_per_step_mbs": 5,
        "message_size_bytes": 255
      }
    }
  ]
}
```
In this example, we have two produce tasks. The second task, _B_, will start one minute after _A_ starts.

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
