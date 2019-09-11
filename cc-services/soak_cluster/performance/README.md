# Performance Tests

The performance_test package contains functionality to orchestrate configurable tests that stress a Kafka cluster. It allows you to schedule _scenarios_, which consist of multiple tests.
 
We use Kafka's Trogdor framework to achieve this and plan on supporting multiple test types (throughput tests, connection/authentication/rebalance storms, etc).

Each kind of test has a different type, specified in a JSON format.
Currently supported types:   
* `ProgressiveWorkload`
* `TailConsume`

The framework allows you to schedule multiple tests at once with flexible schedules (one after the other, overlapping, etc)

### Definitions

There can be confusion when reading this code because we divide it into many subdivisions of work, all with similar names.  Here is what each subdivision means.

* `Workload` (*Java*) - What is actually running inside Trogdor.
* `Task` - This is the smallest unit of work in the Go client.  This has a 1:1 mapping to a `workload` within Trogdor.
* `Step` - A group of `task` all sharing the same configuration.  In general, a step has an equal number of tasks as trogdor agents, but that can be overridden. 
* `Fanout` - This should be a property of a `step` and will duplicate the tasks linearly.  For example: A fanout of 2 will double the tasks, and a fanout of 3 will triple it. 
* `Scenario` - A scenario is a metadata wrapper around `step` configurations.
* `Workload` (*Go*) - An overarching configuration that defines all `scenario` configurations.

### General JSON Parameters
* scenario_name
* schedule - defines when each test in the scenario should run in relation to one another
    * start_after_begin - a list of tasks that should all begin before the given task starts
    * start_after_end - a list of tasks that should all end before the given task starts
        * we do not support configuring both start_after_begin and start_after_end at once
    * start_delay - defines a duration that should pass before we start the task
    * run_until_end_of - defines the duration of the given test. It will run until the test with the latest end time in the list
* test_definitions - defines the different tests this scenario will consist of
    * test_name - name of the test
    * test_type - defines the type of test we will run. Each test supports different test_parameters
    * test_parameters - custom parameters for this specific test type

#### Progressive Workload
We define a progressive workload as a continuous series of test scenarios where each step progressively issues more load on the cluster.
Each step essentially consists of multiple Trogdor tasks. We schedule exactly one Trogdor task per Trogdor agent at any one given time.

* `workload_type` - This currently only supports `"Produce"`.
* `topic_name` (*optional*) - The topic to produce to.  The default is derived from the name parameter: `[workload_name]-topic`. 
* `partition_count` - The number of partitions the topic should have.
* `step_duration_ms` - The duration, in milliseconds, of a single iteration.
* `start_throughput_mbs` - The throughput, in MB/s, we want to start at.
* `end_throughput_mbs` - The throughput, in MB/s, we want this test to end at (inclusive).
* `step_cooldown_ms` (*optional*) - A configurable amount of time, in milliseconds, in between each iteration.  Only applicable if the start and end throughput are different.
* `throughput_increase_mbs` (*optional*) - The amount of throughput we want to progressively increase each step by.  Only applicable if the start and end throughput are different.
* `message_size_bytes` (*optional*) - An approximation of the message size.  The default is 900 bytes.
* `message_padding_bytes` (*optional*) - The amount of bytewise 0's to append the end of the message as padding so compression can work.  The default is 100 bytes, and this value is not used unless `message_size_bytes` is specified as well.
* `tasks_per_step` (*optional*) - The number of Trogdor tasks to create per step.  The default is equal to the number of trogdor agents.
* `slow_start_per_step_ms` (*optional*) - If specified, each task in a given step will be progressively delayed by this amount, in milliseconds, as a way to ramp up the load.  The task numbered N will start delayed by `(N-1) * [slow_start_per_step_ms]` milliseconds, and it's duration will be shortened by the same amount of time. The default is 0, or no ramp up.

#### Tail Consumer
A tail consumer test consists of multiple consumers subscribed to a topic. They read from the end of the log at all times with no throttling.
We schedule exactly one Trogdor ConsumeBench task per Trogdor agent **for every** consumer group at any one given time.

* `fanout` (*optional*) - Defines the number of consumer groups and sets the number of tasks created as `[fanout] * [number of trogdor agents]`.  If `consumer_group` is specified, the test will not create a different consumer group for each fanout.
* `topics_from_test` (*optional*) - The name of the test that produces to topics which consumers of this test will subscribe to.  One and only one of this or `topics` below must be set.
    * It is expected for this produce test to be defined in the scenario, the tail consumer to be scheduled to run until the end of said produce test and for the topics field to not be populated.
* `topics` (*optional*) - The topics these consumers will subscribe to.  One and only one of this or `topics_from_test` below must be set.
* `duration` (*optional*) - The duration, as a Go [duration](https://golang.org/pkg/time/#ParseDuration) construct, that this test will run.
* `step_messages_per_second` (*optional*) - The number of messages per second this workload will limit itself to.  Note: This number is divided between each task per step (fanout).  The default is `math.MaxInt32`.
* `tasks_per_step` (*optional*) = The number of Trogdor tasks to create per step (fanout).  The default is equal to the number of trogdor agents.
* `slow_start_per_step_ms` (*optional*) - If specified, each task in a given step (fanout) will be progressively delayed by this amount, in milliseconds, as a way to ramp up the load.  The task numbered N will start delayed by `(N-1) * [slow_start_per_step_ms]` milliseconds, and it's duration will be shortened by the same amount of time. The default is 0, or no ramp up.
* `consumer_group` (*optional*) - Override the generated consumer groups and use this one instead.  If specified, `fanout` does not generate new consumer groups, and all tasks are part of the same one.

##### Single Test Example
See [example.json](example.json) for a sample configuration.
A configuration like
```json
{
  "scenario_name": "ExampleTest",
  "test_definitions": {
    "test_type": "ProgressiveWorkload",
    "test_name": "test-produce",
    "test_parameters": {
      "workload_type": "Produce",
      "step_duration_ms": 60000,
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
would result in 3 steps, consisting of the following throughputs (10 MB/s, 15 MB/s, 20 MB/s). Each step would last one minute and there would be one minute of downtime in between each step.
Note that the _schedule_ field is optional. If omitted, all tasks get scheduled at once.

##### Multi-Test Example
```json
{
  "scenario_name": "TestCPKAFKA",
  "schedule": {
    "A": {},
    "B": {
      "start_delay": "1m",
      "start_after_begin": ["A"]
    },
    "C": {
      "run_until_end_of": ["A"]
    },
    "D": {
      "start_delay": "0s",
      "start_after_begin": ["B"],
      "run_until_end_of": ["B"]
    }
  },
  "test_definitions": [
    {
      "test_type": "ProgressiveWorkload",
      "test_name": "A",
      "test_parameters": {
        "workload_type": "Produce",
        "step_duration_ms": 60000,
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
        "step_duration_ms": 30000,
        "partition_count": 15,
        "step_cooldown_ms": 1000,
        "start_throughput_mbs": 10,
        "end_throughput_mbs": 20,
        "throughput_increase_per_step_mbs": 5,
        "message_size_bytes": 255
      }
    },
    {
      "test_type": "TailConsume",
      "test_name": "C",
      "test_parameters": {
        "fanout": 2,
        "topics_from_test": "A"
      }
    },
    {
      "test_type": "TailConsume",
      "test_name": "D",
      "test_parameters": {
        "fanout": 2,
        "topics_from_test": "B"
      }
    }
  ]
}
```
In this example, we have two produce tasks. The second task, _B_, will start one minute after _A_ starts. Task _C_ will start in the beginning with _A_ and run until _A_ ends, and task _D_ will start with _B_ and run until _B_ ends. 

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
