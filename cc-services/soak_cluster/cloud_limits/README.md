# Cloud Limits Tests

These tests define what is specified in the Cloud Limits workload one-pagers.

Phase 1 is described here: https://confluentinc.atlassian.net/wiki/spaces/QERM/pages/873662767/Evaluating+Kafka+Cloud+Limits+-+Phase+1

## Phase 1

The following basic tests are described as part of Phase 1:

### Experiment 1
* R:W Ratio is `1:1`.
* `compression.type` is set to `lz4`.
* `linger.ms` is set to `100`.

### Experiment 2
This test is identical to *Experiment 1*, but with the following configuration changes:
* R:W Ratio is `2:1`.

### Experiment 3
Once again, this is identical to *Experiments 1-2*, but we're running with a different ratio and adding a few more scenarios.
* R:W Ratio is `3:1`
* 3 Scenarios:
  * All Live Consumers
    * All consumers are reading messages as fast as the producer can send them. 
  * Unthrottled Lag Consumer
    * One consumer with `auto.offset.reset` set to `earliest` is started 1 hour after the produce workload starts.
    * The consumer eventually catches up and becomes a live consumer.
  * Throttled Lag Consumer
    * One consumer with `auto.offset.reset` set to `earliest` is started 1 hour after the produce workload starts.
    * This consumer also has `task_messages_per_second` set so it does not catch up to the live consumers.
    * This consumer remains lagged behind for the remainder of the test.

### Experiment 4
This is identical to *Experiment 3*, but we're running with a different ratio and removing the *Unthrottled Lag Consumer* test.
* R:W Ratio is `4:1`
* 2 Scenarios:
  * All Live Consumers
    * All consumers are reading messages as fast as the producer can send them.
  * Throttled Lag Consumer
    * One consumer with `auto.offset.reset` set to `earliest` is started 1 hour after the produce workload starts.
    * This consumer also has `task_messages_per_second` set so it does not catch up to the live consumers.
    * This consumer remains lagged behind for the remainder of the test.

---
## `admin.conf` Configuration File
The `admin.conf` configuration file has all the additional configuration parameters we need to add to the soak client CLI to get these tests to behave as we expect.  This file needs to be merged with `/mnt/config/client/client_properties.json` locally, and then the `TROGDOR_ADMIN_CONF` environment variable needs to be changed to point to the modified local file.
