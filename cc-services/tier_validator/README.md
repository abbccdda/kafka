# TierMetadataValidator as a cc-service

Given the necessity of continual integrity monitoring for `tier-topic` events, we have created a cc-service out of
`TierMetadataValidator` tool. This service can be created from the supplied Dockerfile.

## Docker Image

The name of the docker image for this service is `confluentinc/cc-tier-validator`. The latest version can be obtained
the JFrog artifactory search with the image name set as: `confluentinc/cc-tier-validator`.

Or, locally, the presence of this image can be seen by running:

```shell script
make show-docker-all
```

It should have a section titled: `Docker info for tier-validator-services` with the details.
In case these details are absent or after making local changes, a new local image can be built using:

```shell script
make build-docker build-docker-cc-services
```

Once, the docker image is built, tagged and pushed onto the remote repositories, we need to update
`spec.containers.image` in the YAML files:

- [`tier_validator_deployment.yaml`](tier_validator_deployment.yaml): Used to create a standalone `tier-validator` pod
- [`tier_validator_cron.yaml`](tier_validator_cron.yaml): Used to create the nightly `tier-validator` cron job

## Standalone pod

A standalone `tier-validator` pod can be created in any environment from the bastions for one-off investigations.

A detailed runbook for operating and debugging this is in the `cc-documentation` repo ([link](https://github.com/confluentinc/cc-documentation/tree/master/Operations/RunBook/KafkaCore/Running%20TierMetadataValidator.md)).

## Kubernetes cron job

In addition to the standalone capabilities, the `tier-validator-cron` service have been setup in the `soak` cluster
to run daily at midnight. The cron job currently solely executes the [runner script](scripts/tier_validator_cron_runner.sh).

As part of this script, the following tasks are completed:
- All the pods that are part of the kafka cluster in the namespace are verified using `kafka.tier.tools.TierMetadataValidator`
- The Python wrapper [script](scripts/tier_validator_wrapper.py) manages the copying of the `.tierstate` files from the brokers onto the local pod
- Once the run is complete, the validator log files from all the brokers as well as the overall stdout is uploaded to S3

> The log-file prefix in the S3 bucket is: `tier_validator_cron_logs`


### Monitoring and Debugging

Some of the monitoring and debugging commands around the same service are as follows:

> Setup utility: `kube_namespace=pkc-lgmzn`

- Verify if the job is installed in the bastion:
```shell script
kubectl -n $kube_namespace get jobs -l app=tier-validator-cron
```

- Install the job if the above command returns `No resources found.`
```shell script
kubectl -n $kube_namespace create -f tier_validator_cron.yaml
```

- Run an adhoc instance of the job (out-of-schedule):
```shell script
kubectl -n $kube_namespace create job --from=cronjob/tier-validator-cron tier-validator-cron-manual
```

- Get pods spawned by the job:
```shell script
kubectl -n $kube_namespace describe job <job_instance> | grep "Created pod"
```

> Now, once the pod instance is obtained, we can log onto the pod to tail the various log files:

- Log-file for the Python wrapper script
```shell script
tail -f /mnt/tier_validator/workdir/python_stdout_stderr.log
```

- Log-file for the individual broker validation logs
```shell script
tail -f /mnt/tier_validator/workdir/2020-03-31-001/{kafka_pod_name}/run_validator.log
```

#### Retrieving logs

Both the `python_stdout_stderr.log` and the individual broker `run_validator.log` files are uploaded onto the S3 within
the defined path prefix. The available files can be searched from the pod by running:

```shell script
aws s3api list-objects --bucket cloud-cpdev-devel-tiered-us-west-2 --prefix tier_validator_cron_logs
{
    "Contents": [
        {
            "Key": "tier_validator_cron_logs/pkc-lgmzn/2020-03-30-22-46-33_tier-validator-cron-manual-l9xhm.tar",
            ...
        },
        ...
    ]
}
```

As shown above, the key (S3 path) is composed of: `{S3_prefix}/{kube_namespace}/{datetime}_{pod_id}.tar`

In addition to this, kubernetes `logs` api can also be used to capture any failed/error logs from a failed pod

```shell script
pods=$(kubectl -n $kube_namespace get pods --selector=app=tier-validator-cron --output=jsonpath={.items[*].metadata.name})
kubectl -n $kube_namespace logs $pods
```
