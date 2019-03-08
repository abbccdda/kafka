# Cloud

Here we store CCloud utilities (written in Go) that are closely tied to Kafka.
Currently we have code orchestrating the Kafka Core Soak cluster's clients via Trogdor under the `soak_cluster `package. It is responsible for creating Trogdor tasks in accordance to a given configuration consisting of desired soak testing length, throughput and etc.

# Soak Testing

### How to run on CPD

You need:
 - a running CPD cluster: https://github.com/confluentinc/cpd
 - A kafka cluster provisioned in that CPD
 - A pair of keys from that cluster

Target your CPD k8s in kubectl. It is recommended to inject the required creds for your CPD instance in the new namespace, so that your CPD can pull images from the docker repositories directly:

```
CPD_ID="$(cpd priv ls | fgrep ' gke-' | head -1 | awk '{print $1}')"

cpd priv export --id ${CPD_ID}

cpd priv inject-credentials --id ${CPD_ID} --namespace soak-tests
```

Then:

1. Update the configurations
    * `cc-services/trogdor/charts/values/local.yaml`
        - number of Trogdor agents
    * `cc-services/soak_cluster/charts/values/local.yaml`
        - with the same number of Trogdor agents,
        - bootstrap URL,
        - api keys/secrets
    * `cc-services/soak_cluster/charts/cc-soak-clients/templates/configMaps.yaml`
        - with the desired throughput/client count for every topic

2. Build and push your custom docker images. See the section [on building images](#building-new-docker-images) below.

3. Deploy the Trogdor agents
    * Optionally `make -C cc-services/trogdor helm-clean` to clean the state.
    * `make -C cc-services/trogdor helm-deploy-soak`
    * Inspect the pods: `kubectl get pods -n soak-tests`

4. Deploy the Soak clients
    * Optionally `make -C cc-services/soak_cluster helm-clean` to clean the state.
    * `make -C cc-services/soak_cluster helm-deploy-soak`

5. Trigger the `clients-spawner`:
    * Option 1: Edit the clients-spawner cronjob `schedule:` field in `kubectl edit cronjobs -n soak-tests cc-soak-clients-clients-spawner`.
    * Option 2: start a oneoff job: `kubectl create job --from=cronjob/cc-soak-clients-clients-spawner -n soak-tests cc-soak-clients-clients-spawner-oneoff`
    * Track the new clients-spawner pod from the cronjob: `kubectl get pods -n soak-tests`

If all goes well, you should have clients producing and consuming from the configured Kafka cluster.

It is easiest to inspect the tasks by manually querying the Trogdor coordinator.

```bash
kubectl port-forward cc-trogdor-service-coordinator-0 9002:9002 &
curl -X GET 'http://localhost:9002/coordinator/tasks?state=RUNNING'
```

### Building new docker images

To test a newer build of `cc-trogdor` or `cc-soak-clients`, you need to build a docker image and push it to [JFrog's Artifactory](https://confluent.jfrog.io/confluent/webapp/):

You can build and push for all the `cc-services` based on your current branch using:

```
make -C cc-services/soak_cluster build-docker push-docker
make -C cc-services/trogdor build-docker push-docker
```

With the previous command, Trogdor would use the latest image  of `ce-kafka` built from the `master` branch.

If you need a custom kafka base image for trogdor from the local branch run from the root of the project. It will build all the containers, including ce-kafka and the cc-services.

```
make build-docker build-docker-cc-services push-docker-cc-services
```
