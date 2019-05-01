FROM confluent-docker.jfrog.io/confluentinc/cc-service-base:1.10

ARG version

WORKDIR /root
COPY .ssh .ssh
COPY .netrc ./
RUN ssh-keyscan -t rsa github.com > /root/.ssh/known_hosts

WORKDIR /go/src/github.com/confluentinc/ce-kafka/cc-services/soak_cluster
COPY . .
# Replace mk-include link with the local context copy
RUN rm ./mk-include
COPY .mk-include.local-context-copy ./mk-include
RUN make deps DEP_ARGS=-vendor-only VERSION=${version}

RUN make lint-go test-go
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 make build-go GO_OUTDIR= VERSION=${version}

FROM confluent-docker.jfrog.io/confluentinc/cc-built-base:v1.0.0

COPY --from=0 /soak-clients /

ENTRYPOINT ["/soak-clients"]

# keep container running
CMD tail -f /dev/null
