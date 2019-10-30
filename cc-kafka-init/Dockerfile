FROM golang:1.12.7 AS build_base

WORKDIR /go/src/github.com/confluentinc/ce-kafka/cc-kafka-init

ENV GO111MODULE=on

COPY go.mod .
COPY go.sum .

RUN go mod download

FROM build_base AS server_builder
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix nocgo -o /cc-kafka-init .

FROM alpine AS cc_kafka_init

ENV KAFKA_CONFIG_DIR=/mnt/config

RUN apk add ca-certificates
COPY --from=server_builder /cc-kafka-init /bin/cc-kafka-init
COPY scripts/encode-host.sh /bin/cc-encode-host.sh
