FROM golang:1.12.7 AS build_base

WORKDIR /go/src/github.com/confluentinc/ce-kafka/cc-readiness-check

ENV GO111MODULE=on

COPY go.mod .
COPY go.sum .

RUN go mod download

FROM build_base AS server_builder
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix nocgo -o /app .

FROM alpine AS cc_readiness_check
RUN apk add ca-certificates
COPY --from=server_builder /app /bin/cc-readiness-check
ENTRYPOINT ["/bin/cc-readiness-check"]
