FROM golang:1.26-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /kafka-agent ./cmd
RUN mkdir /data-init

FROM gcr.io/distroless/static-debian12
COPY --from=builder /kafka-agent /kafka-agent
COPY --from=builder --chown=nonroot:nonroot /data-init /data
COPY config/config.yaml /etc/kafka-agent/config.yaml
EXPOSE 8080
VOLUME ["/data"]
USER nonroot:nonroot
ENTRYPOINT ["/kafka-agent", "--config", "/etc/kafka-agent/config.yaml"]
