FROM golang:1.24-alpine AS build
WORKDIR /src
RUN apk add --no-cache git
COPY go.mod go.sum* ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/kv-node ./cmd/node && \
    CGO_ENABLED=0 go build -o /out/kvctl ./cmd/client

FROM alpine:3.21
RUN adduser -D -u 10001 kv && mkdir -p /data && chown -R kv:kv /data
USER kv
WORKDIR /app
COPY --from=build /out/kv-node /kv-node
COPY --from=build /out/kvctl /kvctl
EXPOSE 50051 7000
ENTRYPOINT ["/kv-node"]
