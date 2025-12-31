# syntax=docker/dockerfile:1

ARG GO_VERSION=1.21

FROM golang:${GO_VERSION}-alpine AS builder
WORKDIR /app
ENV CGO_ENABLED=0 \
    GOCACHE=/app/.cache/go-build

RUN apk add --no-cache ca-certificates

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN mkdir -p $GOCACHE && go build -o /app/bin/server ./cmd/server

FROM alpine:3.19
WORKDIR /app
RUN addgroup -S app && adduser -S app -G app
RUN apk add --no-cache ca-certificates
COPY --from=builder /app/bin/server /usr/local/bin/server
USER app
EXPOSE 4000 9090
ENTRYPOINT ["/usr/local/bin/server"]
