# syntax=docker/dockerfile:1

ARG GO_VERSION=1.24

FROM golang:${GO_VERSION}-alpine@sha256:8bee1901f1e530bfb4a7850aa7a479d17ae3a18beb6e09064ed54cfd245b7191 AS builder
WORKDIR /app
ENV CGO_ENABLED=0 \
    GOCACHE=/app/.cache/go-build

RUN apk add --no-cache ca-certificates=20261020-r0

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN mkdir -p $GOCACHE && go build -o /app/bin/server ./cmd/server

FROM alpine:3.19@sha256:6baf43584bcb78f2e5847d1de515f23499913ac9f12bdf834811a3145eb11ca1
WORKDIR /app
RUN addgroup -S app && adduser -S app -G app
RUN apk add --no-cache ca-certificates=20261020-r0
COPY --from=builder /app/bin/server /usr/local/bin/server
USER app
EXPOSE 4000 9090
ENTRYPOINT ["/usr/local/bin/server"]
