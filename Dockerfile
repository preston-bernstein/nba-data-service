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

FROM alpine:3.24@sha256:f59abe656b692d901e184be3445e084786602108599d2a51d229c0f5a48c71bf
WORKDIR /app
RUN addgroup -S app && adduser -S app -G app
RUN apk add --no-cache ca-certificates=20261020-r0
COPY --from=builder /app/bin/server /usr/local/bin/server
USER app
EXPOSE 4000 9090
ENTRYPOINT ["/usr/local/bin/server"]
