package events

import (
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

// kafkajs cannot read snappy, so the producer must use gzip.
func TestKafkaPublisherUsesGzip(t *testing.T) {
	pub, err := NewKafkaPublisher([]string{"localhost:1"}, "t")
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()

	codecs, ok := pub.client.OptValue(kgo.ProducerBatchCompression).([]kgo.CompressionCodec)
	if !ok || len(codecs) != 1 {
		t.Fatalf("expected exactly one codec, got %#v", pub.client.OptValue(kgo.ProducerBatchCompression))
	}
	if codecs[0] != kgo.GzipCompression() {
		t.Fatalf("expected gzip, got %#v", codecs[0])
	}
}
