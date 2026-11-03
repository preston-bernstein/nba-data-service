//go:build integration

package events

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestKafkaSameKeySamePartitionInOrder(t *testing.T) {
	brokers := os.Getenv("KAFKA_TEST_BROKERS")
	if brokers == "" {
		t.Skip("KAFKA_TEST_BROKERS not set")
	}
	list := strings.Split(brokers, ",")
	topic := fmt.Sprintf("it.game.updated.%d", time.Now().UnixNano())

	admClient, err := kgo.NewClient(kgo.SeedBrokers(list...))
	if err != nil {
		t.Fatal(err)
	}
	defer admClient.Close()
	adm := kadm.NewClient(admClient)
	setupCtx, setupCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer setupCancel()
	if _, err := adm.CreateTopic(setupCtx, 3, 1, nil, topic); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if _, err := adm.DeleteTopics(ctx, topic); err != nil {
			t.Errorf("delete topic: %v", err)
		}
	}()

	pub, err := NewKafkaPublisher(list, topic)
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()

	g := sampleGame()
	g.ID = fmt.Sprintf("it-%d", time.Now().UnixNano())
	first := NewEvent(g, ChangeCreated, time.Now())
	g.Score.Home += 2
	second := NewEvent(g, ChangeScore, time.Now())

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := pub.Publish(ctx, first); err != nil {
		t.Fatal(err)
	}
	if err := pub.Publish(ctx, second); err != nil {
		t.Fatal(err)
	}

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(list...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	var ids []string
	var parts []int32
	for len(ids) < 2 {
		fetches := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			t.Fatalf("timed out; saw %v", ids)
		}
		fetches.EachRecord(func(r *kgo.Record) {
			if string(r.Key) != g.ID {
				return
			}
			var ev Event
			if err := json.Unmarshal(r.Value, &ev); err != nil {
				t.Errorf("bad value: %v", err)
				return
			}
			ids = append(ids, ev.EventID)
			parts = append(parts, r.Partition)
		})
	}
	if ids[0] != first.EventID || ids[1] != second.EventID {
		t.Fatalf("out of order: got %v want %s,%s", ids, first.EventID, second.EventID)
	}
	if parts[0] != parts[1] {
		t.Fatalf("different partitions: %v", parts)
	}
}
