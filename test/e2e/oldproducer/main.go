// Command oldproducer drives the v0.10.0 producer, which writes request
// payloads inline, so e2e tests can run it against the current dispatcher.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/llm-d/llm-d-async/api"
	"github.com/llm-d/llm-d-async/producer"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		return errors.New("usage: oldproducer submit|cancel|result [flags]")
	}
	fs := flag.NewFlagSet(args[0], flag.ContinueOnError)
	redisURL := fs.String("redis-url", "", "Redis URL")
	requestQueue := fs.String("request-queue", "", "request sorted set")
	resultQueue := fs.String("result-queue", "", "result list")
	id := fs.String("id", "", "request ID for submit and cancel")
	timeout := fs.Duration("timeout", time.Minute, "time limit for the command")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}

	p, err := producer.NewRedisSortedSetProducer(producer.RedisSortedSetConfig{
		RedisURL:         *redisURL,
		RequestQueueName: *requestQueue,
		ResultQueueName:  *resultQueue,
	})
	if err != nil {
		return err
	}
	defer p.Close() //nolint:errcheck

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	switch args[0] {
	case "submit":
		now := time.Now()
		return p.SubmitRequest(ctx, &api.RequestMessage{
			ID:       *id,
			Created:  now.Unix(),
			Deadline: now.Add(time.Hour).Unix(),
			Payload:  map[string]any{"model": "test-model", "prompt": *id},
		})
	case "cancel":
		return p.CancelRequests(ctx, []string{*id})
	case "result":
		result, err := p.GetResult(ctx)
		if err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(result)
	}
	return fmt.Errorf("unknown command %q", args[0])
}
