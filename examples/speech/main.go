// Command speech submits one /v1/audio/speech request through the redis-sortedset transport
// and writes the audio from its result to a file.
//
//	go run ./examples/speech -redis-url redis://localhost:6379 \
//	  -request-queue request-sortedset -result-queue result-list \
//	  -model Qwen/Qwen3-TTS-12Hz-1.7B-CustomVoice -voice vivian -out speech.wav
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/llm-d/llm-d-async/api"
	"github.com/llm-d/llm-d-async/producer"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	redisURL := flag.String("redis-url", "redis://localhost:6379", "Redis URL")
	requestQueue := flag.String("request-queue", "request-sortedset", "request queue the processor reads")
	resultQueue := flag.String("result-queue", "result-list", "the processor's result_queue_name")
	model := flag.String("model", "", "TTS model name")
	voice := flag.String("voice", "", "voice name")
	input := flag.String("input", "Hello from the async queue.", "text to speak")
	format := flag.String("format", "wav", "response_format: wav, mp3, flac, ...")
	out := flag.String("out", "speech.wav", "file to write the audio to")
	timeout := flag.Duration("timeout", 5*time.Minute, "request deadline")
	flag.Parse()

	p, err := producer.NewRedisSortedSetProducer(producer.RedisSortedSetConfig{
		RedisURL:         *redisURL,
		RequestQueueName: *requestQueue,
		ResultQueueName:  *resultQueue,
	})
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer func() { _ = p.Close() }()

	payload, err := json.Marshal(map[string]string{"model": *model, "voice": *voice, "input": *input, "response_format": *format})
	if err != nil {
		return fmt.Errorf("encode payload: %w", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	now := time.Now()
	req := &api.RequestMessage{
		ID:       fmt.Sprintf("speech-%d", now.UnixNano()),
		Created:  now.Unix(),
		Deadline: now.Add(*timeout).Unix(),
		Endpoint: "/v1/audio/speech",
		Payload:  payload,
	}
	if err := p.SubmitRequest(ctx, req); err != nil {
		return fmt.Errorf("submit: %w", err)
	}

	res, err := p.GetResult(ctx)
	if err != nil {
		return fmt.Errorf("get result: %w", err)
	}
	if res.ID != req.ID {
		return fmt.Errorf("result %q is not for request %q: use a result queue of your own", res.ID, req.ID)
	}
	if res.StatusCode == 0 {
		return fmt.Errorf("no response: %s: %s", res.ErrorCode, res.ErrorMessage)
	}
	body, err := res.Body()
	if err != nil {
		return fmt.Errorf("decode result: %w", err)
	}
	if res.StatusCode != 200 {
		return fmt.Errorf("status %d: %s", res.StatusCode, body)
	}
	if err := os.WriteFile(*out, body, 0o644); err != nil {
		return fmt.Errorf("write audio: %w", err)
	}
	fmt.Printf("%s: %d bytes of %s\n", *out, len(body), res.ContentType)
	for name, value := range res.Headers {
		fmt.Printf("  %s: %s\n", name, value)
	}
	return nil
}
