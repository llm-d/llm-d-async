package api

import "context"

type Flow interface {

	// Characteristic of the impl
	Characteristics() Characteristics

	// starts processing requests.
	Start(ctx context.Context)

	// returns the channels for requests. Implementation is responsible for publishing on these channels.
	RequestChannels() []RequestChannel
	// returns the channel that accepts messages to be retries with their backoff delay. Implementation is responsible
	// for consuming messages on this channel.
	RetryChannel() chan RetryMessage
	// returns the channel for storing the results. Implementation is responsible for consuming messages on this channel.
	ResultChannel() chan ResultMessage
}

type Characteristics struct {
	HasExternalBackoff bool
}

type RequestMergePolicy interface {
	MergeRequestChannels(channels []RequestChannel) EmbelishedRequestChannel
}

// TODO: Consider per-message metadata map[string]string
// add enpoint to message level.
type RequestMessage struct {
	Id              string            `json:"id"`
	RetryCount      int               `json:"retry_count,omitempty"` // TODO: Consider
	DeadlineUnixSec string            `json:"deadline"`              // TODO: check about using int64, change name to timeout
	Payload         map[string]any    `json:"payload"`
	Metadata        map[string]string `json:"metadata,omitempty"`
}

type RequestChannel struct {
	Channel chan RequestMessage

	InferenceObjective string
	RequestPathURL     string
}

type EmbelishedRequestChannel struct {
	Channel chan EmbelishedRequestMessage
}

type EmbelishedRequestMessage struct {
	RequestMessage
	HttpHeaders    map[string]string
	RequestPathURL string
	Metadata       map[string]string
}

type RetryMessage struct {
	EmbelishedRequestMessage
	BackoffDurationSeconds float64
}

// optional field of httpstatus, golang error?
type ResultMessage struct {
	Id       string            `json:"id"`
	Payload  string            `json:"payload"`
	Metadata map[string]string `json:"-"`
}
