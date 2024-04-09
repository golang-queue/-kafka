package kafka

import (
	"context"

	"github.com/golang-queue/queue"
	"github.com/golang-queue/queue/core"
)

// Option for queue system
type Option func(*options)

type options struct {
	runFunc   func(context.Context, core.QueuedMessage) error
	logger    queue.Logger
	addr      string
	network   string
	queue     string
	topic     string
	partition int //kafka's partition
}

// WithAddr setup the URI
func WithAddr(addr string) Option {
	return func(w *options) {
		w.addr = addr
	}
}

func WithNetwork(network string) Option {
	return func(w *options) {
		w.network = network
	}
}

// WithTopic setup the Topic
func WithTopic(topic string) Option {
	return func(w *options) {
		w.topic = topic
	}
}

// WithPartition setup the partition
func WithPartition(partition int) Option {
	return func(w *options) {
		w.partition = partition
	}
}

// WithQueue setup the queue name
func WithQueue(val string) Option {
	return func(w *options) {
		w.queue = val
	}
}

// WithRunFunc setup the run func of queue
func WithRunFunc(fn func(context.Context, core.QueuedMessage) error) Option {
	return func(w *options) {
		w.runFunc = fn
	}
}

// WithLogger set custom logger
func WithLogger(l queue.Logger) Option {
	return func(w *options) {
		w.logger = l
	}
}

func newOptions(opts ...Option) options {
	defaultOpts := options{}
	return defaultOpts
}
