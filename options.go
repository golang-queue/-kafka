package kafka

import (
	"context"

	"github.com/golang-queue/queue"
	"github.com/golang-queue/queue/core"
)

// Option for queue system
type Option func(*options)

type options struct {
	runFunc func(context.Context, core.QueuedMessage) error
	logger  queue.Logger
	queue   string
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
