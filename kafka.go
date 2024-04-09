package kafka

import (
	"context"

	"github.com/golang-queue/queue"
	"github.com/golang-queue/queue/core"
)

var _ core.Worker = (*Worker)(nil)

type Worker struct {
	//
	//shutdown  func() //
	// stop      chan struct{}
	// stopFlag  int32
	// stopOnce  sync.Once
	// startOnce sync.Once
	opts options
	// conn      kafkaAPI.Dialer
}

func NewWorker(opts ...Option) *Worker {
	//var err error
	w := &Worker{
		opts: newOptions(opts...),
	}
	return w
}

func (w *Worker) startConsumer() (err error) {

	// err := nil

	// if err != nil {
	// 	//
	// }
	return err
}

// Run start the worker
func (w *Worker) Run(ctx context.Context, task core.QueuedMessage) error {
	return w.opts.runFunc(ctx, task)
}

// Shutdown worker
func (w *Worker) Shutdown() (err error) {

	return err
}

// Queue send notification to queue
func (w *Worker) Queue(job core.QueuedMessage) (err error) {
	//err := nil

	return err

}

func (w *Worker) Request() (core.QueuedMessage, error) {
	_ = w.startConsumer()
	return nil, queue.ErrNoTaskInQueue

}
