package kafka

import (
	"context"
	"net"
	"sync/atomic"

	"github.com/golang-queue/queue"
	"github.com/golang-queue/queue/core"
	kafkaAPI "github.com/segmentio/kafka-go"
)

var _ core.Worker = (*Worker)(nil)

type Worker struct {
	//
	//shutdown  func() //
	// stop      chan struct{}
	stopFlag int32
	// stopOnce  sync.Once
	// startOnce sync.Once
	opts options
	conn *kafkaAPI.Conn
}

func NewWorker(opts ...Option) *Worker {
	var err error
	w := &Worker{
		opts: newOptions(opts...),
	}
	w.conn, err =
		//conn, err :=
		(&kafkaAPI.Dialer{
			Resolver: &net.Resolver{},
		}).DialLeader(context.Background(), w.opts.network,
			w.opts.addr, w.opts.topic, 0)
	if err != nil {
		w.opts.logger.Fatal("can't connect kafka: ", err)
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
	if atomic.LoadInt32(&w.stopFlag) == 1 {
		return queue.ErrQueueShutdown
	}
	return err

}

func (w *Worker) Request() (core.QueuedMessage, error) {
	_ = w.startConsumer()
	return nil, queue.ErrNoTaskInQueue

}
