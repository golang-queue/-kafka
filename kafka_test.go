package kafka

import (
	"testing"

	"github.com/golang-queue/queue"
	"github.com/stretchr/testify/assert"
	//"github.com/stretchr/testify/assert"
)

// func TestFetchData(t *testing.T) {
// 	// m := mockMessage{
// 	// 	Message: "foo",
// 	// }
// 	// w := NewWorker()
// 	// q, err := queue.NewQueue(
// 	// 	queue.WithWorker(w),
// 	// 	queue.WithWorkerCount(2),
// 	// )
// 	// assert.NoError(t, err)
// 	// q.Start()
// 	// time.Sleep(50 * time.Millisecond)
// 	// q.Shutdown()
// 	// // can't queue task after shutdown
// 	// err = q.Queue(m)
// 	// assert.Error(t, err)
// 	// assert.Equal(t, queue.ErrQueueShutdown, err)
// 	// q.Wait()
// 	fmt.Printf("start\n")
// 	InitConsumer(WithAddr("localhost"),
// 		WithPartition(1),
// 		WithTopic("hello"))
// 	fmt.Printf("end\n")
// }

func TestNewWork(t *testing.T) {
	w :=
		NewWorker(
			WithAddr("localhost"),
			WithNetwork("tcp"),
			WithPartition(1),
			WithTopic("hello"),
		)
	q, err := queue.NewQueue(
		queue.WithWorker(w),
		queue.WithWorkerCount(1),
	)
	assert.NoError(t, err)
	q.Start()
	// time.Sleep(100 * time.Millisecond)
	// //assert.Equal(t, 1, int(q.metric.BusyWorkers()))
	// time.Sleep(600 * time.Millisecond)
	// q.Shutdown()
	// q.Wait()
}
