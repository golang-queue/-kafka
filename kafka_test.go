package kafka

import (
	"fmt"
	"testing"
)

// import (
// 	"testing"
// 	"time"

// 	"github.com/golang-queue/queue"
// 	"github.com/stretchr/testify/assert"
// )

func TestFetchData(t *testing.T) {
	// m := mockMessage{
	// 	Message: "foo",
	// }
	// w := NewWorker()
	// q, err := queue.NewQueue(
	// 	queue.WithWorker(w),
	// 	queue.WithWorkerCount(2),
	// )
	// assert.NoError(t, err)
	// q.Start()
	// time.Sleep(50 * time.Millisecond)
	// q.Shutdown()
	// // can't queue task after shutdown
	// err = q.Queue(m)
	// assert.Error(t, err)
	// assert.Equal(t, queue.ErrQueueShutdown, err)
	// q.Wait()
	fmt.Printf("start\n")
	InitConsumer(WithAddr("localhost"),
		WithPartition(1),
		WithTopic("hello"))
	fmt.Printf("end\n")
}
