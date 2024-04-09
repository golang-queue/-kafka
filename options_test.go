package kafka

import (
	"testing"

	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

// type mockMessage struct {
// 	Message string
// }

// func (m mockMessage) Bytes() []byte {
// 	return []byte(m.Message)
// }

// func TestShutdownWorkFlow(t *testing.T) {
// 	w := NewWorker(
// 		WithQueue("test"),
// 	)
// 	q, err := queue.NewQueue(
// 		queue.WithWorker(w),
// 		queue.WithWorkerCount(2),
// 	)
// 	assert.NoError(t, err)
// 	q.Start()
// 	time.Sleep(1 * time.Second)
// 	q.Shutdown()
// 	// check shutdown once
// 	q.Shutdown()
// 	q.Wait()
// }
