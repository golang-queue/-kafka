package kafka

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang-queue/queue"
	"github.com/golang-queue/queue/core"
	kafkaAPI "github.com/segmentio/kafka-go"
)

var _ core.Worker = (*Worker)(nil)

// one consumer connect to kafka broker
var kafkaConsumer *KafkaConsumer

type KafkaConsumer struct {
	//stopFlag int32
	opts   options
	reader *kafkaAPI.Reader
	ring   queueAPI.Ring
}
type ConnWaitGroup struct {
	DialFunc func(context.Context, string, string) (net.Conn, error)
	sync.WaitGroup
}

// start consumer, get message from kafka
func InitConsumer(opts ...Option) {
	//var err error
	kafkaConsumer = &KafkaConsumer{
		opts: newOptions(opts...),
	}

	_, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 创建client，创建topic，创建shutdown
	// client, shutdown := newLocalClientAndTopic(kafkaConsumer.opts.addr, kafkaConsumer.opts.topic,
	// 	kafkaConsumer.opts.partition)
	reader := kafkaAPI.NewReader(kafkaAPI.ReaderConfig{
		Brokers:  []string{fmt.Sprintf("%s:9092", kafkaConsumer.opts.addr)},
		Topic:    kafkaConsumer.opts.topic,
		MinBytes: 1,
		MaxBytes: 10e6,
		MaxWait:  100 * time.Millisecond,
		//Logger:   newTestKafkaLogger(t, ""),
	})
	kafkaConsumer.reader = reader
	//kafkaConsumer.shutdown = shutdown
	fmt.Printf("get data.\n")
	GetData()
	fmt.Printf("shutdown now!!!\n")
	defer reader.Close()
}

// func newLocalClient(address string) (*kafkaAPI.Client, func()) {
// 	return newClient(kafkaAPI.TCP(address))
// }

// func newClient(addr net.Addr) (*kafkaAPI.Client, func()) {
// 	conns := &ktesting.ConnWaitGroup{
// 		DialFunc: (&net.Dialer{}).DialContext,
// 	}

// 	transport := &kafkaAPI.Transport{
// 		Dial:     conns.Dial,
// 		Resolver: kafkaAPI.NewBrokerResolver(nil),
// 	}

// 	client := &kafkaAPI.Client{
// 		Addr:      addr,
// 		Timeout:   5 * time.Second,
// 		Transport: transport,
// 	}

// 	return client, func() { transport.CloseIdleConnections(); conns.Wait() }
// }

// func newLocalClientAndTopic(address string, topic string, partition int) (*kafkaAPI.Client, func()) {
// 	//topic := makeTopic()
// 	client, shutdown := newLocalClientWithTopic(address, topic, partition)
// 	return client, shutdown
// }

// func newLocalClientWithTopic(address string, topic string, partitions int) (*kafkaAPI.Client, func()) {
// 	client, shutdown := newLocalClient(address)
// 	if err := clientCreateTopic(client, topic, partitions); err != nil {
// 		shutdown()
// 		panic(err)
// 	}
// 	return client, func() {
// 		client.DeleteTopics(context.Background(), &kafkaAPI.DeleteTopicsRequest{
// 			Topics: []string{topic},
// 		})
// 		shutdown()
// 	}
// }

// func clientCreateTopic(client *kafkaAPI.Client, topic string, partitions int) error {
// 	_, err := client.CreateTopics(context.Background(), &kafkaAPI.CreateTopicsRequest{
// 		Topics: []kafkaAPI.TopicConfig{{
// 			Topic:             topic,
// 			NumPartitions:     partitions,
// 			ReplicationFactor: 1,
// 		}},
// 	})
// 	if err != nil {
// 		return err
// 	}

// 	// Topic creation seems to be asynchronous. Metadata for the topic partition
// 	// layout in the cluster is available in the controller before being synced
// 	// with the other brokers, which causes "Error:[3] Unknown Topic Or Partition"
// 	// when sending requests to the partition leaders.
// 	//
// 	// This loop will wait up to 2 seconds polling the cluster until no errors
// 	// are returned.
// 	for i := 0; i < 20; i++ {
// 		r, err := client.Fetch(context.Background(), &kafkaAPI.FetchRequest{
// 			Topic:     topic,
// 			Partition: 0,
// 			Offset:    0,
// 		})
// 		if err == nil && r.Error == nil {
// 			break
// 		}
// 		time.Sleep(100 * time.Millisecond)
// 	}

// 	return nil
// }

// 获取消息发送到队列中去
func GetData() {
	for {
		// select {
		// case <-time.After(leftTime):
		// 	return //context.DeadlineExceeded
		// // case err := <-done: // job finish
		// // 	return err
		// // case p := <-panicChan:
		// // 	panic(p)
		// default:
		// 接收消息
		fmt.Printf("start fetch data")
		res, err := kafkaConsumer.reader.ReadMessage(context.Background())
		if err != nil {
			//t.Fatal(err)
			fmt.Printf("%v", err)
		}
		// 打印出消息，后续放入队列中去
		fmt.Printf("%v", res)
		// }
	}
}

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
	// send message
	base := time.Now()

	msg := kafkaAPI.Message{
		Time:  base.Truncate(time.Millisecond),
		Value: job.Bytes(),
	}
	if w.opts.compression == nil {
		_, err = w.conn.WriteMessages(msg)
	} else {
		_, err = w.conn.WriteCompressedMessages(w.opts.compression, msg)
	}
	// if err != nil {
	// 	t.Fatal(err)
	// }
	return err
}

func (w *Worker) Request() (core.QueuedMessage, error) {
	_ = w.startConsumer()
	return nil, queue.ErrNoTaskInQueue

}
