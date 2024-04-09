module github.com/golang-queue/kafka

// go 1.22.0

go 1.18

require (
	github.com/golang-queue/queue v0.1.4-0.20221230133718-0314ef173f98
    github.com/segmentio/kafka-go v0.4.47
	github.com/stretchr/testify v1.8.1
	go.uber.org/goleak v1.2.0
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/goccy/go-json v0.10.0 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)


replace github.com/golang-queue/kafka => ../../