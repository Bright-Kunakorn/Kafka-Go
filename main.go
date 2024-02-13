package main

import (
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/Shopify/sarama"
)

var buffer = make([]byte, 0, 10)
var all sync.WaitGroup

var bufferChan = make(chan bool, 1)

func main() {
	rand.Seed(10)
	all.Add(1)

	config := sarama.NewConfig()
	config.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer([]string{"localhost:9092"}, config)
	if err != nil {
		fmt.Printf("Failed to create Kafka producer: %v\n", err)
		return
	}

	consumer, err := sarama.NewConsumer([]string{"localhost:9092"}, config)
	if err != nil {
		fmt.Printf("Failed to create Kafka consumer: %v\n", err)
		return
	}

	topic := "logs"

	go writer('a', producer, topic)
	go writer('b', producer, topic)
	go consumers(consumer, topic)

	all.Wait()
	producer.Close()
	consumer.Close()
}

func writer(c byte, producer sarama.SyncProducer, topic string) {
	for i := 0; i < 5; i++ {
		time.Sleep(time.Duration(rand.Int63n(1e9)))
		bufferChan <- true
		lb := len(buffer)
		if lb < cap(buffer) {
			buffer = buffer[:lb+1]
			buffer[lb] = c

			message := &sarama.ProducerMessage{
				Topic: topic,
				Value: sarama.StringEncoder(string(buffer)),
			}
			_, _, err := producer.SendMessage(message)
			if err != nil {
				fmt.Printf("Failed to write to Kafka: %v\n", err)
			} else {
				fmt.Printf("'%c' written to Kafka. buffer contents: %s\n", c, string(buffer))
			}
		}
		<-bufferChan
	}
}

func consumers(consumer sarama.Consumer, topic string) {
	partitionConsumer, err := consumer.ConsumePartition(topic, 0, sarama.OffsetOldest)
	if err != nil {
		fmt.Printf("Failed to create partition consumer: %v\n", err)
		all.Done()
		return
	}

	defer func() {
		if err := partitionConsumer.Close(); err != nil {
			fmt.Printf("Failed to close partition consumer: %v\n", err)
		}
	}()

	for i := 0; i < 5; {
		select {
		case message := <-partitionConsumer.Messages():
			value := string(message.Value)

			bufferChan <- true
			buffer = append(buffer, []byte(value)...)
			fmt.Printf("Received message from Kafka. Buffer contents: %s\n", string(buffer))
			<-bufferChan

			a := []byte{'a'}
			b := []byte{'b'}
			ai := bytes.Index(buffer, a)
			bi := bytes.Index(buffer, b)
			if ai >= 0 && bi >= 0 {
				if ai > bi {
					ai, bi = bi, ai
				}
				copy(buffer[bi:], buffer[bi+1:])
				copy(buffer[ai:], buffer[ai+1:])
				buffer = buffer[:len(buffer)-2]
				fmt.Printf("Pair removed from buffer. Buffer contents: %s\n", string(buffer))
				i++
			}

		case <-bufferChan:
			<-bufferChan
		}
	}

	all.Done()
}
