/*
* Copyright (c) 2017-2020. Canal+ Group
* All rights reserved
 */
package kafka

import (
	"context"
	"github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"
)

// Consumer represents a Sarama consumer group consumer
type Consumer struct {
	config   *sarama.Config
	brokers  []string
	readChan chan<- string
}

func NewConsumer(config *sarama.Config, brokers []string, readChan chan<- string) *Consumer {
	return &Consumer{
		config:   config,
		readChan: readChan,
		brokers:  brokers,
	}
}

func (c *Consumer) Consume(ctx context.Context, group string, topics []string, doneChan chan<- struct{}, handler sarama.ConsumerGroupHandler) {
	log.Print("Starting a new consumer")

	client, err := sarama.NewConsumerGroup(c.brokers, group, c.config)
	if err != nil {
		log.Fatalf("Error creating consumer group client: %v", err)
	}
	done := false
	for !done {
		select {
		case <-ctx.Done():
			err := client.Close()
			if err != nil {
				log.WithError(err).Error("Failed to close Kafka client")
			}
			done = true
		default:
			if err := client.Consume(ctx, topics, handler); err != nil {
				log.Fatalf("Error from consumer: %v", err)
			}
		}
	}
	doneChan <- struct{}{}
}