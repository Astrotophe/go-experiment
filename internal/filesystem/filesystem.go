package filesystem

import (
	"encoding/json"
	"github.com/Shopify/sarama"
	"log"
)

type filesystemEvent struct {


}

type filesystemDeleteEvent struct {

}

type filesystemCreateEvent struct {}

type filesystemMoveEvent struct {}



// Setup is run at the beginning of a new session, before ConsumeClaim
func (fe *filesystemEvent) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (fe *filesystemEvent) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (fe *filesystemEvent) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		log.Printf("Message claimed: value = %s, timestamp = %v, topic = %s", string(message.Value), message.Timestamp, message.Topic)
		err := fe.logic(message)
		if err != nil {
			return err
		}

		session.MarkMessage(message, "")
	}

	return nil
}

func (fe *filesystemEvent) logic(message *sarama.ConsumerMessage) error {
	var t map[string]interface{}
	err := json.Unmarshal(message.Value, &t)
	switch t["event_type"] {
	case "delete":
		toto := t["payload"].(map[string]interface{})
		log.Print(toto)
	default:
		log.Print("something else")
	}
	return err
}