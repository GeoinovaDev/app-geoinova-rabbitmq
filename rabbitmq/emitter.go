package rabbitmq

import (
	"encoding/json"

	"github.com/GeoinovaDev/domain-painel-geoinova/events"
	"github.com/streadway/amqp"
)

const (
	EVENT_NAME_HEADER = "x-event-name"
)

type emitter struct {
	session  *Session
	exchange string
}

func NewEmitter(session *Session, exchange string) (*emitter, error) {
	e := &emitter{
		session:  session,
		exchange: exchange,
	}

	err := e.config()
	if err != nil {
		return nil, err
	}

	return e, nil
}

func (e *emitter) config() error {
	return e.session.Channel(func(ch *amqp.Channel) error {
		return ch.ExchangeDeclare(e.exchange, "topic", true, false, false, false, nil)
	})
}

func (e *emitter) Emit(event events.Event) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}

	return e.session.Channel(func(ch *amqp.Channel) error {
		msg := amqp.Publishing{
			Headers:     amqp.Table{EVENT_NAME_HEADER: event.Name()},
			Body:        payload,
			ContentType: "application/json",
		}
		return ch.Publish(e.exchange, event.Name(), false, false, msg)
	})
}
