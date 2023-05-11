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
	conn     *amqp.Connection
	exchange string
}

func NewEmitter(conn *amqp.Connection, exchange string) (*emitter, error) {
	e := &emitter{
		conn:     conn,
		exchange: exchange,
	}

	err := e.config()
	if err != nil {
		return nil, err
	}

	return e, nil
}

func (e *emitter) config() error {
	ch, err := e.conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	return ch.ExchangeDeclare(e.exchange, "topic", true, false, false, false, nil)
}

func (e *emitter) Emit(event events.Event) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}

	ch, err := e.conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	msg := amqp.Publishing{
		Headers:     amqp.Table{EVENT_NAME_HEADER: event.Name()},
		Body:        payload,
		ContentType: "application/json",
	}

	return ch.Publish(e.exchange, event.Name(), false, false, msg)
}
