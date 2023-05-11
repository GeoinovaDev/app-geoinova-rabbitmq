package rabbitmq

import (
	"encoding/json"
	"fmt"

	"github.com/GeoinovaDev/domain-painel-geoinova/core"
	"github.com/GeoinovaDev/domain-painel-geoinova/events"
	"github.com/streadway/amqp"
)

type listener struct {
	conn     *amqp.Connection
	exchange string
	queue    string
}

func NewListener(conn *amqp.Connection, exchange string, queue string) (*listener, error) {
	e := &listener{
		conn:     conn,
		exchange: exchange,
		queue:    queue,
	}

	err := e.config()
	if err != nil {
		return nil, err
	}

	return e, nil
}

func (e *listener) config() error {
	ch, err := e.conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	_, err = ch.QueueDeclare(e.queue, true, false, false, false, nil)
	return err
}

func (e *listener) Listen(eventNames ...string) (<-chan events.Event, <-chan error, error) {
	ch, err := e.conn.Channel()
	if err != nil {
		return nil, nil, err
	}
	defer ch.Close()

	for _, eventName := range eventNames {
		err := ch.QueueBind(e.queue, eventName, e.exchange, false, nil)
		if err != nil {
			return nil, nil, err
		}
	}

	msgs, err := ch.Consume(e.queue, "", false, false, false, false, nil)
	if err != nil {
		return nil, nil, err
	}

	eventsChan := make(chan events.Event)
	errorsChan := make(chan error)
	go func() {
		for msg := range msgs {
			rawEventName, ok := msg.Headers[EVENT_NAME_HEADER]
			if !ok {
				errorsChan <- fmt.Errorf("mensagem não contem o cabecalho " + EVENT_NAME_HEADER)
				msg.Nack(false, false)
				continue
			}

			eventName, ok := rawEventName.(string)
			if !ok {
				errorsChan <- fmt.Errorf("mensagem não contem o cabecalho " + EVENT_NAME_HEADER)
				msg.Nack(false, false)
				continue
			}

			var event events.Event

			switch eventName {
			case core.DETECCOES_CREATE_EVENT:
				event = new(core.DeteccoesCreateEvent)
			case core.DETECCOES_DELETE_EVENT:
				event = new(core.DeteccoesDeleteEvent)
			default:
				errorsChan <- fmt.Errorf("evento desconhecido")
				continue
			}

			err := json.Unmarshal(msg.Body, event)
			if !ok {
				errorsChan <- err
				continue
			}

			eventsChan <- event
			msg.Ack(false)
		}

	}()

	return eventsChan, errorsChan, nil
}
