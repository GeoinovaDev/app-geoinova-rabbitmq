package rabbitmq

import (
	"fmt"

	"github.com/GeoinovaDev/domain-painel-geoinova/events"
	"github.com/streadway/amqp"
)

type NewMessageCallback func(string, []byte) (events.Event, error)

type listener struct {
	session      *Session
	exchange     string
	queue        string
	fnNewMessage NewMessageCallback
	eventsChan   chan events.Event
	errorsChan   chan error
}

func NewListener(session *Session, exchange string, queue string) (*listener, error) {
	e := &listener{
		session:    session,
		exchange:   exchange,
		queue:      queue,
		eventsChan: make(chan events.Event),
		errorsChan: make(chan error),
	}

	err := e.config()
	if err != nil {
		return nil, err
	}

	session.OnReconnect(func(c *amqp.Connection) {

	})

	return e, nil
}

func (e *listener) OnNewMessage(fn NewMessageCallback) *listener {
	e.fnNewMessage = fn
	return e
}

func (e *listener) config() (err error) {
	return e.session.Channel(func(ch *amqp.Channel) error {
		err = ch.ExchangeDeclare(e.exchange, "topic", true, false, false, false, nil)
		if err != nil {
			return err
		}

		_, err = ch.QueueDeclare(e.queue, true, false, false, false, nil)
		return err
	})
}

func (e *listener) Stream() <-chan events.Event {
	return e.eventsChan
}

func (e *listener) Errors() chan error {
	return e.errorsChan
}

func (e *listener) Listen(eventNames ...string) error {
	err := e.session.Channel(func(ch *amqp.Channel) error {
		for _, eventName := range eventNames {
			err := ch.QueueBind(e.queue, eventName, e.exchange, false, nil)
			if err != nil {
				return err
			}
		}

		return nil
	})

	if err != nil {
		return err
	}

	e.session.OnReconnect(func(c *amqp.Connection) {
		go e._listen()
	})

	go e._listen()

	return nil
}

func (e *listener) _listen() {
	e.session.Channel(func(ch *amqp.Channel) error {
		msgs, err := ch.Consume(e.queue, "", false, false, false, false, nil)
		if err != nil {
			return err
		}

		for msg := range msgs {
			rawEventName, ok := msg.Headers[EVENT_NAME_HEADER]
			if !ok {
				e.errorsChan <- fmt.Errorf("mensagem não contem o cabecalho " + EVENT_NAME_HEADER)
				msg.Nack(false, false)
				continue
			}

			eventName, ok := rawEventName.(string)
			if !ok {
				e.errorsChan <- fmt.Errorf("mensagem não contem o cabecalho " + EVENT_NAME_HEADER)
				msg.Nack(false, false)
				continue
			}

			event, err := e.fnNewMessage(eventName, msg.Body)
			if err != nil {
				e.errorsChan <- err
				continue
			}

			e.eventsChan <- event
			msg.Ack(false)
		}

		return nil
	})
}
