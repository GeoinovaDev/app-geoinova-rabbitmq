package rabbitmq

import (
	"encoding/json"

	"github.com/streadway/amqp"
)

type Publisher struct {
	uri      string
	conn     *amqp.Connection
	exchange string
}

type PublisherOption func(*Publisher)

func WithExchange(exchange string) PublisherOption {
	return func(p *Publisher) {
		p.exchange = exchange
	}
}

func NewPublisher(uri string, options ...PublisherOption) *Publisher {
	conn, err := amqp.Dial(uri)
	if err != nil {
		return nil
	}

	p := &Publisher{uri, conn, ""}
	for _, opt := range options {
		opt(p)
	}

	return p
}

func (p *Publisher) Publish(queue string, obj interface{}) error {
	ch, err := p.conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	q, err := ch.QueueDeclare(
		queue, // Nome da fila
		false, // Durabilidade da fila
		false, // Delete quando não estiver em uso
		false, // Exclusiva
		true,  // Sem espera
		nil,   // Argumentos extras
	)
	if err != nil {
		return err
	}

	data, err := converToJson(obj)
	if err != nil {
		return err
	}

	err = ch.Publish(
		p.exchange, // Exchange
		q.Name,     // Nome da fila
		false,      // Mandar mensagem imediatamente
		false,      // Mandar mensagem como persistente
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        data,
		})

	if err != nil {
		return err
	}

	return nil
}

func converToJson(obj interface{}) ([]byte, error) {
	jsonData, err := json.Marshal(obj)

	if err != nil {
		return []byte(""), err
	}

	return jsonData, nil
}
