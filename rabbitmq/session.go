package rabbitmq

import (
	"log"
	"time"

	"github.com/streadway/amqp"
)

const (
	RECONNECT_TIMEOUT = 20
)

type Session struct {
	url         string
	conn        *amqp.Connection
	chClose     chan *amqp.Error
	fnReconnect func(*amqp.Connection)
}

func NewSession(url string) *Session {
	session := &Session{
		url: url,
	}

	session.connect()
	go session.watchDisconnect()

	return session
}

func (s *Session) Channel(fn func(*amqp.Channel) error) error {
	ch, err := s.conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	return fn(ch)
}

func (s *Session) OnReconnect(fn func(*amqp.Connection)) *Session {
	s.fnReconnect = fn
	return s
}

func (s *Session) connect() {
	conn, err := amqp.Dial(s.url)

	if err != nil {
		log.Panic(err)
	}

	s.conn = conn

	s.chClose = make(chan *amqp.Error)
	conn.NotifyClose(s.chClose)
}

func (s *Session) watchDisconnect() {
	for {
		select {
		case <-s.chClose:
			s.connect()
			if s.fnReconnect != nil {
				s.fnReconnect(s.conn)
			}
		case <-time.After(10 * time.Second):
		}
	}
}
