package servicebus

import (
	"bytes"
	"errors"
)

var (
	ErrCannotConnectToServer = errors.New("Cannot connect to server")
)

// amqpSender is Sender interface implements for one RabbitMQ connection
type amqpSender struct {
	driver *AMQPDriver
}

func (s *amqpSender) Ping(target string, timeout int) bool {
	queue, _, err := createEventMessage(target, "", []byte{})
	if err != nil {
		return false
	}
	ret, err := s.driver.Call(queue, []byte("PING"), timeout)
	if err != nil {
		return false
	}
	return bytes.Equal(ret, []byte("PONG"))
}

func (s *amqpSender) Send(target string, message []byte) error {
	token := s.driver.config.generateToken("now")
	queue, msg, err := createEventMessage(target, token, message)
	if err != nil {
		return err
	}
	return s.driver.Send(queue, msg.toXML())
}

func (s *amqpSender) Call(target string, message []byte, timeout int) ([]byte, error) {
	token := s.driver.config.generateToken("now")
	queue, msg, err := createEventMessage(target, token, message)
	if err != nil {
		return nil, err
	}
	ret, err := s.driver.Call(queue, msg.toXML(), timeout)
	if err != nil {
		return nil, err
	}
	resp, err := decodeEventResponse(ret)
	if err != nil {
		return nil, err
	}
	return resp.Message, nil
}

func (s *amqpSender) Close() error {
	return s.driver.Close()
}

// smartSender is Sender interface implements
// It can choose a available path to send message to Server
type smartSender struct {
	config  *Config
	senders []*amqpSender
}

// NewSender create smart sender
func NewSender(config *Config) Sender {
	return &smartSender{
		config: config,
	}
}

func (s *smartSender) selectSender(target string, doPing bool) Sender {
	if len(s.senders) == 0 {
		s.initializeSenders()
	}
	if doPing {
		for _, sender := range s.senders {
			if sender.Ping(target, 3) {
				return sender
			}
		}
		return nil
	} else {
		if len(s.senders) > 0 {
			return s.senders[0]
		}
		return nil
	}
}

func (s *smartSender) Ping(target string, timeout int) bool {
	sender := s.selectSender(target, false)
	if sender == nil {
		return false
	}
	return sender.Ping(target, timeout)
}

func (s *smartSender) Send(target string, message []byte) error {
	sender := s.selectSender(target, true)
	if sender == nil {
		return ErrCannotConnectToServer
	}
	return sender.Send(target, message)
}

func (s *smartSender) Call(target string, message []byte, timeout int) ([]byte, error) {
	sender := s.selectSender(target, true)
	if sender == nil {
		return nil, ErrCannotConnectToServer
	}
	return sender.Call(target, message, timeout)
}

func (s *smartSender) initializeSenders() {
	drivers := []*AMQPDriver{}
	for _, host := range s.config.Hosts {
		driver := newAMQPDriver(host, s.config)
		err := driver.Dial()
		if err == nil {
			drivers = append(drivers, driver)
		}
	}
	senders := make([]*amqpSender, len(drivers))
	for i, driver := range drivers {
		senders[i] = &amqpSender{driver}
	}
	s.senders = senders
}

func (s *smartSender) Close() error {
	var err error = nil
	for _, sender := range s.senders {
		if err == nil {
			err = sender.Close()
		} else {
			sender.Close()
		}
	}
	return err
}
