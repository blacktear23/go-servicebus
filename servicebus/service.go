package servicebus

import (
	"errors"

	"github.com/streadway/amqp"
)

const (
	MessageType int = 1
	RPCType     int = 2
)

var (
	ErrAlreadySend = errors.New("Already send response")
)

// Sender is message sender
type Sender interface {
	// Ping a target
	Ping(target string, timeout int) bool
	// Call make RPC request to target, timeout unit is second
	Call(target string, message []byte, timeout int) ([]byte, error)
	// Send just send message to target and not wait response
	Send(target string, message []byte) error
	// Close close Sender connection
	Close() error
}

// Request is request from Sender
type Request interface {
	// GetMessage get message sender sent
	GetMessage() []byte
	// GetSender return a smart sender for user to send message to other target
	GetSender() Sender
}

// Response works for Service to send RPC response
type Response interface {
	// Send send []byte message to RPC caller
	Send(message []byte) error
	// SendString send string message to RPC caller
	SendString(message string) error
}

// Service is a service interface
type Service interface {
	// IsBackground if return true it will run service in new goroutine
	IsBackground() bool
	// OnMessage when Message received this method will be called
	OnMessage(req Request)
	// OnCall when RPC received this method will be called
	OnCall(req Request, resp Response)
}

// SimpleService is simple implements for Service interface
type SimpleService struct {
	Background bool
}

func (s *SimpleService) IsBackground() bool {
	return s.Background
}

func (s *SimpleService) OnMessage(req Request) {
	// Do nothing
}

func (s *SimpleService) OnCall(req Request, resp Response) {
	// Do nothing, just send "0" response
	resp.SendString("0")
}

// amqpRequest is Request interface implement
type amqpRequest struct {
	driver *AMQPDriver
	msg    []byte
}

func (r *amqpRequest) GetMessage() []byte {
	return r.msg
}

func (r *amqpRequest) GetSender() Sender {
	return NewSender(r.driver.config)
}

// amqpResponse is Response interface implement
type amqpResponse struct {
	driver   *AMQPDriver
	delivery amqp.Delivery
	event    *EventMessage
	sended   bool
}

func (r *amqpResponse) SendString(msg string) error {
	return r.Send([]byte(msg))
}

func (r *amqpResponse) Send(msg []byte) error {
	if r.sended {
		return ErrAlreadySend
	}
	replyMsg := createEventResponse(r.event, msg)
	err := r.driver.channel.Publish(
		"",                 // exchange
		r.delivery.ReplyTo, // routing key
		false,              // mandatory
		false,              // immediate
		amqp.Publishing{
			ContentType:   "text/plain",
			CorrelationId: r.delivery.CorrelationId,
			Body:          replyMsg.toXML(),
		},
	)
	if err == nil {
		r.sended = true
	}
	return err
}
