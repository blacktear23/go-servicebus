package servicebus

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/streadway/amqp"
)

var (
	ErrServiceNotFound = errors.New("Service not found")
	ErrInvalidToken    = errors.New("Invalid token")
)

// Server is a server to receive messages and execute services.
type Server struct {
	config    *Config
	workers   map[string]*worker
	receivers []*receiver
}

// NewServer create a Server instance
func NewServer(config *Config) *Server {
	return &Server{
		config:    config,
		workers:   make(map[string]*worker),
		receivers: []*receiver{},
	}
}

// Start start Server
func (s *Server) Start() {
	for _, worker := range s.workers {
		worker.Start()
	}
	for _, host := range s.config.Hosts {
		driver := newAMQPDriver(host, s.config)
		recv := &receiver{
			driver: driver,
			server: s,
		}
		recv.Start()
		s.receivers = append(s.receivers, recv)
	}
}

// RegisterService register service bus's Service
// config.NodeName, module, service three parameter compose a final target: `NodeName.module.service`
func (s *Server) RegisterService(module, service string, instance Service) {
	key := fmt.Sprintf("%s.%s", module, service)
	worker := newWorker(key, instance)
	s.workers[key] = worker
}

// selectWorker select correct service instance to process message
func (s *Server) selectWorker(event *EventMessage) (*worker, error) {
	key := fmt.Sprintf("%s.%s", event.Category, event.Service)
	worker, have := s.workers[key]
	if !have {
		return nil, ErrServiceNotFound
	}
	return worker, nil
}

// receiver connect to one RabbitMQ server and receive messages then
// call related Service to process received message
type receiver struct {
	driver  *AMQPDriver
	server  *Server
	running bool
	status  string
}

func (r *receiver) receiveMessages() error {
	queue, err := r.driver.Consume()
	if err != nil {
		return err
	}
	for msg := range queue {
		msg.Ack(false)
		if msg.ReplyTo != "" {
			if bytes.Equal(msg.Body, []byte("PING")) {
				err := r.onPing(msg)
				if err != nil {
					return err
				}
			} else {
				err := r.onCall(msg)
				if err != nil {
					if err == ErrServiceNotFound || err == ErrInvalidEvent || err == ErrInvalidToken {
						log.Println(err)
					} else {
						return err
					}
				}
			}
		} else {
			err := r.onMessage(msg)
			if err != nil {
				if err == ErrServiceNotFound || err == ErrInvalidEvent || err == ErrInvalidToken {
					log.Println(err)
				} else {
					return err
				}
			}
		}
	}
	return nil
}

func (r *receiver) onCall(msg amqp.Delivery) error {
	event, err := decodeEventMessage(msg.Body)
	if err != nil {
		return err
	}
	if !r.server.config.validateToken(event.Token) {
		return ErrInvalidToken
	}
	worker, err := r.server.selectWorker(event)
	if err != nil {
		return err
	}
	worker.PushJob(&job{
		Type:    RPCType,
		Driver:  r.driver,
		Message: msg,
		Event:   event,
	})
	return nil
}

func (r *receiver) onMessage(msg amqp.Delivery) error {
	event, err := decodeEventMessage(msg.Body)
	if err != nil {
		return err
	}
	if !r.server.config.validateToken(event.Token) {
		return ErrInvalidToken
	}
	worker, err := r.server.selectWorker(event)
	if err != nil {
		return err
	}
	worker.PushJob(&job{
		Type:    MessageType,
		Driver:  r.driver,
		Message: msg,
		Event:   event,
	})
	return nil
}

func (r *receiver) onPing(msg amqp.Delivery) error {
	return r.driver.channel.Publish(
		"",
		msg.ReplyTo,
		false,
		false,
		amqp.Publishing{
			ContentType:   "text/plain",
			CorrelationId: msg.CorrelationId,
			Body:          []byte("PONG"),
		},
	)
}

// Run execute receiver's main process
func (r *receiver) Run() {
	for r.running {
		log.Printf("[%s] Connecting to Server...", r.driver.host)
		err := r.driver.Dial()
		if err == nil {
			err := r.driver.BindQueueToExchange()
			if err == nil {
				log.Printf("[%s] Start Receive Messages", r.driver.host)
				err := r.receiveMessages()
				if err != nil {
					r.driver.Close()
					log.Println(err)
				}
			} else {
				r.driver.Close()
				log.Println(err)
			}
		} else {
			log.Println(err)
		}
		log.Printf("[%s] Connection Error, Wait 5 Seconds to Retry", r.driver.host)
		time.Sleep(5 * time.Second)
	}
	r.status = "Stopped"
}

// Start start receiver
func (r *receiver) Start() {
	if !r.running && r.status != "Running" {
		r.running = true
		r.status = "Running"
		go r.Run()
	}
}

// Stop stop receiver
func (r *receiver) Stop() {
	r.running = false
}

// Status report receiver's status
func (r *receiver) Status() string {
	return r.status
}
