package servicebus

import (
	"crypto/md5"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/streadway/amqp"
)

var (
	ErrTimeout = errors.New("Timeout")
)

// AMQPDriver is a basic AMQP client, provide basic operation to RabbitMQ
type AMQPDriver struct {
	host    string
	config  *Config
	queue   amqp.Queue
	conn    *amqp.Connection
	channel *amqp.Channel
}

func newAMQPDriver(host string, config *Config) *AMQPDriver {
	return &AMQPDriver{
		host:   host,
		config: config,
	}
}

// Dial connect to RabbitMQ server and then create a Channel via this connection
func (d *AMQPDriver) Dial() error {
	port := 5672
	schema := "amqp"
	if d.config.UseSSL {
		port = 5671
		schema = "amqps"
	}
	uri := fmt.Sprintf("%s://%s:%s@%s:%d", schema, d.config.User, d.config.Password, d.host, port)
	conn, err := amqp.Dial(uri)
	if err != nil {
		return err
	}
	channel, err := conn.Channel()
	if err != nil {
		d.conn.Close()
		return err
	}
	d.conn = conn
	d.channel = channel
	return nil
}

func (d *AMQPDriver) reopenChannel() error {
	if d.channel != nil {
		d.channel.Close()
		d.channel = nil
	}
	channel, err := d.conn.Channel()
	if err != nil {
		return err
	}
	d.channel = channel
	return nil
}

// Close close this connection
func (d *AMQPDriver) Close() error {
	if d.channel != nil {
		d.channel.Close()
	}
	if d.conn != nil {
		return d.conn.Close()
	}
	return nil
}

// DeclareQueue declare a queue to RabbitMQ server
func (d *AMQPDriver) DeclareQueue(name string, rpc bool) (amqp.Queue, error) {
	if rpc {
		return d.channel.QueueDeclare(
			name,  // name
			false, // durable
			false, // delete when unused
			true,  // exclusive
			false, // no-wait
			nil,   // arguments
		)
	}
	return d.channel.QueueDeclare(
		name,  // name
		true,  // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
}

// BindQueueToExchange create exchange and then bind queue to exchange
func (d *AMQPDriver) BindQueueToExchange() error {
	// If default exchange we do not need to declare it
	if d.config.ExchangeName != "" {
		// We don't need to care about error.
		// If exchange exists code below will run no error
		// If exchange not created code below will return error
		err := d.channel.ExchangeDeclarePassive(
			d.config.ExchangeName, // name
			"direct",              // type
			false,                 // durable
			false,                 // auto-deleted
			false,                 // internal
			false,                 // no-wait
			nil,                   // arguments
		)
		if err != nil {
			log.Println("Declare Exchange Error:", err)
			if err := d.reopenChannel(); err != nil {
				return err
			}
		}
	}
	queue, err := d.DeclareQueue(d.config.NodeName, false)
	if err != nil {
		return err
	}
	d.queue = queue
	// If default exchange we do not need to bind it
	if d.config.ExchangeName != "" {
		err = d.channel.QueueBind(
			queue.Name,            // name
			queue.Name,            // routing-key
			d.config.ExchangeName, // exchange
			false, // no-wait
			nil,   // arguments
		)
		return err
	}
	return nil
}

// Consume return AMQP message channel
func (d *AMQPDriver) Consume() (<-chan amqp.Delivery, error) {
	return d.channel.Consume(
		d.queue.Name, // queue
		"",           // consumer
		false,        // auto ack
		false,        // exclusive
		false,        // no local
		false,        // no wait
		nil,          // args
	)
}

// Send send message to queue
func (d *AMQPDriver) Send(queue string, msg []byte) error {
	return d.channel.Publish(
		d.config.ExchangeName, // exchange
		queue, // routing key
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        msg,
		},
	)
}

// Call do RPC request to queue
func (d *AMQPDriver) Call(queue string, msg []byte, timeout int) ([]byte, error) {
	retQ, err := d.DeclareQueue("", true)
	if err != nil {
		return nil, err
	}
	defer d.channel.QueueDelete(retQ.Name, false, false, false)

	corrId := randString()
	err = d.channel.Publish(
		d.config.ExchangeName, // exchange
		queue, // routing key
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			ContentType:   "text/plain",
			CorrelationId: corrId,
			ReplyTo:       retQ.Name,
			Body:          msg,
		},
	)
	if err != nil {
		return nil, err
	}
	msgs, err := d.channel.Consume(
		retQ.Name, // queue
		"",        // consumer
		true,      // auto ack
		false,     // exclusive
		false,     // no-local
		false,     // no-wait
		nil,       // args
	)
	if err != nil {
		return nil, err
	}
	timer := time.After(time.Duration(timeout) * time.Second)
	for {
		select {
		case <-timer:
			return nil, ErrTimeout
		case msg := <-msgs:
			if msg.CorrelationId == corrId {
				return msg.Body, nil
			}
		}
	}
}

func randString() string {
	t := time.Now().String()
	return fmt.Sprintf("%x", md5.Sum([]byte(t)))
}
