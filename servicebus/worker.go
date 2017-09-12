package servicebus

import (
	"log"

	"github.com/streadway/amqp"
)

// job store message and more information for worker to execute Service
type job struct {
	// Type is Message type
	Type int
	// Driver is AMQPDriver for send response
	Driver *AMQPDriver
	// Message is AMQP message
	Message amqp.Delivery
	// Event is decoded AMQP message
	Event *EventMessage
}

// worker is Service runner for service bus
type worker struct {
	name    string
	service Service
	queue   chan *job
}

// newWorker create new worker to execute service
func newWorker(name string, srv Service) *worker {
	return &worker{
		name:    name,
		service: srv,
		queue:   make(chan *job, 2),
	}
}

func (w *worker) processMessage(jobj *job) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Service %s (recover): %v", w.name, r)
		}
	}()
	req := &amqpRequest{
		driver: jobj.Driver,
		msg:    jobj.Event.Params,
	}
	switch jobj.Type {
	case MessageType:
		log.Println("Process Message Service:", w.name)
		w.service.OnMessage(req)
		log.Println("Process Message Service:", w.name, "Done")
	case RPCType:
		resp := &amqpResponse{
			driver:   jobj.Driver,
			delivery: jobj.Message,
			event:    jobj.Event,
			sended:   false,
		}
		log.Println("Process RPC Service:", w.name)
		w.service.OnCall(req, resp)
		log.Println("Process RPC Service:", w.name, "Done")
	}
}

// Run execute worker's Service related methods
func (w *worker) Run() {
	for jobj := range w.queue {
		w.processMessage(jobj)
	}
}

// Start start worker
func (w *worker) Start() {
	go w.Run()
}

// PushJob push a job to worker's queue
func (w *worker) PushJob(jobj *job) {
	w.queue <- jobj
}
