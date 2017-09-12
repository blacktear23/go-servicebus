package servicebus

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/beevik/etree"
)

var (
	ErrInvalidTarget          = errors.New("Invalid target format")
	ErrInvalidEvent           = errors.New("Invalid event message")
	ErrInvalidResponse        = errors.New("Invalid response message")
	globalID           uint32 = 0
)

// EventMessage is request message for service bus
type EventMessage struct {
	ID       int
	Token    string
	Category string
	Service  string
	Params   []byte
}

// toXML marshal EventMessage to XML format
func (m *EventMessage) toXML() []byte {
	xmlTpl := "<?xml version=\"1.0\"?>\n"
	xmlTpl += "<event version=\"1\">\n"
	xmlTpl += "  <id>%d</id>\n"
	xmlTpl += "  <token>%s</token>\n"
	xmlTpl += "  <catgory>%s</catgory>\n"
	xmlTpl += "  <service>%s</service>\n"
	xmlTpl += "  <params><![CDATA[%s]]></params>\n"
	xmlTpl += "</event>\n"

	return []byte(fmt.Sprintf(
		xmlTpl,
		m.ID,
		m.Token,
		m.Category,
		m.Service,
		string(m.Params),
	))
}

// EventResponse is response message for service bus
type EventResponse struct {
	ID      int
	Message []byte
}

// toXML marshal EventResponse to XML format
func (m *EventResponse) toXML() []byte {
	xmlTpl := "<?xml version=\"1.0\"?>\n"
	xmlTpl += "<response>\n"
	xmlTpl += "  <id>%d</id>\n"
	xmlTpl += "  <message><![CDATA[%s]]></message>\n"
	xmlTpl += "</response>\n"
	return []byte(fmt.Sprintf(
		xmlTpl,
		m.ID,
		string(m.Message),
	))
}

func createEventMessage(target string, token string, msg []byte) (string, *EventMessage, error) {
	parts := strings.Split(target, ".")
	if len(parts) != 3 {
		return "", nil, ErrInvalidTarget
	}
	id := atomic.AddUint32(&globalID, 1)
	queue := parts[0]
	ret := &EventMessage{
		ID:       int(id),
		Token:    token,
		Category: parts[1],
		Service:  parts[2],
		Params:   msg,
	}
	return queue, ret, nil
}

func createEventResponse(event *EventMessage, msg []byte) *EventResponse {
	return &EventResponse{
		ID:      event.ID,
		Message: msg,
	}
}

func decodeEventMessage(data []byte) (*EventMessage, error) {
	doc := etree.NewDocument()
	if err := doc.ReadFromBytes(data); err != nil {
		return nil, ErrInvalidEvent
	}
	root := doc.SelectElement("event")
	if root == nil {
		return nil, ErrInvalidEvent
	}

	xversion := root.SelectAttr("version")
	if xversion == nil {
		return nil, ErrInvalidEvent
	}
	if xversion.Value != "1" {
		return nil, ErrInvalidEvent
	}

	xid := root.SelectElement("id")
	if xid == nil {
		return nil, ErrInvalidEvent
	}
	id, err := strconv.Atoi(xid.Text())
	if err != nil {
		return nil, ErrInvalidEvent
	}

	xtoken := root.SelectElement("token")
	if xtoken == nil {
		return nil, ErrInvalidEvent
	}
	token := xtoken.Text()

	xcategory := root.SelectElement("catgory")
	if xcategory == nil {
		return nil, ErrInvalidEvent
	}
	category := xcategory.Text()

	xservice := root.SelectElement("service")
	if xservice == nil {
		return nil, ErrInvalidEvent
	}
	service := xservice.Text()

	xparams := root.SelectElement("params")
	if xparams == nil {
		return nil, ErrInvalidEvent
	}
	params := xparams.Text()

	ret := &EventMessage{
		ID:       id,
		Token:    token,
		Category: category,
		Service:  service,
		Params:   []byte(params),
	}
	return ret, nil
}

func decodeEventResponse(data []byte) (*EventResponse, error) {
	doc := etree.NewDocument()
	if err := doc.ReadFromBytes(data); err != nil {
		return nil, ErrInvalidResponse
	}
	root := doc.SelectElement("response")
	if root == nil {
		return nil, ErrInvalidResponse
	}

	xid := root.SelectElement("id")
	if xid == nil {
		return nil, ErrInvalidResponse
	}
	id, err := strconv.Atoi(xid.Text())
	if err != nil {
		return nil, ErrInvalidResponse
	}

	xmessage := root.SelectElement("message")
	if xmessage == nil {
		return nil, ErrInvalidResponse
	}
	message := xmessage.Text()

	ret := &EventResponse{
		ID:      id,
		Message: []byte(message),
	}
	return ret, nil
}
