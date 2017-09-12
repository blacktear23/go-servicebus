package servicebus

import (
	"crypto/sha1"
	"fmt"
	"time"
)

// Config is configuration for create a client to RabbitMQ server.
type Config struct {
	Hosts        []string
	User         string
	Password     string
	UseSSL       bool
	ExchangeName string
	NodeName     string
	SecretToken  string
}

// CreateSender create smart sender instance
func (c *Config) CreateSender() Sender {
	return NewSender(c)
}

// CreateServer create service bus server instance
func (c *Config) CreateServer() *Server {
	return NewServer(c)
}

func (c *Config) generateToken(date string) string {
	var dstr string
	now := time.Now()
	dayDur := 24 * time.Hour
	switch date {
	case "now":
		year, month, day := now.Year(), int(now.Month()), now.Day()
		dstr = fmt.Sprintf("%4d-%02d-%02d", year, month, day)
	case "prev":
		prev := now.Add(-dayDur)
		year, month, day := prev.Year(), int(prev.Month()), prev.Day()
		dstr = fmt.Sprintf("%4d-%02d-%02d", year, month, day)
	case "next":
		next := now.Add(dayDur)
		year, month, day := next.Year(), int(next.Month()), next.Day()
		dstr = fmt.Sprintf("%4d-%02d-%02d", year, month, day)
	}
	tokenStr := fmt.Sprintf("%s - %s", c.SecretToken, dstr)
	return fmt.Sprintf("%x", sha1.Sum([]byte(tokenStr)))
}

func (c *Config) validateToken(token string) bool {
	dates := []string{"now", "prev", "next"}
	for _, date := range dates {
		gtoken := c.generateToken(date)
		if token == gtoken {
			return true
		}
	}
	return false
}
