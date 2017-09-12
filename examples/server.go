package main

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/blacktear23/go-servicebus/servicebus"
)

type Calculator struct{}

func (*Calculator) OnCall(req servicebus.Request, resp servicebus.Response) {
	fmt.Println("Receive Message:", req.GetMessage())
	var ints = []int{}
	err := json.Unmarshal(req.GetMessage(), &ints)
	if err != nil {
		resp.SendString(fmt.Sprintf("%v", err))
		return
	}
	ret := 0
	for _, v := range ints {
		ret += v
	}
	fmt.Println("Send Result:", ret)
	resp.SendString(fmt.Sprintf("%d", ret))
}

func (*Calculator) OnMessage(req servicebus.Request) {
	fmt.Println(string(req.GetMessage()))
}

func main() {
	config := &servicebus.Config{
		Hosts:        []string{"172.16.10.210"},
		User:         "admin",
		Password:     "123456",
		UseSSL:       false,
		ExchangeName: "test-service1",
		NodeName:     "Add-Service",
		SecretToken:  "Secret-Token",
	}
	server := config.CreateServer()
	server.RegisterService("math", "add", &Calculator{})
	server.Start()
	for {
		time.Sleep(1 * time.Minute)
	}
}
