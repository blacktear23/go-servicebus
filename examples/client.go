package main

import (
	"encoding/json"
	"fmt"

	"github.com/blacktear23/go-servicebus/servicebus"
)

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
	sender := config.CreateSender()
	sender.Send("Add-Service.math.add", []byte("Something to print"))
	ints := []int{10, 20}
	data, _ := json.Marshal(&ints)
	resp, err := sender.Call("Add-Service.math.add", data, 30)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(string(resp))
}
