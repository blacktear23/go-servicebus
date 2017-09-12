# go-servicebus

go-servicebus is py-servicebus’s Golang version. You can use golang to write server side function and use py-servicebus to call those functions.

## Install

```
# go get github.com/blacktear23/go-servicebus
```

## Server Side

```go

import "github.com/blacktear23/go-servicebus/servicebus"

type SomeService struct {}

func (s *SomeService) OnMessage(req servicebus.Request) {
    // do process request
}

func (s *SomeService) OnCall(req servicebus.Request, resp servicebus.Response) {
    // do process request
    resp.SendString("Response")
}

func main() {
    config := &servicebus.Config{
        Hosts:        []string{"172.16.10.210"},
        User:         "admin",
        Password:     "123456",
        UseSSL:       false,
        ExchangeName: "test",
        NodeName:     "Node1",
        SecretToken:  "Secret-Token",
    }
    server := config.CreateServer()
    server.RegisterService("util", "function", &SomeService{})
    server.Start()
    // ...
}
```

## Client Side


```go
import "github.com/blacktear23/go-servicebus/servicebus"

func main() {
    config := &servicebus.Config{
        Hosts:        []string{"172.16.10.210"},
        User:         "admin",
        Password:     "123456",
        UseSSL:       false,
        ExchangeName: "test",
        NodeName:     "Node1",
        SecretToken:  "Secret-Token",
    }
    sender := config.CreateSender()
    params := []byte{"Some Parameter"}
    resp, err := sender.Call("Node1.util.function", params, 30)
    if err != nil {
        fmt.Println(err)
    }
    fmt.Println(resp)
}

```
