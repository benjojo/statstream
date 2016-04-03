package main

import (
	"flag"
	"golang.org/x/net/websocket"
	"gopkg.in/redis.v3"
	"io/ioutil"
	"net/http"
	"strings"
)

var (
	bindaddr    = flag.String("bind", "127.0.0.1:1189", "http bind")
	redisaddr   = flag.String("redis", "localhost:6379", "redis address")
	redistopic  = flag.String("redis-topic", "collectd", "the redis pubsub topic")
	PublishChan chan string
)

func main() {
	flag.Parse()

	PublishChan = make(chan string)
	go Publisher()

	http.HandleFunc("/poststat", addStat)
	http.Handle("/statstream", websocket.Handler(streamStats))
	err := http.ListenAndServe(*bindaddr, nil)
	if err != nil {
		panic("ListenAndServe: " + err.Error())
	}
}

func Publisher() {
	client := redis.NewClient(&redis.Options{
		Addr:     *redisaddr,
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	for msg := range PublishChan {
		if client.Publish(*redistopic, msg).Err() != nil {
			client.Close()
			client = redis.NewClient(&redis.Options{
				Addr:     *redisaddr,
				Password: "", // no password set
				DB:       0,  // use default DB
			})
		}
	}
}

func addStat(rw http.ResponseWriter, req *http.Request) {
	data, err := ioutil.ReadAll(req.Body)

	if err != nil {
		return
	}

	lines := strings.Split(string(data), "\n")

	for _, v := range lines {
		if v != "" {
			PublishChan <- v
		}
	}
}

func streamStats(ws *websocket.Conn) {
	defer ws.Close()

	grep := ws.Request().URL.Query().Get("grep")

	client := redis.NewClient(&redis.Options{
		Addr:     *redisaddr,
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	defer client.Close()

	psub, err := client.Subscribe(*redistopic)
	if err != nil {
		return
	}

	defer psub.Close()

	for {
		msg, err := psub.ReceiveMessage()
		if err != nil {
			break
		}

		if grep != "" && !strings.Contains(msg.Payload, grep) {
			continue
		}

		err = websocket.Message.Send(ws, msg.Payload+"\n")
		if err != nil {
			break
		}
	}
}
