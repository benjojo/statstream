package main

import (
	"flag"
	"github.com/tuxychandru/pubsub"
	"golang.org/x/net/websocket"
	"io/ioutil"
	"net/http"
	"strings"
)

var PubSub *pubsub.PubSub

func main() {
	PubSub = pubsub.New(20)
	bindaddr := flag.String("bind", "127.0.0.1:1189", "http bind")
	flag.Parse()

	http.HandleFunc("/poststat", addStat)
	http.Handle("/statstream", websocket.Handler(streamStats))
	err := http.ListenAndServe(*bindaddr, nil)
	if err != nil {
		panic("ListenAndServe: " + err.Error())
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
			PubSub.Pub(v, "data")
		}
	}
}

func streamStats(ws *websocket.Conn) {
	defer ws.Close()

	grep := ws.Request().URL.Query().Get("grep")

	// inbound := make(chan interface{})

	inbound := PubSub.Sub("data")

	// PubSub.AddSub(inbound, "data")
	defer PubSub.Unsub(inbound)

	for {
		in := <-inbound
		str := in.(string)

		if grep != "" && !strings.Contains(str, grep) {
			continue
		}

		err := websocket.Message.Send(ws, str+"\n")
		if err != nil {
			break
		}
	}
}
