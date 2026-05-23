package main

import (
	"context"
	"fmt"
	"log"
	"net/http"

	"github.com/gorilla/websocket"
	"github.com/nihankhan/CryptoCrunch/pkg/processor"
	"github.com/nihankhan/CryptoCrunch/pkg/tracker"
)

type HTTPServer struct {
	trackerPriceCh chan tracker.PriceData
}

func main() {
	trackerPriceCh := make(chan tracker.PriceData)
	processorPriceCh := make(chan processor.PriceData)
	processedDataCh := make(chan processor.ProcessedData)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go tracker.TrackPrices(ctx, trackerPriceCh)

	go func() {
		for price := range trackerPriceCh {
			convertedPrice := processor.PriceData{
				Currency: price.Currency,
				Price:    price.Price,
			}

			processorPriceCh <- convertedPrice
		}

		close(processorPriceCh)
	}()

	go processor.ProcessData(processorPriceCh, processedDataCh)

	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		serveWebSocket(w, r, processedDataCh)
	})

	http.Handle("/", http.FileServer(http.Dir("./web")))

	server := &http.Server{
		Addr:    "127.0.0.1:8080",
		Handler: nil,
	}

	fmt.Println("CryptoCrunch server is Running on 127.0.0.1:8080")

	done := make(chan struct{})

	go func() {
		err := server.ListenAndServe()
		if err != nil {
			log.Fatal(err)
		}

		done <- struct{}{}
	}()

	<-done
}

func serveWebSocket(w http.ResponseWriter, r *http.Request, processedDataCh <-chan processor.ProcessedData) {
	upgrader := websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
	}

	conn, err := upgrader.Upgrade(w, r, nil)

	if err != nil {
		log.Println("WebSocket upgrade failed!")

		return
	}

	defer conn.Close()

	for processedData := range processedDataCh {

		log.Println(processedData)

		err := conn.WriteJSON(processedData)

		if err != nil {
			log.Println("Error sending message over WebSocket!")
			return
		}
	}
}
