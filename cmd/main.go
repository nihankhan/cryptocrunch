package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/nihankhan/CryptoCrunch/pkg/processor"
	"github.com/nihankhan/CryptoCrunch/pkg/sse"
	"github.com/nihankhan/CryptoCrunch/pkg/tracker"
)

func main() {
	trackerPriceCh := make(chan tracker.PriceData)
	processorPriceCh := make(chan processor.PriceData)
	processedDataCh := make(chan processor.ProcessedData)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sse := sse.NewSSE()
	go sse.Run(ctx)

	go tracker.TrackPrices(ctx, trackerPriceCh)
	go processor.ProcessData(ctx, processorPriceCh, processedDataCh)

	go func() {
		for price := range trackerPriceCh {
			select {
			case processorPriceCh <- processor.PriceData{
				Currency: price.Currency,
				Price:    price.Price,
			}:
			case <-ctx.Done():
				return
			}
		}
		close(processorPriceCh)
	}()

	go func() {
		for data := range processedDataCh {
			sse.Broadcast(ctx, data)
		}
	}()

	http.HandleFunc("/stream", func(w http.ResponseWriter, r *http.Request) {
		sse.Stream(w, r)
	})

	http.Handle("/", http.FileServer(http.Dir("./web")))

	server := &http.Server{
		Addr:    "127.0.0.1:8080",
		Handler: nil,
	}

	fmt.Println("CryptoCrunch server is Running on 127.0.0.1:8080")

	go func() {
		err := server.ListenAndServe()
		if err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGABRT, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan
	cancel()

	server.Shutdown(ctx)
}
