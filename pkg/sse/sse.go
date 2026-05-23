package sse

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/nihankhan/CryptoCrunch/pkg/processor"
)

type SSE struct {
	add    chan chan processor.ProcessedData
	remove chan chan processor.ProcessedData
	fanOut chan processor.ProcessedData
}

func NewSSE() *SSE {
	return &SSE{
		add:    make(chan chan processor.ProcessedData),
		remove: make(chan chan processor.ProcessedData),
		fanOut: make(chan processor.ProcessedData),
	}
}

func (s *SSE) Run(ctx context.Context) {
	clients := make(map[chan processor.ProcessedData]struct{})
	for {
		select {
		case <-ctx.Done():
			for c := range clients {
				delete(clients, c)
				close(c)
			}
		case c := <-s.add:
			clients[c] = struct{}{}
		case c := <-s.remove:
			if _, ok := clients[c]; ok {
				delete(clients, c)
				close(c)
			}
		case data := <-s.fanOut:
			for c := range clients {
				select {
				case c <- data:
				default:
					if _, ok := clients[c]; ok {
						delete(clients, c)
						close(c)
					}
				}
			}
		}
	}
}

func (s *SSE) Broadcast(ctx context.Context, data processor.ProcessedData) {
	select {
	case <-ctx.Done():
		return
	case s.fanOut <- data:
	}
}

func (s *SSE) Stream(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	client := make(chan processor.ProcessedData, 10)
	s.add <- client
	defer func() { s.remove <- client }()

	for {
		select {
		case <-r.Context().Done():
			return
		case data, ok := <-client:
			if !ok {
				return
			}
			b, err := json.Marshal(data)
			if err != nil {
				continue
			}
			fmt.Fprintf(w, "data: %s\n\n", b)
			flusher.Flush()
		}
	}
}
