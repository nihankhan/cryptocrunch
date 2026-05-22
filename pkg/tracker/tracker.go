package tracker

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

type PriceData struct {
	Currency string  `json:"currency"`
	Price    float64 `json:"price"`
}

func TrackPrices(ctx context.Context, priceCh chan<- PriceData) {
	currencies := []string{"bitcoin", "ethereum", "ripple", "litecoin", "bitcoin-cash"}
	ids := strings.Join(currencies, ",")
	url := fmt.Sprintf("https://api.coingecko.com/api/v3/simple/price?ids=%s&vs_currencies=usd", ids)

	client := http.Client{
		Timeout: 5 * time.Second,
	}
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	var result map[string]PriceData

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			resp, err := client.Get(url)
			if err != nil {
				fmt.Printf("Error fetching price for %s: %v", ids, err)
				continue
			}

			err = json.NewDecoder(resp.Body).Decode(&result)
			if err != nil {
				fmt.Printf("Error decoding price data for %s: %v", ids, err)
				continue
			}
			resp.Body.Close()

			for _, currency := range currencies {
				data, ok := result[currency]
				if !ok {
					continue
				}

				priceCh <- PriceData{
					Currency: currency,
					Price:    data.Price,
				}
			}
		}
	}
}
