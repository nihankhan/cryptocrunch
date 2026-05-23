package tracker

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type PriceData struct {
	Currency string  `json:"currency"`
	Price    float64 `json:"price"`
}

func TrackPrices(ctx context.Context, priceCh chan<- PriceData) {
	currencies := []string{
		"bitcoin",
		"ethereum",
		"ripple",
		"litecoin",
		"bitcoin-cash",
	}
	ids := strings.Join(currencies, ",")
	url := fmt.Sprintf(
		"https://api.coingecko.com/api/v3/simple/price?ids=%s&vs_currencies=usd",
		ids,
	)

	client := http.Client{
		Timeout: 10 * time.Second,
	}

	ticker := time.NewTicker(7 * time.Second)
	defer ticker.Stop()

	var result map[string]map[string]interface{}

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			resp, err := client.Get(url)
			if err != nil {
				fmt.Println("fetch error:", err)
				continue
			}

			err = json.NewDecoder(resp.Body).Decode(&result)
			resp.Body.Close()
			if err != nil {
				fmt.Println("decode error:", err)
				continue
			}

			for _, currency := range currencies {
				data, ok := result[currency]
				if !ok {
					continue
				}
				rawPrice, ok := data["usd"]
				if !ok {
					continue
				}

				var price float64
				switch v := rawPrice.(type) {
				case float64:
					price = v
				case string:
					price, _ = strconv.ParseFloat(v, 64)
				default:
					continue
				}

				select {
				case <-ctx.Done():
					return
				case priceCh <- PriceData{
					Currency: currency,
					Price:    price,
				}:
				}
			}
		}
	}
}
