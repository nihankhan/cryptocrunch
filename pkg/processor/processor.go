package processor

import (
	"context"
	"time"
)

type PriceData struct {
	Currency string
	Price    float64
}

type ProcessedData struct {
	Currency  string
	AvgPrice  float64
	Timestamp time.Time
}

type priceAggregate struct {
	total float64
	count int
}

func ProcessData(
	ctx context.Context,
	priceCh <-chan PriceData,
	processedCh chan<- ProcessedData,
) {
	defer close(processedCh)

	aggregates := make(map[string]priceAggregate)

	for {
		select {

		case <-ctx.Done():
			return

		case price, ok := <-priceCh:
			if !ok {
				return
			}

			agg := aggregates[price.Currency]
			agg.total += price.Price
			agg.count++
			aggregates[price.Currency] = agg

			select {
			case <-ctx.Done():
				return
			case processedCh <- ProcessedData{
				Currency:  price.Currency,
				AvgPrice:  agg.total / float64(agg.count),
				Timestamp: time.Now().UTC(),
			}:
			}
		}
	}
}
