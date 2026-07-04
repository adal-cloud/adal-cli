package ws

type RequestStatus string
type RequestDeliveryStatus string

const (
	RequestDeliveryStatusSuccess RequestDeliveryStatus = "success"
	RequestDeliveryStatusFailed  RequestDeliveryStatus = "failed"
)

type Request struct {
	Id             int64               `json:"id"`
	Method         string              `json:"method"`
	URI            string              `json:"uri"`
	Headers        map[string][]string `json:"headers"`
	Body           []byte              `json:"body"`
	IdempotencyKey []byte              `json:"idempotency_key"`
	Deliveries     []Delivery          `json:"deliveries"`
}

type Destination struct {
	Id                int64 `json:"id"`
	AddIdempotencyKey bool  `json:"add_idempotency_key"`
}

type Delivery struct {
	Id            int64       `json:"id"`
	Destination   Destination `json:"destination"`
	AttemptNumber int         `json:"attempt_number"`
	URL           string      `json:"url"`
}

type DeliveryResult struct {
	DeliveryId       int64                 `json:"delivery_id"`
	DestinationId    int64                 `json:"destination_id"`
	URL              string                `json:"url"`
	AttemptNumber    int                   `json:"attempt_number"`
	Status           RequestDeliveryStatus `json:"status"`
	ResponseCode     *int                  `json:"response_code"`
	Message          *string               `json:"message"`
	DeliveryDuration int64                 `json:"delivery_duration"`
}
