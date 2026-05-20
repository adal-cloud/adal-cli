package config

type ConnectionData struct {
	AccessToken   string `json:"access_token"`
	ConnectionURL string `json:"connection_url"`
	EndpointName  string `json:"endpoint_name"`
	Country       string `json:"country"`
	City          string `json:"city"`
}
