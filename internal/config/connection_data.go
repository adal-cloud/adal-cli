package config

type ConnectionData struct {
	AccessToken   string `json:"access_token"`
	ConnectionURL string `json:"connection_url"`
	ServerName    string `json:"server_name"`
	Country       string `json:"country"`
	City          string `json:"city"`
	ServerURL     string `json:"server_url"`
}
