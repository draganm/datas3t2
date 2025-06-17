package datas3t2

import "github.com/draganm/datas3t2/client"

func NewClient(baseURL string) *client.Client {
	return client.NewClient(baseURL)
}
