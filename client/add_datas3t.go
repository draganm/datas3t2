package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"

	"github.com/draganm/datas3t2/server/datas3t"
)

func (c *Client) AddDatas3t(ctx context.Context, datas3t *datas3t.AddDatas3tRequest) error {
	ur, err := url.JoinPath(c.baseURL, "api", "v1", "datas3ts")
	if err != nil {
		return fmt.Errorf("failed to join path: %w", err)
	}

	body, err := json.Marshal(datas3t)
	if err != nil {
		return fmt.Errorf("failed to marshal dataset info: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", ur, bytes.NewReader(body))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to add datas3t: %w", err)
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("failed to add datas3t: %s", resp.Status)
	}

	return nil
}
