package client

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"

	"github.com/draganm/datas3t2/server/datas3t"
)

func (c *Client) ListDatas3ts(ctx context.Context) ([]datas3t.Datas3tInfo, error) {
	ur, err := url.JoinPath(c.baseURL, "api", "v1", "datas3ts")
	if err != nil {
		return nil, fmt.Errorf("failed to join path: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "GET", ur, nil)
	if err != nil {
		return nil, err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to list datas3ts: %w", err)
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to list datas3ts: %s", resp.Status)
	}

	var datas3ts []datas3t.Datas3tInfo
	err = json.NewDecoder(resp.Body).Decode(&datas3ts)
	if err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return datas3ts, nil
}
