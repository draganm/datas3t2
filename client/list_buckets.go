package client

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"

	"github.com/draganm/datas3t2/server/bucket"
)

func (c *Client) ListBuckets(ctx context.Context) ([]*bucket.BucketListInfo, error) {
	ur, err := url.JoinPath(c.baseURL, "api", "v1", "buckets")
	if err != nil {
		return nil, fmt.Errorf("failed to join path: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "GET", ur, nil)
	if err != nil {
		return nil, err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to list buckets: %w", err)
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to list buckets: %s", resp.Status)
	}

	var buckets []*bucket.BucketListInfo
	err = json.NewDecoder(resp.Body).Decode(&buckets)
	if err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return buckets, nil
}
