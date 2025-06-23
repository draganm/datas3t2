package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"

	"github.com/draganm/datas3t2/server/bucket"
)

func (c *Client) AddBucket(ctx context.Context, bucket *bucket.BucketInfo) error {
	ur, err := url.JoinPath(c.baseURL, "api", "v1", "buckets")
	if err != nil {
		return fmt.Errorf("failed to join path: %w", err)
	}

	body, err := json.Marshal(bucket)
	if err != nil {
		return fmt.Errorf("failed to marshal bucket info: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", ur, bytes.NewReader(body))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to add bucket: %w", err)
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("failed to add bucket: %s", resp.Status)
	}

	return nil
}
