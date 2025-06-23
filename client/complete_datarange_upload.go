package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"

	"github.com/draganm/datas3t2/server/dataranges"
)

func (c *Client) CompleteDatarangeUpload(ctx context.Context, r *dataranges.CompleteUploadRequest) error {
	ur, err := url.JoinPath(c.baseURL, "api", "v1", "upload-datarange", "complete")
	if err != nil {
		return fmt.Errorf("failed to join path: %w", err)
	}

	body, err := json.Marshal(r)
	if err != nil {
		return fmt.Errorf("failed to marshal request info: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", ur, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to complete datarange upload: %w", err)
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to complete datarange upload: %s", resp.Status)
	}

	return nil
}
