package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/draganm/datas3t2/server/dataranges"
)

func (c *Client) StartDatarangeUpload(ctx context.Context, r *dataranges.UploadDatarangeRequest) (*dataranges.UploadDatarangeResponse, error) {
	ur, err := url.JoinPath(c.baseURL, "api", "v1", "upload-datarange")
	if err != nil {
		return nil, fmt.Errorf("failed to join path: %w", err)
	}

	body, err := json.Marshal(r)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request info: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", ur, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to start datarange upload: %w", err)
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("failed to add datas3t: %s: %s", resp.Status, string(body))
	}

	var respBody dataranges.UploadDatarangeResponse
	err = json.NewDecoder(resp.Body).Decode(&respBody)
	if err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &respBody, nil

}
