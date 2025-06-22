package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"

	"github.com/draganm/datas3t2/server/uploaddatarange"
)

func (c *Client) CancelDatarangeUpload(ctx context.Context, r *uploaddatarange.CancelUploadRequest) error {
	body, err := json.Marshal(r)
	if err != nil {
		return err
	}

	u, err := url.JoinPath(c.baseURL, "api", "v1", "upload-datarange", "cancel")
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, "POST", u, bytes.NewBuffer(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to cancel datarange upload: %s", resp.Status)
	}

	return nil
}
