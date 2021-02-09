package alpaca

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"sync"
)

const (
	// EnvAPIKeyID ...
	EnvAPIKeyID = "AKE7AHA457UXTGHIKNTA"
	// EnvAPISecretKey ...
	EnvAPISecretKey = "i7N491Q8PW5swKct38hQy/AJCgzPxz8z1zC0fORZ"
)

var (
	once sync.Once
	key  *APIKey
)

// APIKey ...
type APIKey struct {
	ID     string
	Secret string
}

// Credentials returns the user's Alpaca API key ID
// and secret for use through the SDK.
func Credentials() *APIKey {
	return &APIKey{
		ID:     EnvAPIKeyID,
		Secret: EnvAPISecretKey,
	}
}

// Client is an Alpaca REST API client
type Client struct {
	credentials *APIKey
}

// Asset ...
type Asset struct {
	ID           string `json:"id"`
	Name         string `json:"name"`
	Exchange     string `json:"exchange"`
	Class        string `json:"asset_class"`
	Symbol       string `json:"symbol"`
	Status       string `json:"status"`
	Tradable     bool   `json:"tradable"`
	Marginable   bool   `json:"marginable"`
	Shortable    bool   `json:"shortable"`
	EasyToBorrow bool   `json:"easy_to_borrow"`
}

var (
	// DefaultClient is the default Alpaca client using the
	// environment variable set credentials
	DefaultClient = NewClient(Credentials())
	base          = "https://api.alpaca.markets"
	apiVersion    = "v2"
	do            = func(c *Client, req *http.Request) (*http.Response, error) {
		req.Header.Set("APCA-API-KEY-ID", c.credentials.ID)
		req.Header.Set("APCA-API-SECRET-KEY", c.credentials.Secret)

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, err
		}

		if err = verify(resp); err != nil {
			return nil, err
		}

		return resp, nil
	}
)

// NewClient creates a new Alpaca client with specified
// credentials
func NewClient(credentials *APIKey) *Client {
	return &Client{credentials: credentials}
}

// ListAssets returns the list of assets, filtered by
// the input parameters.
func (c *Client) ListAssets(status *string) ([]Asset, error) {
	// TODO: support different asset classes
	u, err := url.Parse(fmt.Sprintf("%s/%s/assets", base, apiVersion))
	if err != nil {
		return nil, err
	}

	q := u.Query()

	if status != nil {
		q.Set("status", *status)
	}

	u.RawQuery = q.Encode()

	resp, err := c.get(u)
	if err != nil {
		return nil, err
	}

	assets := []Asset{}

	if err = unmarshal(resp, &assets); err != nil {
		return nil, err
	}

	return assets, nil
}

func verify(resp *http.Response) (err error) {
	if resp.StatusCode >= http.StatusMultipleChoices {
		var body []byte
		defer resp.Body.Close()

		body, err = ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}

		apiErr := APIError{}

		err = json.Unmarshal(body, &apiErr)
		if err == nil {
			err = &apiErr
		}
	}

	return
}

func (c *Client) get(u *url.URL) (*http.Response, error) {
	req, err := http.NewRequest(http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}

	return do(c, req)
}

func unmarshal(resp *http.Response, data interface{}) error {
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	return json.Unmarshal(body, data)
}

// APIError wraps the detailed code and message supplied
// by Alpaca's API for debugging purposes
type APIError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (e *APIError) Error() string {
	return e.Message
}
