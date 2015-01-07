/*

Client for interacting with ts4 API.

*/

package ts4

import (
	"bytes"
	"crypto/sha1"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"mime/multipart"
	"net/http"
)

// ErrBlobNotFound is returned from a get/stat request
// if the blob does not exist.
var ErrBlobNotFound = errors.New("blob not found")

var defaultServerAddr = "http://localhost:8010"

type BlobStore struct {
	ServerAddr string
	client     *http.Client
}

func New(serverAddr string) *BlobStore {
	if serverAddr == "" {
		serverAddr = defaultServerAddr
	}
	return &BlobStore{
		ServerAddr: serverAddr,
		client:     &http.Client{},
	}
}

type BlobInfo struct {
	Hash string `json:"hash"`
	Time string `json:"time"`
}

type QueryResp struct {
	Blobs []*BlobInfo `json:"blobs"`
	Start string      `json:"start"`
	End   string      `json:"end"`
}

// Iter sends all blobs from start to end over the blobs channel,
// start default to 0 and end to time.Now().UTC() if left empty.
func (bs *BlobStore) Iter(start, end string, blobs chan<- []byte) error {
	for {
		res, err := bs.Query(start, end)
		if err != nil {
			return err
		}
		if len(res.Blobs) == 0 {
			break
		}
		for _, blobinfo := range res.Blobs {
			start = blobinfo.Time
			blob, err := bs.Get(blobinfo.Hash)
			if err != nil {
				return err
			}
			blobs <- blob
		}
	}
	return nil
}

// Query returns a QueryResp containing blobs hash and time.
// start default to 0 and end to time.Now().UTC() if left empty.
func (bs *BlobStore) Query(start, end string) (*QueryResp, error) {
	request, err := http.NewRequest("GET", bs.ServerAddr+"/blobs?start="+start+"&end="+end, nil)
	if err != nil {
		return nil, err
	}
	resp, err := bs.client.Do(request)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	res := &QueryResp{}
	if err := json.NewDecoder(resp.Body).Decode(res); err != nil {
		return nil, err
	}
	switch {
	case resp.StatusCode == 200:
		return res, nil
	case resp.StatusCode == 404:
		return nil, ErrBlobNotFound
	default:
		return nil, fmt.Errorf("failed to query blobs")
	}
}

// Get fetch the given blob.
func (bs *BlobStore) Get(hash string) ([]byte, error) {
	request, err := http.NewRequest("GET", bs.ServerAddr+"/blob/"+hash, nil)
	if err != nil {
		return nil, err
	}
	resp, err := bs.client.Do(request)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	switch {
	case resp.StatusCode == 200:
		return body, nil
	case resp.StatusCode == 404:
		return nil, ErrBlobNotFound
	default:
		return nil, fmt.Errorf("failed to get blob %v: %v", hash, string(body))
	}
}

// Put upload the given blob
func (bs *BlobStore) Put(blob []byte) error {
	hash := fmt.Sprintf("%x", sha1.Sum(blob))
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	part, err := writer.CreateFormFile(hash, hash)
	if err != nil {
		return err
	}
	if _, err := part.Write(blob); err != nil {
		return err
	}
	if err := writer.Close(); err != nil {
		return err
	}
	request, err := http.NewRequest("POST", bs.ServerAddr+"/upload", body)
	if err != nil {
		return err
	}
	resp, err := bs.client.Do(request)
	if err != nil {
		return err
	}
	body.Reset()
	body.ReadFrom(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("failed to put blob %v", body.String())
	}
	return nil
}
