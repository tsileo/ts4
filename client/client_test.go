package ts4

import (
	"bytes"
	"crypto/rand"
	"crypto/sha1"
	"fmt"
	"testing"
)

func TestClient(t *testing.T) {
	t.Logf("ok")
	bs := New("")
	blobs := []string{}
	for i := 0; i < 5; i++ {
		blob := make([]byte, 50)
		rand.Read(blob)
		var err error
		hash, err := bs.Put(blob)
		if err != nil {
			t.Fatalf("failed to put blob %v: %v", hash, err)
		}
		blobs = append(blobs, hash)
		rblob, err := bs.Get(hash)
		if err != nil {
			t.Fatalf("failed to get blob %v: %v", hash, err)
		}
		if !bytes.Equal(blob, rblob) {
			t.Errorf("blob != rblob")
		}
	}
	cblobs := make(chan []byte)
	go bs.Iter("", "", cblobs)
	y := 0
	for b := range cblobs {
		hash := fmt.Sprintf("%x", sha1.Sum(b))
		if blobs[y] != hash {
			t.Errorf("blobs not ordered correctly")
		}
		y++
	}
}
