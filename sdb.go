package main

import (
	"bytes"
	"crypto/sha1"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"time"

	"github.com/goamz/goamz/aws"
	lsdb "github.com/goamz/goamz/exp/sdb"
	ls3 "github.com/goamz/goamz/s3"
	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
)

var (
	sdb       *lsdb.SDB
	domain    *lsdb.Domain
	bucket    *ls3.Bucket
	sdbDomain string
	s3Bucket  string
	startedAt string
)

var version = "0.0.0"

func addSlash(s string) string {
	return s[0:2] + "/" + s[2:40]
}

func WriteJSON(w http.ResponseWriter, data interface{}) {
	js, err := json.Marshal(data)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(js)
}

func blobHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	blob, err := bucket.Get(addSlash(vars["hash"]))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Write(blob)
}
func statsHandler(w http.ResponseWriter, r *http.Request) {
	query, err := sdb.Select(fmt.Sprintf("SELECT COUNT(*) FROM %v", sdbDomain), false)
	if err != nil {
		log.Printf("err:%v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	i := query.Items[0]
	cnt, _ := strconv.Atoi(i.Attrs[0].Value)
	sizeCounter := domain.Item("size")
	attrs, err := sizeCounter.Attrs(nil, false)
	if err != nil {
		log.Printf("err:%v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	var size int
	if len(attrs.Attrs) != 0 {
		size, _ = strconv.Atoi(attrs.Attrs[0].Value)
	}
	WriteJSON(w, map[string]interface{}{
		"blob_count": cnt - 1,
		"blob_size":  size,
		"version":    version,
		"started_at": startedAt,
		"s3_bucket":  s3Bucket,
		"sdb_domain": sdbDomain,
	})
}
func blobsHandler(w http.ResponseWriter, r *http.Request) {
	start := r.URL.Query().Get("start")
	if start == "" {
		start = "0"
	}
	end := r.URL.Query().Get("end")
	if end == "" {
		end = time.Now().UTC().Format(time.RFC3339Nano)
	}
	query, err := sdb.Select(fmt.Sprintf("SELECT * FROM %v where time > '%s' and time <= '%s' order by time asc", sdbDomain, start, end), false)
	if err != nil {
		log.Printf("err:%v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	res := []map[string]string{}
	for _, i := range query.Items {
		m := map[string]string{
			"hash": i.Name,
		}
		for _, attr := range i.Attrs {
			m[attr.Name] = attr.Value
		}
		res = append(res, m)
	}
	WriteJSON(w, map[string]interface{}{
		"blobs": res,
		"start": start,
		"end":   end,
	})
}

func uploadHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "POST":
		mr, err := r.MultipartReader()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		for {
			part, err := mr.NextPart()
			if err == io.EOF {
				break
			}
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			hash := part.FormName()
			var data bytes.Buffer
			data.ReadFrom(part)
			blob := data.Bytes()
			computedHash := fmt.Sprintf("%x", sha1.Sum(blob))
			if hash != computedHash {
				http.Error(w, "hash don't match", http.StatusInternalServerError)
			}
			if err := bucket.Put(addSlash(hash), blob, "", ls3.Private, ls3.Options{}); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			item := domain.Item(hash)
			pa := &lsdb.PutAttrs{}
			itemAttrs, err := item.Attrs(nil, false)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			if len(itemAttrs.Attrs) != 0 {
				// the blob is already indexed
				return
			}
			pa.Add("time", time.Now().UTC().Format(time.RFC3339Nano))
			if _, err := item.PutAttrs(pa); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			sizeCounter := domain.Item("size")
			for {
				attrs, err := sizeCounter.Attrs(nil, true)
				if err != nil {
					http.Error(w, err.Error(), http.StatusInternalServerError)
				}
				update := &lsdb.PutAttrs{}
				var prevSize int
				if len(attrs.Attrs) != 0 {
					prevSize, _ = strconv.Atoi(attrs.Attrs[0].Value)
					update.Replace("size", strconv.Itoa(prevSize+len(blob)))
					update.IfValue("size", strconv.Itoa(prevSize))
				} else {
					update.Add("size", strconv.Itoa(prevSize+len(blob)))
				}
				if _, err := sizeCounter.PutAttrs(update); err != nil {
					continue
				}
				break
			}
		}
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func main() {
	startedAt = time.Now().Format(time.RFC3339)
	auth, err := aws.EnvAuth()
	if err != nil {
		panic(err)
	}
	sdbDomain = os.Getenv("TS4_SDB_DOMAIN")
	if sdbDomain == "" {
		panic("TS4_SDB_DOMAIN not set")
	}
	s3Bucket = os.Getenv("TS4_S3_BUCKET")
	if s3Bucket == "" {
		panic("TS4_S3_BUCKET not set")
	}
	sdb = lsdb.New(auth, aws.USEast)
	s3 := ls3.New(auth, aws.USEast)
	domain = sdb.Domain(sdbDomain)
	if _, err := domain.CreateDomain(); err != nil {
		panic(err)
	}
	bucket = s3.Bucket(s3Bucket)
	if err := bucket.PutBucket(ls3.Private); err != nil {
		panic(err)
	}
	runtime.GOMAXPROCS(runtime.NumCPU())
	r := mux.NewRouter()
	r.Handle("/api/blobs", handlers.LoggingHandler(os.Stdout, http.HandlerFunc(blobsHandler)))
	r.Handle("/api/blob/{hash}", handlers.LoggingHandler(os.Stdout, http.HandlerFunc(blobHandler)))
	r.Handle("/api/upload", handlers.LoggingHandler(os.Stdout, http.HandlerFunc(uploadHandler)))
	r.Handle("/_stats", handlers.LoggingHandler(os.Stdout, http.HandlerFunc(statsHandler)))
	http.Handle("/", r)
	log.Printf("Starting ts4 version %v; %v (%v/%v)", version, runtime.Version(), runtime.GOOS, runtime.GOARCH)
	log.Printf("Listening on port 8010")
	http.ListenAndServe(":8010", nil)
}
