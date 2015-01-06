package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"runtime"
	"time"

	"github.com/goamz/goamz/aws"
	lsdb "github.com/goamz/goamz/exp/sdb"
	ls3 "github.com/goamz/goamz/s3"
	"github.com/gorilla/mux"
)

var (
	sdb    *lsdb.SDB
	domain *lsdb.Domain
	bucket *ls3.Bucket
)

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
func blobsHandler(w http.ResponseWriter, r *http.Request) {
	start := r.URL.Query().Get("start")
	if start == "" {
		start = "0"
	}
	end := r.URL.Query().Get("end")
	if end == "" {
		end = time.Now().UTC().Format(time.RFC3339Nano)
	}
	query, err := sdb.Select(fmt.Sprintf("SELECT * FROM s3indextest4 where time > '%s' and time <= '%s' order by time asc", start, end), false)
	if err != nil {
		log.Printf("err:%v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	res := []map[string]string{}
	for _, i := range query.Items {
		m := map[string]string{
			"name": i.Name,
		}
		for _, attr := range i.Attrs {
			m[attr.Name] = attr.Value
		}
		res = append(res, m)
	}
	WriteJSON(w, map[string]interface{}{
		"data":  res,
		"start": start,
		"end":   end,
	})
}

func uploadHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "POST":
		//parse the multipart form in the request
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
			if err := bucket.Put(addSlash(hash), data.Bytes(), "", ls3.Private, ls3.Options{}); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			item := domain.Item(hash)
			pa := &lsdb.PutAttrs{}
			pa.Add("time", time.Now().UTC().Format(time.RFC3339Nano))
			if _, err := item.PutAttrs(pa); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
		}
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func main() {
	auth, err := aws.EnvAuth()
	if err != nil {
		panic(err)
	}
	sdb = lsdb.New(auth, aws.USEast)
	s3 := ls3.New(auth, aws.USEast)
	domain = sdb.Domain("s3indextest4")
	if _, err := domain.CreateDomain(); err != nil {
		panic(err)
	}
	bucket = s3.Bucket("thomassileo.s3indextexst2")
	if err := bucket.PutBucket(ls3.Private); err != nil {
		panic(err)
	}
	runtime.GOMAXPROCS(runtime.NumCPU())
	r := mux.NewRouter()
	r.HandleFunc("/api/blobs", blobsHandler)
	r.HandleFunc("/api/blob/{hash}", blobHandler)
	r.HandleFunc("/api/upload", uploadHandler)
	http.Handle("/", r)
	http.ListenAndServe(":8010", nil)
}
