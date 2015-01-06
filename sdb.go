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
)

var (
	sdb    *lsdb.SDB
	domain *lsdb.Domain
	bucket *ls3.Bucket
)

func WriteJSON(w http.ResponseWriter, data interface{}) {
	js, err := json.Marshal(data)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(js)
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
	query, err := sdb.Select(fmt.Sprintf("SELECT * FROM s3indextest2 where time > '%s' and time <= '%s' order by time asc", start, end), false)
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
			if err := bucket.Put(hash, data.Bytes(), "", ls3.Private, ls3.Options{}); err != nil {
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
	domain = sdb.Domain("s3indextest2")
	if _, err := domain.CreateDomain(); err != nil {
		panic(err)
	}
	bucket = s3.Bucket("thomassileo.s3indextexst")
	if err := bucket.PutBucket(ls3.Private); err != nil {
		panic(err)
	}
	//r, err := sdb.Select("SELECT * FROM s3indextest2 where time > '2010' order by time asc", false)
	//log.Printf("%+v, %+v", r, err)
	//i := d.Item("ok")
	//pa := &lsdb.PutAttrs{}
	//pa.Add("a", "b")
	//r2, err := i.PutAttrs(pa)
	//r2, err := i.Attrs(nil, false)
	//log.Printf("%v, %v", r2, err)
	runtime.GOMAXPROCS(runtime.NumCPU())
	http.HandleFunc("/api/blobs", blobsHandler)
	http.HandleFunc("/api/upload", uploadHandler)
	http.ListenAndServe(":8010", nil)
}
