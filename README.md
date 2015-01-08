# ts4: time series simple storage service

A content-addressed blob store backed by S3, indexed by upload time (in SimpleDB) accessible via a simple HTTP API.

Initially designed to backup and replay SQS messages in chronological order.

## Client

A go client is available, see [godoc reference](https://godoc.org/github.com/tsileo/ts4/client).

## API

### /api/upload

A multipart/form-data endpoint, sha1 of the blob must be computed and used as name.

	$ curl -v -include  --form aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d=hello http://localhost:8010/api/upload	

### /api/blob/{hash}

Get the raw blob data.

	$ curl http://localhost:8010/api/blob/aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d
	hello

### /api/blobs?start=&end=

Retrieves blobs saved during the given range (start/end).

Start defaults to 0 and end to time.Now().UTC().

The time attributed is sorted lexicographically so any subset of time.RFC3339Nano will works.

	$ curl http://localhost:8010/api/blobs?start=2015-01
	{
    		"blobs": [
        	  {
        	    "hash": "aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d", 
        	    "time": "2015-01-06T23:21:47.718317501Z"
        	  }
    		], 
    		"end": "2015-01-06T23:23:12.069583002Z", 
    		"start": "2015-01"
	}

### /_stats

	$ curl http://localhost:8010/_stats
	{
	    "blob_count": "5", 
	    "blob_size": "534", 
	    "s3_bucket": "mybucket", 
	    "sdb_domain": "mydomain", 
	    "started_at": "2015-01-08T00:08:43+01:00", 
	    "version": "0.0.0"
	}

## Build with Docker

	$ sudo docker build -t tsileo/ts4 .
	$ sudo docker run -p 8010:8010 -e AWS_ACCESS_KEY=accesskey -e AWS_SECRET_KEY=secrekey -e TS4_S3_BUCKET=yourbucket -e TS4_SDB_DOMAIN=yourdomain tsileo/ts4

## License

MIT

