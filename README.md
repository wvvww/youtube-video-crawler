# youtube-crawler
 
An attempt at finding truly random content on youtube by crawling comments and their authors' videos.

# Setup
* HTTP proxies are required
* A redis server is required

```bash
pip install -r requirements.txt
```


# Usage
```
python crawl.py --workers 8 --queue-file queue.txt --proxy-file proxies.txt --redis-host 127.0.0.1 --redis-password hunter2 --redis-db 6
```

The tool will output links to all the videos it crawls through.
