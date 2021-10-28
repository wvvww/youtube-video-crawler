from multiprocessing import Queue
from typing import Iterator
from redis import Redis
import socket
import ssl
import zlib

context = ssl.create_default_context()

def parse_chunked_body(data):
    temp = b""
    while True:
        size, data = data.split(b"\r\n", 1)
        size = int(size, 16)
        if not size: break
        temp += data[:size]
        data = data[size+2:]
    if temp.endswith(b"\x00"):
        temp = zlib.decompress(temp, -15)
    return temp

def find_channel_ids(data: bytes):
    ids = set()
    index = 0
    while True:
        offset = data.find(b'browseId": "', index)
        if offset == -1: return ids
        ids.add(data[offset+12:offset+36].decode())
        index = offset + 24

def find_video_ids(data: bytes):
    ids = set()
    index = 0
    while True:
        offset = data.find(b"?v=", index)
        if offset == -1: return ids
        ids.add(data[offset+3:offset+14].decode())
        index = offset + 11

def crawler(
    crawl_queue: Queue,
    crawl_cache: Redis,
    proxy_iter: Iterator
    ):
    sock = None

    while True:
        if sock:
            try: sock.shutdown(2)
            except OSError: pass
            sock.close()
            sock = None
        
        proxy = next(proxy_iter, None)

        try:
            sock = socket.socket()
            sock.settimeout(5)
            sock.connect(proxy[1])
            sock.sendall(f"CONNECT www.youtube.com:443 HTTP/1.1\r\nProxy-Authorization: {proxy[0]}\r\n\r\n".encode())
            resp = sock.recv(1024000)
            if not any(resp.startswith(f"HTTP/{v} 200".encode()) for v in ("1.0", "1.1")):
                continue
            sock = context.wrap_socket(sock, False, False, False, "www.youtube.com")
            sock.do_handshake()
        except:
            pass

        while True:
            target = None
            try:
                target_type, target = crawl_queue.get(True)

                if crawl_cache.get(target):
                    continue

                crawl_cache.set(target, 1)

                if target_type == "channel":
                    sock.sendall((
                        f"GET /channel/{target}/videos HTTP/1.1\r\n"
                        "Host: www.youtube.com\r\n"
                        "Accept-Encoding: deflate\r\n"
                        "\r\n"
                    ).encode())
                    resp = sock.recv(1024000)

                    if resp.startswith(b"HTTP/1.1 404"):
                        print(f"DROPPED: Channel {target} does not exist.")
                        continue

                    if not resp.startswith(b"HTTP/1.1 200"):
                        print(f"RE-ADDED: Channel {target} returned non-OK status: {resp[:50]}")
                        crawl_cache.delete(target)
                        crawl_queue.put((target_type, target))
                        break

                    body = b""
                    while not body.endswith(b"0\r\n\r\n"):
                        body += sock.recv(100000)
                    body = parse_chunked_body(body)
                    
                    for video_id in find_video_ids(body):
                        if not crawl_cache.get(video_id):
                            print(f"https://www.youtube.com/watch?v={video_id}")
                            crawl_queue.put(("video", video_id))

                elif target_type == "video":
                    sock.sendall((
                        f"GET /watch?v={target} HTTP/1.1\r\n"
                        "Host: www.youtube.com\r\n"
                        "Accept-Encoding: deflate\r\n"
                        "\r\n"
                    ).encode())
                    resp = sock.recv(1024000)
                    
                    if resp.startswith(b"HTTP/1.1 404"):
                        print(f"DROPPED: Video {target} does not exist.")
                        continue

                    if not resp.startswith(b"HTTP/1.1 200"):
                        print(f"RE-ADDED: Video {target} returned non-OK status: {resp[:50]}")
                        crawl_cache.delete(target)
                        crawl_queue.put((target_type, target))
                        break

                    body = b""
                    while not body.endswith(b"0\r\n\r\n"):
                        body += sock.recv(100000)
                    body = parse_chunked_body(body)
                    
                    continuation_key = body.split(b'":{"token":"', 1)[1].split(b'"', 1)[0].decode()
                    payload = '{"context":{"client":{"clientName":"WEB","clientVersion":"2.20211025.01.00"},"user":{"lockedSafetyMode":false}},"continuation":"%s"}' % continuation_key
                    sock.sendall((
                        "POST /youtubei/v1/next?key=AIzaSyAO_FJ2SlqU8Q4STEHLGCilw_Y9_11qcW8 HTTP/1.1\r\n"
                        "Host: www.youtube.com\r\n"
                        "Accept-Encoding: deflate\r\n"
                        f"Content-Length: {len(payload)}\r\n"
                        "Content-Type: application/json\r\n"
                        "\r\n"
                        f"{payload}"
                    ).encode())

                    resp = sock.recv(102400)
                    
                    if not resp.startswith(b"HTTP/1.1 200"):
                        print(f"RE-ADDED: Comment API for video {target} returned non-OK status: {resp[:50]}")
                        crawl_queue.put((target_type, target))
                        break

                    body = resp.split(b"\r\n\r\n", 1)[1]
                    while not body.endswith(b"0\r\n\r\n"):
                        body += sock.recv(100000)
                    body = parse_chunked_body(body)

                    for channel_id in find_channel_ids(body):
                        if not crawl_cache.get(channel_id):
                            crawl_queue.put(("channel", channel_id))
                    
            except (socket.timeout, ssl.SSLError):
                try: crawl_cache.delete(target)
                except: pass
                break
                
            except Exception as err:
                print(f"{err!r}")
                try: crawl_cache.delete(target)
                except: pass
                break