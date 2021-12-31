from .constants import *
from multiprocessing import Queue
from typing import Iterator
from redis import Redis
from time import perf_counter
import socket
import ssl
import brotli

context = ssl.create_default_context()

def parse_chunked_body(data):
    temp = b""
    while True:
        size, data = data.split(b"\r\n", 1)
        size = int(size, 16)
        if not size: break
        temp += data[:size]
        data = data[size+2:]
    temp = brotli.decompress(temp)
    return temp

def find_channel_ids(data: bytes):
    ids = set()
    index = 0
    while True:
        offset = data.find(b'browseId": "', index)
        if offset == -1: return tuple(ids)
        ids.add(data[offset+12:offset+36].decode())
        index = offset + 24

def find_video_ids(data: bytes):
    ids = set()
    index = 0
    while True:
        offset = data.find(b"?v=", index)
        if offset == -1: return tuple(ids)
        ids.add(data[offset+3:offset+14].decode())
        index = offset + 11

def find_playlist_ids(data: bytes):
    ids = set()
    index = 0
    while True:
        offset = data.find(b"list=", index)
        if offset == -1: return tuple(ids)
        ids.add(data[offset+5:offset+39].split(b'"', 1)[0].decode())
        index = offset + 34

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
        
        proxy = next(proxy_iter)

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
            continue

        while True:
            try: target_type, target = crawl_queue.get(True)
            except: continue

            try:
                if crawl_cache.get(target): continue
                crawl_cache.set(target, 1)

                if target_type == CHANNEL:
                    # Get channel's playlists
                    sock.sendall((
                        f"GET /channel/{target}/playlists HTTP/1.1\r\n"
                        "Host: www.youtube.com\r\n"
                        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.54 Safari/537.36\r\n"
                        "Accept-Encoding: br\r\n"
                        "Cookie: CONSENT=YES+cb.20211026-09-p1.en+FX+731\r\n"
                        "\r\n"
                    ).encode())
                    resp = sock.recv(1024000)

                    if resp.startswith(b"HTTP/1.0 404"):
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

                    playlist_ids = find_playlist_ids(body)
                    for index, cached in enumerate(crawl_cache.mget(playlist_ids)):
                        playlist_id = playlist_ids[index]
                        if not cached:
                            crawl_queue.put((PLAYLIST, playlist_id))
                        
                    #
                    sock.sendall((
                        f"GET /channel/{target}/videos HTTP/1.1\r\n"
                        "Host: www.youtube.com\r\n"
                        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.54 Safari/537.36\r\n"
                        "Accept-Encoding: br\r\n"
                        "Cookie: CONSENT=YES+cb.20211026-09-p1.en+FX+731\r\n"
                        "\r\n"
                    ).encode())
                    resp = sock.recv(1024000)

                    if resp.startswith(b"HTTP/1.0 404"):
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
                    
                    video_ids = find_video_ids(body)
                    for index, cached in enumerate(crawl_cache.mget(video_ids)):
                        video_id = video_ids[index]
                        if not cached:
                            print(f"https://www.youtube.com/watch?v={video_id}")
                            crawl_queue.put((VIDEO, video_id))
                        
                    try:
                        continuation_key = body.split(b'"token":"', 1)[1].split(b'"', 1)[0].decode()
                    except:
                        continue
                    
                    for _ in range(5):
                        payload = '{"context":{"client":{"clientName":"WEB","clientVersion":"2.20211025.01.00"},"user":{"lockedSafetyMode":false}},"continuation":"%s"}' % continuation_key
                        sock.sendall((
                            "POST /youtubei/v1/browse?key=AIzaSyAO_FJ2SlqU8Q4STEHLGCilw_Y9_11qcW8 HTTP/1.1\r\n"
                            "Host: www.youtube.com\r\n"
                            "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.54 Safari/537.36\r\n"
                            "Accept-Encoding: br\r\n"
                            f"Content-Length: {len(payload)}\r\n"
                            "Content-Type: application/json\r\n"
                            "Cookie: CONSENT=YES+cb.20211026-09-p1.en+FX+731\r\n"
                            "\r\n"
                            f"{payload}"
                        ).encode())

                        resp = sock.recv(102400)
                        
                        if not resp.startswith(b"HTTP/1.1 200"):
                            print(f"RE-ADDED: Video list API for channel {target} returned non-OK status: {resp[:50]}")
                            crawl_queue.put((target_type, target))
                            break
                        
                        resp, body = resp.split(b"\r\n\r\n", 1)
                        length = int(resp.split(b"Content-Length: ", 1)[1].split(b"\r\n", 1)[0])
                        while length > len(body):
                            body += sock.recv(100000)
                        body = brotli.decompress(body)

                        video_ids = find_video_ids(body)
                        for index, cached in enumerate(crawl_cache.mget(video_ids)):
                            video_id = video_ids[index]
                            if not cached:
                                print(f"https://www.youtube.com/watch?v={video_id}")
                                crawl_queue.put((VIDEO, video_id))

                        try:
                            continuation_key = body.split(b'":{"token": "', 1)[1].split(b'"', 1)[0].decode()
                        except:
                            break

                elif target_type == VIDEO:
                    sock.sendall((
                        f"GET /watch?v={target} HTTP/1.1\r\n"
                        "Host: www.youtube.com\r\n"
                        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.54 Safari/537.36\r\n"
                        "Accept-Encoding: br\r\n"
                        "Cookie: CONSENT=YES+cb.20211026-09-p1.en+FX+731\r\n"
                        "\r\n"
                    ).encode())
                    resp = sock.recv(1024000)
                    
                    if resp.startswith(b"HTTP/1.0 404"):
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

                    for page in range(5):
                        payload = '{"context":{"client":{"clientName":"WEB","clientVersion":"2.20211025.01.00"},"user":{"lockedSafetyMode":false}},"continuation":"%s"}' % continuation_key
                        sock.sendall((
                            "POST /youtubei/v1/next?key=AIzaSyAO_FJ2SlqU8Q4STEHLGCilw_Y9_11qcW8 HTTP/1.1\r\n"
                            "Host: www.youtube.com\r\n"
                            "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.54 Safari/537.36\r\n"
                            "Accept-Encoding: br\r\n"
                            f"Content-Length: {len(payload)}\r\n"
                            "Content-Type: application/json\r\n"
                            "Cookie: CONSENT=YES+cb.20211026-09-p1.en+FX+731\r\n"
                            "\r\n"
                            f"{payload}"
                        ).encode())

                        resp = sock.recv(102400)
                        
                        if not resp.startswith(b"HTTP/1.1 200"):
                            print(f"RE-ADDED: Comment API for video {target} returned non-OK status: {resp[:50]}")
                            crawl_queue.put((target_type, target))
                            break
                        
                        resp, body = resp.split(b"\r\n\r\n", 1)
                        length = int(resp.split(b"Content-Length: ", 1)[1].split(b"\r\n", 1)[0])
                        while length > len(body):
                            body += sock.recv(100000)
                        body = brotli.decompress(body)

                        channel_ids = find_channel_ids(body)
                        for index, cached in enumerate(crawl_cache.mget(channel_ids)):
                            channel_id = channel_ids[index]
                            if not cached:
                                crawl_queue.put((CHANNEL, channel_id))

                        if not b"RELOAD_CONTINUATION_SLOT_BODY" in body:
                            break

                        continuation_key = body.rsplit(b'"token": "', 1)[1].split(b'"', 1)[0].decode()

                elif target_type == PLAYLIST:
                    sock.sendall((
                        f"GET /playlist?list={target} HTTP/1.1\r\n"
                        "Host: www.youtube.com\r\n"
                        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.54 Safari/537.36\r\n"
                        "Accept-Encoding: br\r\n"
                        "Cookie: CONSENT=YES+cb.20211026-09-p1.en+FX+731\r\n"
                        "\r\n"
                    ).encode())
                    resp = sock.recv(1024000)
                    
                    if resp.startswith(b"HTTP/1.0 404"):
                        print(f"DROPPED: Playlist {target} does not exist.")
                        continue

                    if not resp.startswith(b"HTTP/1.1 200"):
                        print(f"RE-ADDED: Playlist {target} returned non-OK status: {resp[:50]}")
                        crawl_cache.delete(target)
                        crawl_queue.put((target_type, target))
                        break

                    body = b""
                    while not body.endswith(b"0\r\n\r\n"):
                        body += sock.recv(100000)
                    body = parse_chunked_body(body)

                    video_ids = find_video_ids(body)
                    for index, cached in enumerate(crawl_cache.mget(video_ids)):
                        video_id = video_ids[index]
                        if not cached:
                            print(f"https://www.youtube.com/watch?v={video_id}")
                            crawl_queue.put((VIDEO, video_id))

            except (socket.timeout, ssl.SSLError):
                try: crawl_cache.delete(target)
                except: pass
                break
                
            except Exception as err:
                print(f"{err!r}")
                try: crawl_cache.delete(target)
                except: pass
                break
