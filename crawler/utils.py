from base64 import b64encode

def slice_list(lst, num, total):
    per = int(len(lst)/total)
    chunk = lst[per * num : per * (num + 1)]
    return chunk

def parse_proxy_string(proxy_str):
    proxy_str = proxy_str.rpartition("://")[2]
    auth, _, fields = proxy_str.rpartition("@")
    fields = fields.split(":", 2)

    if len(fields) == 2:
        hostname, port = fields
        if auth:
            auth = "Basic " + b64encode(auth.encode()).decode()
        addr = (hostname.lower(), int(port))
        return auth, addr

    elif len(fields) == 3:
        hostname, port, auth = fields
        auth = "Basic " + b64encode(auth.encode()).decode()
        addr = (hostname.lower(), int(port))
        return auth, addr
    
    raise Exception(f"Unrecognized proxy format: {proxy_str}")