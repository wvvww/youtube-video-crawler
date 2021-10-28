from .threads import crawler
import itertools
import threading
import redis

def worker_func(thread_count, redis_kwargs, proxy_list, **threads_kwargs):
    crawl_cache = redis.Redis(**redis_kwargs)
    proxy_iter = itertools.cycle(proxy_list)
    threads = [
        threading.Thread(
            target=crawler,
            kwargs=dict(
                proxy_iter=proxy_iter,
                crawl_cache=crawl_cache,
                **threads_kwargs
            )
        )
        for num in range(thread_count)
    ]

    for thread in threads:
        thread.start()