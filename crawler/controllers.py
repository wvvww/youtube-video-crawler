from .utils import parse_proxy_string, slice_list
from .workers import worker_func
from redis import Redis
import multiprocessing

def clear_db(redis_info):
    with Redis(**redis_info) as db:
        db.flushdb()

class Controller:
    def __init__(self, arguments):
        self.args = arguments
        self.proxies = []
        self.pipe, self.worker_pipe = multiprocessing.Pipe()
        self.redis_info = dict(
            host=self.args.redis_host,
            port=self.args.redis_port,
            password=self.args.redis_password,
            db=self.args.redis_db
        )

        clear_db(self.redis_info)
        self.load_proxies()
        self.load_queue_input()
    
    def load_proxies(self):
        if not self.args.proxy_file: return
        proxies = set()
        for line in self.args.proxy_file:
            try:
                proxy = parse_proxy_string(line.rstrip())
                if not proxy in proxies:
                    proxies.add(proxy)
            except:
                continue
        self.proxies.extend(proxies)

    def load_queue_input(self):
        if not self.args.queue_file: return
        for line in self.args.queue_file:
            fields = line.split(",")
            self.pipe.send(fields)

    def start(self):
        workers = [
            multiprocessing.Process(
                target=worker_func,
                kwargs=dict(
                    thread_count=self.args.threads,
                    redis_kwargs=self.redis_info,
                    proxy_list=slice_list(
                        self.proxies, num, self.args.workers),
                    pipe_in=self.worker_pipe,
                    pipe_out=self.pipe
                )
            )
            for num in range(self.args.workers)
        ]
        
        for worker in workers:
            worker.start()