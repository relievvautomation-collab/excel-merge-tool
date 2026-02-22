import multiprocessing

bind = "0.0.0.0:10000"
workers = multiprocessing.cpu_count() * 2 + 1
threads = 4
timeout = 600
worker_class = "gthread"
max_requests = 1000
max_requests_jitter = 50
preload_app = True
