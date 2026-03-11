import multiprocessing

# Use all available CPU cores (2 workers per core is optimal for I/O-bound work)
workers = multiprocessing.cpu_count() * 2 + 1
worker_class = "gthread"
threads = 4
timeout = 600
graceful_timeout = 600
keepalive = 5
worker_connections = 1000
