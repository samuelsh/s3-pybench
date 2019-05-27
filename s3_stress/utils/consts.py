import os

KB1 = 1024
MB1 = KB1 * 1024
MAX_WORKER_THREADS = 50
MAX_WORKER_PROCESSES = 20
RANDOM_CHUNK_2M = os.urandom(2 * 1024 * 1024)
RANDOM_CHUNK_1M = os.urandom(1 * 1024 * 1024)
RANDOM_CHUNK_256K = os.urandom(256 * 1024)
RANDOM_CHUNK_4K = os.urandom(4 * 1024)
DATA_CHUNKS = {"4K": 4 * KB1, "256K": 256 * KB1, "1M": MB1, "2M": 2 * MB1, "5M": 5 * MB1,
               "10M": 10 * MB1, "20M": 20 * MB1}
DATA_BUFFERS = {"4K": os.urandom(4 * KB1), "256K": os.urandom(256 * KB1), "1M": os.urandom(MB1),
                "2M": os.urandom(2 * MB1), "5M": os.urandom(5 * MB1),
                "10M": os.urandom(10 * MB1), "20M": os.urandom(20 * MB1)}
TMP_FILE = 'tmpfile'
