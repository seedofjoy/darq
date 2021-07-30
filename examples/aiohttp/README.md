## Quick start


Start aiohttp server:
```bash
docker-compose up aiohttp
```

Start Darq worker (in another shell):
```bash
docker-compose up darq
```

Make some requests:
```bash
curl 0.0.0.0:8080
curl 0.0.0.0:8080/John
curl 0.0.0.0:8080/John
```

Output (Darq):
```
darq_1      | 06:28:30: Starting worker for 1 functions: aiohttp_example.apps.say_hello.tasks.say_hello_from_worker
darq_1      | 06:28:30: redis_version=5.0.7 mem_usage=834.18K clients_connected=1 db_keys=0
darq_1      | INFO:aiohttp_example.signals:Connected to dummy database
darq_1      | INFO:aiohttp_example.signals:Connected to redis (db=0)
darq_1      | 06:28:42:   0.29s → 17cf0570b4a14845bce3c7b83c610dff:aiohttp_example.apps.say_hello.tasks.say_hello_from_worker('Anonymous')
darq_1      | INFO:darq.worker:  0.29s → 17cf0570b4a14845bce3c7b83c610dff:aiohttp_example.apps.say_hello.tasks.say_hello_from_worker('Anonymous')
darq_1      | INFO:aiohttp_example.apps.say_hello.tasks:Hello from worker, Anonymous. Visited count: 1
darq_1      | 06:28:42:   0.00s ← 17cf0570b4a14845bce3c7b83c610dff:aiohttp_example.apps.say_hello.tasks.say_hello_from_worker ●
darq_1      | INFO:darq.worker:  0.00s ← 17cf0570b4a14845bce3c7b83c610dff:aiohttp_example.apps.say_hello.tasks.say_hello_from_worker ●
darq_1      | 06:28:46:   0.49s → 5aec165ed4f3496bb1dc9a0e38af865d:aiohttp_example.apps.say_hello.tasks.say_hello_from_worker('John')
darq_1      | INFO:darq.worker:  0.49s → 5aec165ed4f3496bb1dc9a0e38af865d:aiohttp_example.apps.say_hello.tasks.say_hello_from_worker('John')
darq_1      | INFO:aiohttp_example.apps.say_hello.tasks:Hello from worker, John. Visited count: 1
darq_1      | 06:28:46:   0.00s ← 5aec165ed4f3496bb1dc9a0e38af865d:aiohttp_example.apps.say_hello.tasks.say_hello_from_worker ●
darq_1      | INFO:darq.worker:  0.00s ← 5aec165ed4f3496bb1dc9a0e38af865d:aiohttp_example.apps.say_hello.tasks.say_hello_from_worker ●
darq_1      | 06:28:48:   0.02s → 62a7c5863f664096b1990ca778284d81:aiohttp_example.apps.say_hello.tasks.say_hello_from_worker('John')
darq_1      | INFO:darq.worker:  0.02s → 62a7c5863f664096b1990ca778284d81:aiohttp_example.apps.say_hello.tasks.say_hello_from_worker('John')
darq_1      | INFO:aiohttp_example.apps.say_hello.tasks:Hello from worker, John. Visited count: 2
darq_1      | 06:28:48:   0.01s ← 62a7c5863f664096b1990ca778284d81:aiohttp_example.apps.say_hello.tasks.say_hello_from_worker ●
darq_1      | INFO:darq.worker:  0.01s ← 62a7c5863f664096b1990ca778284d81:aiohttp_example.apps.say_hello.tasks.say_hello_from_worker ●
```
