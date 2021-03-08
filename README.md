# Demo Redis Streams con Producer e Consumer Threads

## Config
Crea il file __init__.py da __init__.py.example con le credenziali e i dettagli dell'host Redis
```python
import redis

host = '<host>'
port = 5002
user = 'default'
password = '<password>'

#pool = redis.ConnectionPool(host=host, port=port, password=password, decode_responses=True)
pool = redis.ConnectionPool(host=host, port=port, connection_class=redis.SSLConnection, password=password, decode_responses=True)
```

## Esecuzione

Vengono lanciati tre thread per tre producer che in modo asincrono e con tempistiche diverse generano messaggi nei tre diversi stream.
