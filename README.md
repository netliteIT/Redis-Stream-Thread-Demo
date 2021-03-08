# Demo Redis Streams con Producer e Consumer Threads

## Config
Crea il file __init__.py da __init__.py.example con le credenziali e i dettagli dell'host Redis
```python
host = '<host>'
port = <port>
password = '<password>'
```

## Esecuzione

Vengono lanciati tre thread per tre producer che in modo asincrono e con tempistiche diverse generano messaggi nei tre diversi stream.
