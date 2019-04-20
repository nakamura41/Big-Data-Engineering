### Cassandra
```bash
docker run --name cassandra-server --network cda -d -e CASSANDRA_BROADCAST_ADDRESS=172.30.0.172 -p 7000:7000 -p 9042:9042 -v /mnt/data/var/lib/cassandra:/var/lib/cassandra cassandra:3.11.4
```

### Elasticsearch
```bash
docker run -d --name elasticsearch-server --network cda -p 9200:9200 -p 9300:9300 -e "discovery.type=single-node" -v /mnt/data/var/lib/elasticsearch/data:/usr/share/elasticsearch/data -v /mnt/data/var/lib/elasticsearch/logs:/usr/share/elasticsearch/logs elasticsearch:7.0.0
```

### Redis
```bash
docker run --name redis-server -p 6379:6379 -d redis:5.0.4 redis-server --appendonly yes
docker run --name redis-server --network cda -v /mnt/data/var/lib/redis/data:/data -p 6379:6379 -d redis:5.0.4 redis-server --appendonly yes
```

```bash
sudo apt-get install redis -y
docker run -it --network cda --rm redis:5.0.4 redis-cli -h localhost
```