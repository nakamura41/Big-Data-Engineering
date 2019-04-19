```bash
docker run --name cassandra-server -d -e CASSANDRA_BROADCAST_ADDRESS=172.30.0.172 -p 7000:7000 -p 9042:9042 -v /mnt/data/var/lib/cassandra:/var/lib/cassandra cassandra:3.11.4
```

```bash
docker run -d --name elasticsearch-server -p 9200:9200 -p 9300:9300 -e "discovery.type=single-node" -v /mnt/data/var/lib/elasticsearch:/var/lib/elasticsearch -v /mnt/data/var/log/elasticsearch:/var/log/elasticsearch elasticsearch:7.0.0
```