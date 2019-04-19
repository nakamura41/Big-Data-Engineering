```bash
docker run --name cassandra-server -d --network bridge -e CASSANDRA_BROADCAST_ADDRESS=172.30.0.172 -p 7000:7000 -p 9042:9042 -v /mnt/data/var/lib/cassandra:/var/lib/cassandra cassandra:3.11.4
```

```bash
docker run -d --name elasticsearch-server --network bridge -p 9200:9200 -p 9300:9300 -e "discovery.type=single-node" -v /mnt/data/var/lib/elasticsearch/data:/usr/share/elasticsearch/data -v /mnt/data/var/lib/elasticsearch/logs:/usr/share/elasticsearch/logs elasticsearch:7.0.0
```