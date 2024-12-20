(cd src/engine; docker build -t osseahorse .)
(cd src/api; docker build -t oscoral .)
docker compose up

curl -X POST http://localhost:3000/v0/nodes        -H "Content-Type: application/json"        -d '{"nodes": [{"name": "node1", "endpoint": "redis://127.0.0.1:6388"}, {"name":"node2", "endpoint":"redis://127.0.0.1:6389"}, {"name":"node3", "endpoint":"redis://127.0.0.1:6390"}] }'
