# likelib-exporter

Run exporter:

1. Start clickhouse with `docker-compose up` from [infrastructure repo](https://github.com/likelib-analytics/infrastructure)
2. Build docker (e.g. `docker build -t lkl-exporter .`)
3. Run docker app (e.g. `docker run --network host -p 8123:8123 -p 9001:9001 lkl-exporter`)
