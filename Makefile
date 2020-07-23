run:
	docker run --network host -p 8123:8123 -p 9001:9001 -p 9092:9092 lkl-exporter

build:
	docker build -t lkl-exporter .

build_run: build run