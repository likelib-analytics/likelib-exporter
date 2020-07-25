run:
	docker run --network host lkl-exporter

build:
	docker build -t lkl-exporter .

build_run: build run