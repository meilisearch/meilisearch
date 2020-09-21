all:
	cargo build --release
	docker build -t marinpostma/meilisearch .
	docker-compose up --scale raft=3 -V
