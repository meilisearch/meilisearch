all: image
	docker-compose up --scale raft=3 -V
bin: 
	cargo build --release
image: bin
	docker build -t marinpostma/meilisearch .
