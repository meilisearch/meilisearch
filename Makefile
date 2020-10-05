SRC=$(shell find meilisearch-* -name '*.rs')
OUT = target/release/meilisearch

all: image
	docker-compose up --scale raft=3 -V

image: $(OUT)
	cargo build --release
	docker build -t marinpostma/meilisearch .
