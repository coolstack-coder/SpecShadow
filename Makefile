TLC_VERSION ?= v1.7.4
JAR := tools/tla2tools.jar

.PHONY: build test tlc check eval figures

build:
	go build -o specshadow ./cmd/specshadow

$(JAR):
	mkdir -p tools
	curl -sSL -o $(JAR) https://github.com/tlaplus/tlaplus/releases/download/$(TLC_VERSION)/tla2tools.jar

tlc: $(JAR)

test: $(JAR)
	go vet ./...
	go test ./... -count=1

# record one chaos run and check it
check: build $(JAR)
	./specshadow run -scenario chaos -seed 1 -out /tmp/chaos.ndjson
	./specshadow check -trace /tmp/chaos.ndjson

eval: build $(JAR)
	./specshadow eval -out experiments/results.json

figures:
	python3 experiments/figures.py
