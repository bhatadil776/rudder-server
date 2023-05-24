PULSAR_URL = pulsar://localhost:6650
TOPIC_NAME = persistent://public/fcasula/gwpulsar-test

.PHONY: run
run:
	PULSAR_URL=$(PULSAR_URL) TOPIC_NAME=$(TOPIC_NAME) go run main.go

.PHONY: run-verify
run-verify:
	PULSAR_URL=$(PULSAR_URL) TOPIC_NAME=$(TOPIC_NAME) VERIFY=1 go run main.go
