PULSAR_URL = pulsar://localhost:6650
TOPIC_NAME = persistent://public/fcasula/gwpulsar-test

.PHONY: run
run:
	PULSAR_URL=$(PULSAR_URL) TOPIC_NAME=$(TOPIC_NAME) MODE=ORDER go run main.go

.PHONY: run-verify
run-verify:
	PULSAR_URL=$(PULSAR_URL) TOPIC_NAME=$(TOPIC_NAME) MODE=VERIFY go run main.go

.PHONY: run-performance
run-performance:
	PULSAR_URL=$(PULSAR_URL) TOPIC_NAME=$(TOPIC_NAME) MODE=PERFORMANCE go run main.go
