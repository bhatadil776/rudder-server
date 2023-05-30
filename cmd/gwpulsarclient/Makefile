GWPULSAR_NAMESPACE := fcasula
REGISTRY_USERNAME := fracasula
IMAGE_NAME := pulsar-sender
DOCKER_REGISTRY := docker.io
IMAGE_NAME := $(DOCKER_REGISTRY)/$(REGISTRY_USERNAME)/$(IMAGE_NAME)
PULSAR_URL = pulsar://localhost:6650
TOPIC_NAME = persistent://public/fcasula/gwpulsar-test

### VERIFY PARAMS
NUM_SOURCES := 5
USERS_PER_SOURCE := 2
MESSAGES_PER_USER := 500

.PHONY: build
build:
	docker build -t $(IMAGE_NAME) .

.PHONY: push
push: build
	#docker tag $(IMAGE_NAME) $(TAGGED_IMAGE_NAME)
	docker tag $(IMAGE_NAME) $(IMAGE_NAME):latest
	docker push $(IMAGE_NAME):latest

.PHONY: deploy
deploy:
	kubectl -n $(GWPULSAR_NAMESPACE) apply -f job.yaml

.PHONY: undeploy
undeploy:
	kubectl -n $(GWPULSAR_NAMESPACE) delete -f job.yaml

.PHONY: logs
logs:
	kubectl -n $(GWPULSAR_NAMESPACE) logs -l app=pulsar-sender -f

.PHONY: run-verify
run-verify:
	PULSAR_URL=$(PULSAR_URL) TOPIC_NAME=$(TOPIC_NAME) MODE=VERIFY \
	NUM_SOURCES=$(NUM_SOURCES) \
	USERS_PER_SOURCE=$(USERS_PER_SOURCE) \
	MESSAGES_PER_USER=$(MESSAGES_PER_USER) \
		go run main.go

.PHONY: reset-cursor
reset-cursor:
	kubectl -n pulsar exec -it pulsar-toolset-0 -- bin/pulsar-admin \
		topics reset-cursor $(TOPIC_NAME) -s test-client-subscription -t 999w
