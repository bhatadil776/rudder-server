GWPULSAR_NAMESPACE := fcasula
REGISTRY_USERNAME := fracasula
IMAGE_NAME := gwconsumer
DOCKER_REGISTRY := docker.io
IMAGE_NAME := $(DOCKER_REGISTRY)/$(REGISTRY_USERNAME)/$(IMAGE_NAME)
TOPIC_NAME = persistent://public/fcasula/gwpulsar-test

.PHONY: build
build:
	docker build -t $(IMAGE_NAME) .

.PHONY: push
push: build
	docker tag $(IMAGE_NAME) $(IMAGE_NAME):latest
	docker push $(IMAGE_NAME):latest

.PHONY: deploy
deploy:
	kubectl -n $(GWPULSAR_NAMESPACE) apply -f job.yaml

.PHONY: undeploy
undeploy:
	kubectl -n $(GWPULSAR_NAMESPACE) delete -f job.yaml

.PHONY: reset-cursor
reset-cursor:
	kubectl -n pulsar exec -it pulsar-toolset-0 -- bin/pulsar-admin \
		topics reset-cursor $(TOPIC_NAME) -s gwconsumer -t 9999w
