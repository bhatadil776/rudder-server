REGISTRY_USERNAME := fracasula
IMAGE_NAME := pulsar-exp
DOCKER_REGISTRY := docker.io
IMAGE_VERSION := $(shell date +%Y%m%d%H%M%S)
IMAGE_NAME := $(DOCKER_REGISTRY)/$(REGISTRY_USERNAME)/$(IMAGE_NAME)
TAGGED_IMAGE_NAME := $(IMAGE_NAME):$(IMAGE_VERSION)

# PULSAR VARS
TOPIC_NAME := gwpulsar-test-01

.PHONY: build
build:
	docker build -t $(IMAGE_NAME) .

.PHONY: push
push: build
	#docker tag $(IMAGE_NAME) $(TAGGED_IMAGE_NAME)
	docker tag $(IMAGE_NAME) $(IMAGE_NAME):latest
	docker push $(IMAGE_NAME):latest

.PHONY: clean
clean:
	docker rmi $(IMAGE_NAME) --force

.PHONY: run
run:
	docker run -p 8080:8080 $(IMAGE_NAME)

.PHONY: deploy
deploy:
	kubectl apply -f deployment.yaml

.PHONY: undeploy
undeploy:
	kubectl delete -f deployment.yaml

.PHONY: create-topic
create-topic:
	kubens pulsar ;\
	kubectl exec -it pulsar-broker-0 -- /pulsar/bin/pulsar-admin topics create-partitioned-topic persistent://public/default/$(TOPIC_NAME) --partitions 1

.PHONY: list-topics
list-topics:
	kubens pulsar ;\
	kubectl exec -it pulsar-broker-0 -- /pulsar/bin/pulsar-admin topics list public/default

.PHONY: delete-topic
delete-topic:
	kubens pulsar ;\
    kubectl exec -it pulsar-broker-0 -- /pulsar/bin/pulsar-admin topics delete-partitioned-topic persistent://public/default/$(TOPIC_NAME)
