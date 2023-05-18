GWPULSAR_NAMESPACE := fcasula
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
	kubectl -n $(GWPULSAR_NAMESPACE) apply -f deployment.yaml

.PHONY: undeploy
undeploy:
	kubectl -n $(GWPULSAR_NAMESPACE) delete -f deployment.yaml

.PHONY: create-topic
create-topic:
	kubectl -n pulsar exec -it pulsar-broker-0 -- /pulsar/bin/pulsar-admin \
		topics create-partitioned-topic \
		persistent://public/default/$(TOPIC_NAME) --partitions 1

.PHONY: list-topics
list-topics:
	kubectl -n pulsar exec -it pulsar-broker-0 -- /pulsar/bin/pulsar-admin \
		topics list public/default

.PHONY: delete-topic
delete-topic:
	kubectl -n $(GWPULSAR_NAMESPACE) delete -f deployment.yaml ;\
	kubectl -n pulsar exec -it pulsar-broker-0 -- /pulsar/bin/pulsar-admin \
    	topics delete-partitioned-topic persistent://public/default/$(TOPIC_NAME) -f

.PHONY: pulsar-proxy
pulsar-proxy:
	kubectl -n pulsar port-forward pulsar-proxy-0 6650

.PHONY: gwpulsar-pf
gwpulsar-pf:
	kubectl -n $(GWPULSAR_NAMESPACE) port-forward \
		$$(kubectl -n fcasula get pods -l app=pulsar-exp -o jsonpath='{.items[*].metadata.name}') \
		8080

.PHONY: gwpulsar-logs
gwpulsar-logs:
	kubectl -n $(GWPULSAR_NAMESPACE) logs -l app=pulsar-exp -f
