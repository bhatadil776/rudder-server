GWPULSAR_NAMESPACE := fcasula
REGISTRY_USERNAME := fracasula
IMAGE_NAME := gwpulsar
DOCKER_REGISTRY := docker.io
IMAGE_VERSION := $(shell date +%Y%m%d%H%M%S)
IMAGE_NAME := $(DOCKER_REGISTRY)/$(REGISTRY_USERNAME)/$(IMAGE_NAME)
TAGGED_IMAGE_NAME := $(IMAGE_NAME):$(IMAGE_VERSION)
TOPICS := 10
PARTITIONS := 100
BROKER := 0

# PULSAR VARS
TOPIC_NAME := gwpulsar-test

.PHONY: build
build:
	docker build -t $(IMAGE_NAME) .

.PHONY: push
push: build
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
	kubectl -n $(GWPULSAR_NAMESPACE) apply -f statefulset.yaml

.PHONY: undeploy
undeploy:
	kubectl -n $(GWPULSAR_NAMESPACE) delete -f statefulset.yaml

.PHONY: create-namespace
create-namespace:
	kubectl -n pulsar exec -it pulsar-toolset-0 -- /pulsar/bin/pulsar-admin \
		namespaces create public/$(GWPULSAR_NAMESPACE) ;\
	kubectl -n pulsar exec -it pulsar-toolset-0 -- /pulsar/bin/pulsar-admin \
		namespaces set-retention public/$(GWPULSAR_NAMESPACE) --size 10G --time 2d

.PHONY: create-topics
create-topics:
	for i in $$(seq 0 $$(($(TOPICS) - 1))); do \
		kubectl -n pulsar exec -it pulsar-toolset-0 -- /pulsar/bin/pulsar-admin \
			topics create-partitioned-topic \
			persistent://public/$(GWPULSAR_NAMESPACE)/$(TOPIC_NAME)-$$i --partitions $(PARTITIONS) && \
		kubectl -n pulsar exec -it pulsar-toolset-0 -- /pulsar/bin/pulsar-admin \
			topics create-subscription \
			--subscription gwconsumer \
			persistent://public/$(GWPULSAR_NAMESPACE)/$(TOPIC_NAME)-$$i; \
	done


.PHONY: list-topics
list-topics:
	kubectl -n pulsar exec -it pulsar-toolset-0 -- /pulsar/bin/pulsar-admin \
		topics list-partitioned-topics public/$(GWPULSAR_NAMESPACE)

.PHONY: delete-topics
delete-topics:
	for i in $$(seq 0 $$(($(TOPICS) - 1))); do \
		kubectl -n pulsar exec -it pulsar-toolset-0 -- /pulsar/bin/pulsar-admin \
			topics delete-partitioned-topic persistent://public/$(GWPULSAR_NAMESPACE)/$(TOPIC_NAME)-$$i -f; \
	done

.PHONY: topic-stats
topic-stats:
	kubectl -n pulsar exec -it pulsar-toolset-0 -- /pulsar/bin/pulsar-admin \
		topics partitioned-stats persistent://public/$(GWPULSAR_NAMESPACE)/$(TOPIC_NAME)-0 \
		--per-partition

.PHONY: topics-stats
topics-stats:
	for i in $$(seq 0 $$(($(TOPICS) - 1))); do \
		kubectl -n pulsar exec -it pulsar-toolset-0 -- /pulsar/bin/pulsar-admin \
			topics partitioned-stats persistent://public/$(GWPULSAR_NAMESPACE)/$(TOPIC_NAME)-$$i \
			--per-partition; \
	done

.PHONY: pulsar-proxy
pulsar-proxy:
	kubectl -n pulsar port-forward pulsar-proxy-0 6650

.PHONY: gwpulsar-pf
gwpulsar-pf:
	kubectl -n $(GWPULSAR_NAMESPACE) port-forward \
		$$(kubectl -n fcasula get pods -l app=gwpulsar -o jsonpath='{.items[*].metadata.name}') \
		8080

.PHONY: gwpulsar-logs
gwpulsar-logs:
	kubectl -n $(GWPULSAR_NAMESPACE) logs --since=1h -l app=gwpulsar -f

.PHONY: kill
kill:
	kubectl -n pulsar delete pod pulsar-broker-$(BROKER) --force --grace-period=0
