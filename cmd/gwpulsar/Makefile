REGISTRY_USERNAME := fracasula
IMAGE_NAME := pulsar-exp
DOCKER_REGISTRY := docker.io
IMAGE_VERSION := $(shell date +%Y%m%d%H%M%S)
IMAGE_NAME := $(DOCKER_REGISTRY)/$(REGISTRY_USERNAME)/$(IMAGE_NAME)
TAGGED_IMAGE_NAME := $(IMAGE_NAME):$(IMAGE_VERSION)

.PHONY: build
build:
	docker build -t $(IMAGE_NAME) .

.PHONY: push
push: build
	docker tag $(IMAGE_NAME) $(TAGGED_IMAGE_NAME)
	docker push $(TAGGED_IMAGE_NAME)

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
