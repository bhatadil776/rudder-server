GWPULSAR_NAMESPACE := fcasula
REGISTRY_USERNAME := fracasula
IMAGE_NAME := gwproxy
DOCKER_REGISTRY := docker.io
IMAGE_NAME := $(DOCKER_REGISTRY)/$(REGISTRY_USERNAME)/$(IMAGE_NAME)

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
	kubectl -n $(GWPULSAR_NAMESPACE) apply -f deployment.yaml

.PHONY: undeploy
undeploy:
	kubectl -n $(GWPULSAR_NAMESPACE) delete -f deployment.yaml

.PHONY: logs
logs:
	kubectl -n $(GWPULSAR_NAMESPACE) logs -l app=gwproxy -f
