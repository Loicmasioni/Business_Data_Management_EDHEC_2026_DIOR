# Nom de l'image Docker
IMAGE_NAME=bigquery-project

build:
	docker build -t $(IMAGE_NAME) .

run:
	docker run -p 8888:8888 -v $(PWD):/app $(IMAGE_NAME)