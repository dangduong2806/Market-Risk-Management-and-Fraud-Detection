# Định nghĩa build, up và down
build:
	docker build -t spark-jobs:latest ./spark-jobs
	docker build -t train-job:latest ./spark-jobs
	docker build -t fraud-train:latest ./fraud-train

	docker-compose up -d --build
up:
	docker-compose up -d
down:
	docker-compose down -v