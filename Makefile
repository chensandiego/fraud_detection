.PHONY: up down build train produce consume api logs clean

up:
	docker compose up -d --build

down:
	docker compose down

build:
	docker compose build

train:
	# starts trainer once (trainer service runs trainer.py then exits)
	docker compose up --no-deps --build trainer

produce:
	# run producer in foreground
	docker compose up --no-deps --build producer

consume:
	# run consumer in foreground
	docker compose up --no-deps --build consumer

api:
	docker compose up --no-deps --build api

logs:
	docker compose logs -f

clean:
	docker compose down -v --remove-orphans
	rm -f trainer/model.joblib