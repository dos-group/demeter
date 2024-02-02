all: login build push

login:
	docker login;

build:
	cd analytics; docker build -t morgel/demeter_analytics:latest .

push:
	cd analytics; docker push morgel/demeter_analytics:latest
