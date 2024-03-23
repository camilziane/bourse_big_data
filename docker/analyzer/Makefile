fast: Dockerfile
	gtar --transform 's|^analyzer/||' -czvf apps.tgz analyzer/*
	docker build -t my_analyzer .

all: Dockerfile
	gtar --transform 's|^analyzer/||' -czvf apps.tgz analyzer/*
	docker build --no-cache -t my_analyzer .

