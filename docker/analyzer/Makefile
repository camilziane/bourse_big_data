macfast: Dockerfile
	gtar --transform 's|^analyzer/||' -czvf apps.tgz analyzer/*
	docker build -t my_analyzer .

macall: Dockerfile
	gtar --transform 's|^analyzer/||' -czvf apps.tgz analyzer/*
	docker build --no-cache -t my_analyzer .

fast: Dockerfile
	tar --transform 's|^analyzer/||' -czvf apps.tgz analyzer/*
	docker build -t my_analyzer .

all: Dockerfile
	tar --transform 's|^analyzer/||' -czvf apps.tgz analyzer/*
	docker build --no-cache -t my_analyzer .