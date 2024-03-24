fast:
	gtar --transform 's|^dashboard/||' -czvf apps.tgz dashboard/bourse.py dashboard/*/
	docker build -t my_dashboard .

all: Dockerfile
	gtar --transform 's|^dashboard/||' -czvf apps.tgz dashboard/bourse.py dashboard/*/
	docker build --no-cache -t my_dashboard .

