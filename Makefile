.DEFAULT_GOAL = all

REGION = us-east-1
BUCKET = skaushik0-14-848-hw-03-s3
DYNAMO = skaushik0-14-848-hw-03-db
SCRIPT = $(shell dirname $(realpath $(firstword $(MAKEFILE_LIST))))/script.py

all:
	$(SCRIPT)

clean:
	aws s3 rm --recursive s3://$(BUCKET)
	aws s3api delete-bucket --bucket $(BUCKET) --region $(REGION)
	aws dynamodb delete-table --table-name $(DYNAMO)

.PHONY: all clean
