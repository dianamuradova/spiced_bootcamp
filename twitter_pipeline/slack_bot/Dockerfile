#Dockerfile : instructions to build a container

#Docker image to start with: I want a Ubuntu system with Python installed

#equivalent to selecting an OS when starting an AWS server

FROM python:3.6-slim

#create a working directory inside a container
WORKDIR /app

COPY requirements.txt /app
RUN pip install --trusted-host pypi.python.org -r requirements.txt
ADD . /app

CMD ["python", "slack_bot.py"]
