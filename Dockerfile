FROM python:3.6

RUN apt-get update && apt-get install -y \
    nano \
    screen

COPY requirements.txt /
RUN pip install -r requirements.txt

ADD birdspider /