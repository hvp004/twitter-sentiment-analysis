FROM python:3.8-slim-buster

WORKDIR /app

COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

RUN cd /app
RUN mkdir config
RUN mkdir module

COPY config /app/config
COPY module /app/module