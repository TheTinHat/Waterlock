FROM python:latest

WORKDIR /app

COPY . .

ENTRYPOINT 'sh'