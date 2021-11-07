FROM python:latest

WORKDIR /app

COPY . .

RUN pip install --upgrade pip
RUN pip install SQLAlchemy

ENTRYPOINT 'sh'