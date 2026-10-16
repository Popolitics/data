FROM kestra/kestra:latest

WORKDIR /app

COPY src/ /app/src/
COPY job/ /app/job/
