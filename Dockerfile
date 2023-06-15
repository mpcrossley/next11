# syntax=docker/dockerfile:1.4
FROM python:3.10-slim

WORKDIR /app

COPY app/requirements.txt /app
RUN --mount=type=cache,target=/root/.cache/pip \
    pip3 install -r requirements.txt

COPY ./app/ /app

EXPOSE 5000
ENTRYPOINT ["python3"]
CMD ["app.py"]