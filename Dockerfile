FROM python:3.13.1-slim
RUN apt-get -y update; apt-get -y install curl
RUN pip install asyncio fastapi prometheus_client toml uvicorn websockets
RUN mkdir /app