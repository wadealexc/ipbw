FROM python:3.8-alpine

LABEL maintainer="Dominik Muhs <dmuhs+ipbw@protonmail.ch>"

WORKDIR /ipbw
COPY requirements.txt .

RUN apk add --update postgresql-dev gcc python3-dev musl-dev make
RUN pip install -U pip
RUN pip install -r requirements.txt

COPY ipbw_server ipbw_server
COPY templates templates
COPY static static

COPY keys.json .

ENTRYPOINT ["python3", "-m", "uvicorn", "ipbw_server.server:app", "--host=0.0.0.0", "--port=8000"]
