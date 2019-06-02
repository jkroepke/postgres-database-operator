FROM python:3-alpine

WORKDIR /src
ADD . /src
RUN pip install -r requirements.txt
CMD kopf run --standalone handlers.py
