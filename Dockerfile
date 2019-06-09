FROM python:3.7-alpine
WORKDIR /src/

COPY requirements.txt /tmp/

RUN apk add -t .build --no-cache postgresql-dev gcc musl-dev \
    && pip install --no-cache-dir -r /tmp/requirements.txt \
    && apk del --no-cache .build

COPY handlers.py .

USER 1001

ENTRYPOINT ["kopf", "run", "handlers.py"]
CMD ["--verbose"]
