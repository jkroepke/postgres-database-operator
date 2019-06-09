FROM python:3-alpine
WORKDIR /src/

COPY requirements.txt ./

RUN apk add -t .build --no-cache postgresql-dev gcc musl-dev \
    && pip install --no-cache-dir -r requirements.txt \
    && apk del --no-cache .build

COPY . .

USER 1001

ENTRYPOINT ["kopf", "run", "handlers.py"]
CMD ["--verbose"]
