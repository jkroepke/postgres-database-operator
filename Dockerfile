FROM python:3.7
WORKDIR /src/

COPY requirements.txt /tmp/

RUN apt-get update && apt-get install libpq-dev -y \
   && sed -i 's/psycopg2-binary/psycopg2/' /tmp/requirements.txt \
   && pip install --no-cache-dir -r /tmp/requirements.txt \
   && apt-get remove libpq-dev -y && rm -rf /var/lib/apt/lists/*

COPY handlers.py .

USER 1001

ENTRYPOINT ["kopf", "run", "handlers.py"]
CMD ["--verbose"]
