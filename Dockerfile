FROM python:3.8
WORKDIR /src/

COPY requirements.txt .

RUN sed -i 's/psycopg2-binary/psycopg2/' requirements.txt \
   && pip install --no-cache-dir -r requirements.txt \
   && chgrp -R 0 . && chmod g=u -R . \
   && chmod g=u /etc/passwd

COPY docker/root/ /

COPY handlers.py .
COPY lib lib

USER 1001

ENTRYPOINT ["/docker-entrypoint.sh"]
CMD ["kopf", "run", "--liveness", "http://0.0.0.0:8080/healthz", "handlers.py"]
