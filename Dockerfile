FROM python:3.7
WORKDIR /src/

COPY requirements.txt .

RUN sed -i 's/psycopg2-binary/psycopg2/' requirements.txt \
   && pip install --no-cache-dir -r requirements.txt \
   && chgrp -R 0 . && chmod g=u -R .

COPY handlers.py .

USER 1001

ENTRYPOINT ["kopf", "run", "handlers.py"]
CMD ["--verbose"]
