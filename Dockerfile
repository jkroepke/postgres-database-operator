FROM python:3-alpine

ADD . /src
RUN pip install -r requirements.txt
CMD kopf run /src/handlers.py --verbose
