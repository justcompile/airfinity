FROM python:2.7-alpine

WORKDIR /app

COPY requirements.txt /app

RUN pip install -r /app/requirements.txt

COPY *.py /app/

COPY my_events /app/my_events/

ENTRYPOINT ["python"]

CMD ["python", "consumer.py"]
