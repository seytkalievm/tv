FROM python:3

RUN pip install pika
ENV PYTHONUNBUFFERED=1

WORKDIR /.
COPY receiver.py .

CMD ["python", "receiver.py"]