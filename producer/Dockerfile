FROM python:3

RUN pip install pika
ENV PYTHONUNBUFFERED=1

WORKDIR /.
COPY writer.py .

CMD ["python", "writer.py"]