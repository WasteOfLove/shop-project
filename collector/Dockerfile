FROM python:3.12-slim
WORKDIR /app
RUN pip install --no-cache-dir pika requests
COPY main.py /app/main.py
CMD ["python", "main.py"]
