FROM python:3.12-alpine
RUN pip install --no-cache-dir flask==3.1.2 paho-mqtt==2.1.0
WORKDIR /app
COPY recorder.py /app/recorder.py
CMD ["python3", "-u", "/app/recorder.py"]
