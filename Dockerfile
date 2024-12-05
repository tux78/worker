FROM python:3.12-slim-bookworm

WORKDIR /worker

COPY worker/worker.py worker/parser.py worker/tokenizer.so /worker/
COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

ENTRYPOINT ["python3", "-u", "worker.py"]
