FROM python:3.7.5-slim
WORKDIR /usr/src/app
RUN apt-get update && apt-get install -y procps && rm -rf /var/lib/apt/lists/*
COPY . .
RUN pip install -r requirements.txt -c constraints.txt
CMD ["python", "process_terms.py"]