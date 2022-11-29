FROM python:3.7.5-slim
WORKDIR /usr/src/app
COPY . .
RUN pip install -r requirements.txt -c constraints.txt
CMD ["python", "process_terms.py"]