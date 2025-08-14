FROM python:3.10-slim
WORKDIR /app
COPY ../requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
COPY scoring_consumer.py /app/scoring_consumer.py
COPY ../trainer/model.joblib /app/model.joblib
CMD ["python", "scoring_consumer.py"]