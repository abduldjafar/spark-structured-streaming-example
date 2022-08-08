FROM python
COPY consumer_analytics.py  consumer_analytics.py
COPY req.txt req.txt
COPY spark-sql-kafka-0-10_2.12-3.3.0.jar spark-sql-kafka-0-10_2.12-3.3.0.jar
RUN pip install -r req.txt