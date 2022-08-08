FROM python
ENV KAFKA_HOST some_value
ENV KAFKA_TOPIC some_value

COPY consumer_analytics.py  consumer_analytics.py
COPY producer.py producer.py
COPY req.txt req.txt
COPY ds_salaries.csv ds_salaries.csv
COPY spark-sql-kafka-0-10_2.12-3.3.0.jar spark-sql-kafka-0-10_2.12-3.3.0.jar
RUN pip install -r req.txt

CMD [ "bash","run.sh" ]