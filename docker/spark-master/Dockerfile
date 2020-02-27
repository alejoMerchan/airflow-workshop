from "bde2020/spark-master:2.4.0-hadoop2.7"

RUN apk update && apk add libpq
RUN apk add --virtual .build-deps gcc python-dev musl-dev postgresql-dev
RUN pip install --upgrade pip && pip install boto3 && pip install beautifulsoup4 && pip install psycopg2