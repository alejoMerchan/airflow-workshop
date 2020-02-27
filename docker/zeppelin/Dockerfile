FROM "apache/zeppelin:0.8.2"

RUN wget https://archive.apache.org/dist/spark/spark-2.4.0/spark-2.4.0-bin-hadoop2.7.tgz --progress=bar:force && \
    tar xvf spark-2.4.0-bin-hadoop2.7.tgz && \
    mkdir -p /usr/local/spark && \
    mv spark-2.4.0-bin-hadoop2.7/* /usr/local/spark/. && \
    mkdir -p /sample-data
	
RUN mkdir -p /data
RUN mkdir /usr/local/local-repo && chmod -R a+rw /usr/local/local-repo
RUN mkdir /zeppelin/local-repo && chmod -R a+rw /zeppelin/local-repo
RUN apt-get update && apt-get install nano
RUN pip install --upgrade pip && pip install boto3 && pip install beautifulsoup4 && pip install pydash
RUN apt-get install postgresql -y
RUN apt-get install python-dev -y && apt-get install libpq-dev -y && pip install psycopg2
RUN wget https://s3.amazonaws.com/redshift-downloads/drivers/jdbc/1.2.37.1061/RedshiftJDBC42-no-awssdk-1.2.37.1061.jar --progress=bar:force && \
    mkdir -p /zeppelin/redshift/driver/ && chmod -R a+rw /zeppelin/redshift/driver/ && \
    cp RedshiftJDBC42-no-awssdk-1.2.37.1061.jar  /zeppelin/redshift/driver/RedshiftJDBC42-no-awssdk-1.2.37.1061.jar && \
	cp /zeppelin/redshift/driver/RedshiftJDBC42-no-awssdk-1.2.37.1061.jar /zeppelin/interpreter/jdbc

ENV SPARK_HOME "/usr/local/spark/"