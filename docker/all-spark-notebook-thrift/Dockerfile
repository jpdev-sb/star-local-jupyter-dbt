FROM        jupyter/all-spark-notebook:latest

USER        root

ENV SPARK_HOME /usr/local/spark
ENV PATH="/usr/local/spark/bin:/usr/local/spark/sbin:${PATH}"
  
RUN apt-get update && \
    apt-get install -y libpostgresql-jdbc-java && \
    ln -s /usr/share/java/postgresql-jdbc4.jar /usr/local/spark/jars/postgresql-jdbc4.jar

USER        $NB_UID