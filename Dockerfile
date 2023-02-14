FROM --platform=linux/amd64 apache/airflow:2.3.3

USER root
# Install OpenJDK-11
RUN apt update && \
    apt-get install -y openjdk-11-jdk && \
    apt-get install -y ant && \
    apt-get clean;

# Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64/
RUN export JAVA_HOME
RUN apt-get install gcc libc-dev g++ libffi-dev libxml2 libffi-dev unixodbc-dev -y
RUN python -m pip install --upgrade pip

USER airflow


RUN pip install requests
RUN pip install pandas

RUN pip install apache-airflow-providers-jdbc[common.sql]
RUN pip install 'apache-airflow-providers-microsoft-mssql'
RUN pip install sqlalchemy-teradata
RUN pip install sqlalchemy-jdbcapi
RUN pip install teradatasqlalchemy