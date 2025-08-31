FROM docker.io/bitnami/spark:3.5.0

USER root



RUN apt-get update && apt-get install -y curl

# RUN curl -O https://repo1.maven.org/maven2/software/amazon/awssdk/s3/2.18.41/s3-2.18.41.jar \
#     && curl -O https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk/1.12.367/aws-java-sdk-1.12.367.jar \
#     && curl -O https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.2.0/delta-core_2.12-2.2.0.jar \
#     && curl -O https://repo1.maven.org/maven2/io/delta/delta-storage/2.2.0/delta-storage-2.2.0.jar \
#     && curl -O https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.19/mysql-connector-java-8.0.19.jar \
#     && curl -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
#     && curl -O https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar\
#     && curl -O https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar\
#     && curl -O https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.3.2/kafka-clients-3.3.2.jar\
#     && curl -O https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.3.2/spark-sql-kafka-0-10_2.12-3.3.2.jar\
#     && curl -O https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.3.2/spark-token-provider-kafka-0-10_2.12-3.3.2.jar\
#     && curl -O https://repo1.maven.org/maven2/com/johnsnowlabs/nlp/spark-nlp_2.12/5.5.3/spark-nlp_2.12-5.5.3.jar\
#     && curl -O https://s3.amazonaws.com/auxdata.johnsnowlabs.com/public/jars/spark-nlp-assembly-5.5.3.jar\
#     && mv commons-pool2-2.11.1.jar /opt/bitnami/spark/jars \
#     && mv kafka-clients-3.3.2.jar /opt/bitnami/spark/jars \
#     && mv spark-sql-kafka-0-10_2.12-3.3.2.jar /opt/bitnami/spark/jars \
#     && mv spark-token-provider-kafka-0-10_2.12-3.3.2.jar /opt/bitnami/spark/jars \
#     && mv s3-2.18.41.jar /opt/bitnami/spark/jars \
#     && mv aws-java-sdk-1.12.367.jar /opt/bitnami/spark/jars \
#     && mv delta-core_2.12-2.2.0.jar /opt/bitnami/spark/jars \
#     && mv delta-storage-2.2.0.jar /opt/bitnami/spark/jars \
#     && mv mysql-connector-java-8.0.19.jar /opt/bitnami/spark/jars \
#     && mv aws-java-sdk-bundle-1.12.262.jar /opt/bitnami/spark/jars \
#     && mv hadoop-aws-3.3.4.jar /opt/bitnami/spark/jars \
#     && mv spark-nlp_2.12-5.5.3.jar /opt/bitnami/spark/jars\
#     && mv spark-nlp-assembly-5.5.3.jar /opt/bitnami/spark/jars

