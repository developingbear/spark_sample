#FROM idock.daumkakao.io/bizrec/atom_metric:base as builder
ARG OPENJDK_TAG=11.0.13
FROM openjdk:${OPENJDK_TAG}

ARG SBT_VERSION=1.6.2

WORKDIR /app

RUN \
  mkdir /working/ && \
  cd /working/ && \
  curl -L -o sbt-$SBT_VERSION.deb https://repo.scala-sbt.org/scalasbt/debian/sbt-$SBT_VERSION.deb && \
  dpkg -i sbt-$SBT_VERSION.deb && \
  rm sbt-$SBT_VERSION.deb && \
  apt-get update && \
  apt-get install sbt && \
  cd && \
  rm -r /working/ && \
  sbt sbtVersion
#RUN #echo "deb https://dl.bintray.com/sbt/debian /" | tee -a /etc/apt/sources.list.d/sbt.list \
##    && apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 642AC823 \
#    apt-get install -y apt-transport-https \
#    && apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 2EE0EA64E40A89B84B2DF73499E82A75642AC823 \
#    && apt-get update \
#    && apt-get install sbt \
#    && apt-get install -y git

#RUN #curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | apt-key add  \
#    && apt-get update \
#    && apt-get install -y sbt \
#    && apt-get install -y git

WORKDIR /
RUN git clone https://github.com/developingbear/spark_sample.git
WORKDIR /spark_sample

RUN sbt clean assembly


#MAINTAINER aiden.song
#WORKDIR /atom_metric

# 소스코드 복사 / 빌드
#COPY . /atom_metric

# mvn 실행 시 메모리가 더 필요해서 설정
#ENV MAVEN_OPTS "-Xmx1024M -Xss128M -XX:MetaspaceSize=512M -XX:MaxMetaspaceSize=1024M -XX:+CMSClassUnloadingEnabled"
#RUN mvn -Dmaven.test.skip -T 1C -DskipTests package

#FROM idock.daumkakao.io/bizrec/bizrec-spark-doopey:khp
#MAINTAINER aiden.song
#WORKDIR /atom_metric
#
#COPY --from=builder /atom_metric/target/* ./target/
#
#COPY . /atom_metric

