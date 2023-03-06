#FROM idock.daumkakao.io/bizrec/atom_metric:base as builder
#MAINTAINER aiden.song
#WORKDIR /atom_metric
#
## 소스코드 복사 / 빌드
#COPY . /atom_metric
#
## mvn 실행 시 메모리가 더 필요해서 설정
#ENV MAVEN_OPTS "-Xmx1024M -Xss128M -XX:MetaspaceSize=512M -XX:MaxMetaspaceSize=1024M -XX:+CMSClassUnloadingEnabled"
#RUN mvn -Dmaven.test.skip -T 1C -DskipTests package
#
#FROM idock.daumkakao.io/bizrec/bizrec-spark-doopey:khp
#MAINTAINER aiden.song
#WORKDIR /atom_metric
#
#COPY --from=builder /atom_metric/target/* ./target/
#
#COPY . /atom_metric

