#基础镜像
FROM centos:centos7

#将jdk8包放入/usr/local/src并自动解压，jdk8.tar.gz 需要到oracle官方下载，注意解压后的java版本号
ADD jdk8.tar.gz /usr/local/src
COPY tailbaseSampling-1.0-SNAPSHOT.jar /usr/local/src
WORKDIR /usr/local/src
COPY start.sh /usr/local/src
RUN chmod +x /usr/local/src/start.sh
ENTRYPOINT ["/bin/bash", "/usr/local/src/start.sh"]