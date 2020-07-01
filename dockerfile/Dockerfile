#基础镜像
FROM registry.tbocii.tk:8000/ody123/java-tail-based-sampling/java

COPY tailbaseSampling-1.0-SNAPSHOT.jar /usr/local/src
WORKDIR /usr/local/src
COPY start.sh /usr/local/src
RUN chmod +x /usr/local/src/start.sh
ENTRYPOINT ["/bin/bash", "/usr/local/src/start.sh"]