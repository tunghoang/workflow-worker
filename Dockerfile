FROM pipeline:v2.5

RUN pip3 install --no-cache-dir datacube==1.8.19 terracotta==0.7.5 pymysql cryptography redis==4.4.2 rq==1.11
WORKDIR /workflow
ENTRYPOINT ["/tini", "--"]
CMD ["rq", "worker", "--url", "redis://redis:6379"]
