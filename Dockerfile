FROM apache/nifi:latest

ENV SINGLE_USER_CREDENTIALS_USERNAME=admin
ENV SINGLE_USER_CREDENTIALS_PASSWORD=ctsBtRBKHRAx69EqUghvvgEvjnaLjFEB

EXPOSE 8443

VOLUME ["/opt/nifi/nifi-files"]

CMD ["bin/nifi.sh", "run"]