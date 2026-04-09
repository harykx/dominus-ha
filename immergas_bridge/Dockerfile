ARG BUILD_FROM
FROM $BUILD_FROM

RUN apk add --no-cache python3 py3-pip bash
RUN pip3 install --no-cache-dir paho-mqtt==2.1.0

COPY rootfs /
RUN chmod a+x /run.sh /app/immergas_bridge.py
CMD [ "/run.sh" ]
