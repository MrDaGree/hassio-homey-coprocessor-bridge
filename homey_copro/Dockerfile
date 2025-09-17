FROM node:20-alpine AS mqtt-builder

WORKDIR /w

RUN npm init -y && npm i mqtt zwave-js --omit=dev

FROM scratch

ADD homey-pro-rootfs.tar /

COPY --from=mqtt-builder /w/node_modules /opt/mqtt/node_modules

WORKDIR /app
COPY mqtt_bridge.mjs .

COPY run.sh /run.sh
RUN chmod +x /run.sh

ENV HOMEY_LOG_PATH="/data"
ENV HOMEY_COPROCESSOR_GPIO_RESET_PATH="/sys/class/gpio/gpio25/value"
ENV HOMEY_COPROCESSOR_GPIO_BOOT_PATH="/sys/class/gpio/gpio24/value"

ENTRYPOINT ["/run.sh"]