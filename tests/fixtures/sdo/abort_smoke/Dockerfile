ARG BASE_IMAGE=sdo-controller:l4
FROM ${BASE_IMAGE}
COPY .fastloop/sdo-operator /usr/local/bin/sdo-operator
RUN rm -rf /opt/sdo/sdo/operator
COPY sdo/operator /opt/sdo/sdo/operator
