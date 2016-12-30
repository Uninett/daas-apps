FROM gurvin/spark-base:2.0.1
MAINTAINER Pål Karlsrud <paal@128.no>

ENV PIPELINE_RUNNER pipeline-runner-1.0.jar
ENV PIPELINE_RUNNER_SHA256 5dd0be409c99d1ea10b949e649371b0e98c396ce00663cebbb4bd58f3cb386cf

ENV TEST_DATA test_data.tar.gz
ENV TEST_DATA_SHA256 9ae9e48241e3a08a354d0d9514071fae1c964a84f49473fe1772e2dac8189dd0

RUN wget -q "https://f.128.no/genomics/${PIPELINE_RUNNER}" && \
    echo "${PIPELINE_RUNNER_SHA256} ${PIPELINE_RUNNER}" | sha256sum -c - && \
    mv ${PIPELINE_RUNNER} /usr/local/bin/${PIPELINE_RUNNER} && \
    chmod +x /usr/local/bin/${PIPELINE_RUNNER}

RUN wget -q "https://f.128.no/genomics/${TEST_DATA}" && \
    echo "${TEST_DATA_SHA256} ${TEST_DATA}" | sha256sum -c - && \
    mv ${TEST_DATA} /usr/local/share/${TEST_DATA} && \
    tar xvf /usr/local/share/${TEST_DATA} -C /usr/local/share/

ENTRYPOINT ["tini", "--", "spark-submit", "--conf",  "spark.task.cpus=4", "--class", "Runner", "/usr/local/bin/pipeline-runner-1.0.jar"]
