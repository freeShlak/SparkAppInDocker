#!/bin/bash
./gradlew createContainer && \
  cd build/docker && \
  docker start -i spark-labs