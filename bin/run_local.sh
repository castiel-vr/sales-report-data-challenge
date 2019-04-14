#!/bin/bash

SPARK_HOME=/home/castiel-vr/DevSw/spark-2.4.1-bin-hadoop2.7

${SPARK_HOME}/bin/spark-submit \
  --class "it.castielvr.challenge.itemsales.ItemsSalesApp" \
  --master local[4] \
  target/scala-2.11/ItemsSales-SparkProcessing-assembly-0.1.jar \
  $@

# -c src/main/resources/application.conf \
# -s 20181211 \
# -e 20181211
