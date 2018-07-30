from __future__ import print_function

import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: kafka_wordcount.py <zk> <topic>", file=sys.stderr)
        exit(-1)

    sc = SparkContext(appName="PythonStreamingKafkaWordCount")
    sc.setLogLevel("OFF")
    ssc = StreamingContext(sc, 1)

    zkQuorum, topic = sys.argv[1:]
    kvs = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {topic: 1})
    lines = kvs.map(lambda x: x[1])
    counts = lines.flatMap(lambda line: line.split(" ")) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda a, b: a+b)

    counts.pprint()
    # print (lines)

    ssc.start()
    ssc.awaitTermination()


# gamiecocharm.vn 171.233.24.128 [27/Jul/2018:15:27:49 +0700] "GET /?utm_source=UREKA_GDN&utm_medium=DuAn&utm_term=CPC&utm_content=17Jul HTTP/1.1" 200 15816 "-" "Mozilla/5.0 (Linux; Android 6.0; Seoul S6 Build/MRA58K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.87 Mobile Safari/537.36" "beto-page.novalocal" 0.115 0.115
