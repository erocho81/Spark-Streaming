import json
import findspark
import re
findspark.init()

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.flume import FlumeUtils


sc = SparkContext(appName="FlumeStreaming", master="local[2]")
sc.setLogLevel("ERROR")

<FILL IN>

ssc.start()
ssc.awaitTermination()