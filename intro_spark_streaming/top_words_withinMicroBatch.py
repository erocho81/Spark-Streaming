import findspark
findspark.init()

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# Cambiad el username por vuestro nombre de usuario
sc = SparkContext("local[2]", "top_words_username")
sc.setLogLevel("ERROR")

micro_batch_length = 5
ssc = StreamingContext(sc, micro_batch_length)
ssc.checkpoint("checkpoint")

initialStateRDD = sc.parallelize([])

# CUIDADO: Introducid el puerto que se os ha proporcionado.
netCatStream = ssc.socketTextStream("localhost", 40001)

running_counts = netCatStream.flatMap(lambda line: line.split(" "))\
    .filter(lambda word: len(word) and word[0].isupper())\
    .map(lambda word: (word, 1))\
    .reduceByKeyAndWindow(lambda w1,w2: w1+w2,5,5)

running_counts.pprint()

ssc.start()
ssc.awaitTermination()