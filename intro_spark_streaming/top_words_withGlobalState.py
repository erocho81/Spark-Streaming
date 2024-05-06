import findspark
findspark.init()

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# Cambiad el username por vuestro nombre de usuario
sc = SparkContext("local[2]", "top_words_username")
sc.setLogLevel("ERROR")

ssc = StreamingContext(sc, 3)
ssc.checkpoint("checkpoint")

initialStateRDD = sc.parallelize([])


def updateFunc(new_values, last_sum):
    return sum(new_values) + (last_sum or 0)


# CUIDADO: Introducid el puerto que se os ha proporcionado.
netCatStream = ssc.socketTextStream("localhost", 40001)

running_counts = netCatStream.flatMap(lambda line: line.split(" "))\
    .map(lambda word: (word, 1))\
    .updateStateByKey(updateFunc, initialRDD=initialStateRDD)\
    .transform(lambda rdd: rdd.sortBy(lambda word: word[1], ascending=False)
               .zipWithIndex()
               .filter(lambda word: word[1] < 5)
               .map(lambda word: word[0]))


running_counts.pprint()

ssc.start()
ssc.awaitTermination()