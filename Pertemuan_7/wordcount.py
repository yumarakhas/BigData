from pyspark.sql import SparkSession
if __name__ == "__main__":
 spark = SparkSession.builder.appName("WordCount").getOrCreate()

 # Untuk versi membaca file
 # lines = spark.read.text("input.txt").rdd.map(lambda r: r[0])

 # Untuk versi data contoh
 data = ["Hello Spark", "Hello Docker", "Spark is awesome"]
 lines = spark.sparkContext.parallelize(data)

 counts = lines.flatMap(lambda x: x.split(' ')) \
 .map(lambda x: (x, 1)) \
 .reduceByKey(lambda a, b: a + b)

 output = counts.collect()
 for (word, count) in output:
    print("%s: %i" % (word, count))

 spark.stop()
