from pyspark.sql import SparkSession

def main() :
  text_file = sc.textFile("hdfs://...")
  counts = text_file.flatMap(lambda line: line.split(" ")) \
             .map(lambda word: (word, 1)) \
             .reduceByKey(lambda a, b: a + b)
  counts.saveAsTextFile("hdfs://...")

if __name__ == "__main__":
    main()
