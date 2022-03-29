# Cloud Computing - Lab 7

We worked with the Apache Spark framework in Python

> Note: It takes more time to configure Python 3.7 and Java 8 than actual work in Lab 

## Exercise 7.3 
Source Code:
```python
import sys
from pyspark.sql import SparkSession


# Custom function for computing a sum.
# Inputs: a and b are values from two different RDD records/tuples.
def custom_sum(a, b):
    return a + b


def split_words(string):
    return "".join((char if char.isalpha() or char.isnumeric() else " ").lower() for char in string).split()


if __name__ == "__main__":
    # Check the number of arguments
    if len(sys.argv) != 2:
        print("Usage: wordcount <file>", file=sys.stderr)
        exit(-1)

    # Set a name for the application
    appName = "PythonWordCount"

    # Set the input folder location to the firsta rgument of the application
    # NB! sys.argv[0] is the path/name of the script file
    input_folder = sys.argv[1]

    # Create a new Spark application and get the Spark session object
    spark = SparkSession.builder.appName(appName).getOrCreate()

    # Get the spark context object.
    sc = spark.sparkContext

    # Load input RDD from the data folder
    lines = sc.textFile(input_folder)

    # Take 5 records from the RDD and print them out
    records = lines.take(5)
    for record in records:
        print(record)

    # Apply RDD operations to compute WordCount
    # lines RDD contains lines from the input files.
    # Lets split the lines into words and use flatMap operation to generate an RDD of words.
    words = lines.flatMap(lambda line: split_words(line))

    # Transform words into (word, 1) Key & Value tuples
    pairs = words.map(lambda word: (word, 1))

    # Apply reduceBy key to group pairs by key/word and apply sum operation on the list of values inside each group
    # Apply our of customSum function as the aggregation function, but we could also have used "lambda x,y: x+y" function
    counts = pairs.reduceByKey(custom_sum)

    # Read the data out of counts RDD
    # Output is a Python list (of (key, value) tuples)
    output = counts.collect()

    # Print each key and value tuple inside output list
    for (word, count) in output:
        print(word, count)

    # Stop Spark session. It is not required when running locally through PyCharm.
    #spark.sparkContext.stop()
    #spark.stop()
```

> Output is in text file: [`exercise-7-3`](output/exercise-7-3.txt)

## Exercise 7.5
Source Code:
```python
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name


# Custom function for computing a sum.
# Inputs: a and b are values from two different RDD records/tuples.
def custom_sum(a, b):
    return a+b


def split_words(string):
    return "".join((char if char.isalpha() or char.isnumeric() else " ").lower() for char in string).split()


def top5(records):
    top5_list = sorted(list(records), key=lambda x: x[1])[-5:]
    top5_list.reverse()
    return top5_list


if __name__ == "__main__":
    # Check the number of arguments
    if len(sys.argv) != 2:
        print("Usage: wordcount <file>", file=sys.stderr)
        exit(-1)

    # Set a name for the application
    appName = "PythonWordCount"

    # Set the input folder location to the firsta rgument of the application
    # NB! sys.argv[0] is the path/name of the script file
    input_folder = sys.argv[1]

    # Create a new Spark application and get the Spark session object
    spark = SparkSession.builder.appName(appName).getOrCreate()

    # Get the spark context object.
    sc = spark.sparkContext

    # Load input RDD from the data folder
    lines = spark.read.text(input_folder).select(input_file_name(), "value").rdd.map(tuple)

    # Take 5 records from the RDD and print them out
    records = lines.take(5)
    for record in records:
        print(record)

    # Apply RDD operations to compute WordCount
    # lines RDD contains lines from the input files.
    # Lets split the lines into words and use flatMap operation to generate an RDD of words.
    words = lines.flatMapValues(lambda line: split_words(line))

    # Transform words into (word, 1) Key & Value tuples
    pairs = words.map(lambda word: (word, 1))

    # Apply reduceBy key to group pairs by key/word and apply sum operation on the list of values inside each group
    # Apply our of customSum function as the aggregation function, but we could also have used "lambda x,y: x+y" function
    counts = pairs.reduceByKey(custom_sum).map(lambda filename_word_count: (filename_word_count[0][0],
                                                   (filename_word_count[0][1], filename_word_count[1])
                                                   )).groupByKey().mapValues(top5)

    # Read the data out of counts RDD
    # Output is a Python list (of (key, value) tuples)
    output = counts.collect()

    # Print each key and value tuple inside output list
    for (word, count) in output:
        print(word, count)

    # Stop Spark session. It is not required when running locally through PyCharm.
    #spark.sparkContext.stop()
    #spark.stop()
```

> Output is in text file: [`exercise-7-5`](output/exercise-7-5.txt)

