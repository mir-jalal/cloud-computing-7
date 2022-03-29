import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name


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