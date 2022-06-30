# Maps
In Spark, maps take data as input and then transform that data with whatever function you put in the map. They are like directions for the data telling how each input should get to the output.

The first code section creates a SparkContext object. With the SparkContext, you can input a dataset and parallelize the data across a cluster (since you are currently using Spark in local mode on a single machine, technically the dataset isn't distributed yet).

It also instantiates a SparkContext object and then reads in the log_of_songs list into Spark.


```
import pyspark
sc = pyspark.SparkContext(appName="maps_and_lazy_evaluation_example")

log_of_songs = [
        "Despacito",
        "Nice for what",
        "No tears left to cry",
        "Despacito",
        "Havana",
        "In my feelings",
        "Nice for what",
        "despacito",
        "All the stars"
]

# parallelize the log_of_songs to use with Spark
distributed_song_log = sc.parallelize(log_of_songs)


```

The next code section defines a function that converts a song title to lowercase. Then there is an example converting the word "Havana" to "havana".

```
def convert_song_to_lowercase(song):
    return song.lower()

convert_song_to_lowercase("Havana")
```

The following code sections demonstrate how to apply this function using a map step. The map step will go through each song in the list and apply the convert_song_to_lowercase() function.

```
distributed_song_log.map(convert_song_to_lowercase)
```
You may notice that this code ran quite quickly. This is because of lazy evaluation. Spark does not actually execute the map step unless it needs to.

"RDD" in the output refers to resilient distributed dataset. RDDs are exactly what they say they are: fault-tolerant datasets distributed across a cluster. This is how Spark stores data.

To get Spark to actually run the map step, you need to use an "action". One available action is the collect method. The collect() method takes the results from all of the clusters and "collects" them into a single list on the master node.

```
distributed_song_log.map(convert_song_to_lowercase).collect()
```

Note as well that Spark is not changing the original data set: Spark is merely making a copy. You can see this by running collect() on the original dataset.

```
distributed_song_log.collect()
```

You do not always have to write a custom function for the map step. You can also use anonymous (lambda) functions as well as built-in Python functions like string.lower().

Anonymous functions are actually a Python feature for writing functional style programs.

```
distributed_song_log.map(lambda song: song.lower()).collect()
distributed_song_log.map(lambda x: x.lower()).collect()
```

