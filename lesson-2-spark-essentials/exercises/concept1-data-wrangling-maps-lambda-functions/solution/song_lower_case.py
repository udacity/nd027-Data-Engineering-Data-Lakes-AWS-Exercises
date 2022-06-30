### 
# You might have noticed this code in the screencast.
#
# import findspark
# findspark.init('spark-2.3.2-bin-hadoop2.7')
#
# The findspark Python module makes it easier to install
# Spark in local mode on your computer. This is convenient
# for practicing Spark syntax locally. 
# However, the workspaces already have Spark installed and you do not
# need to use the findspark module
#
###

import pyspark
sc = pyspark.SparkContext(appName="maps_and_lazy_evaluation_example")

# Starting off with a regular python list
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
# distributed_song_log is an RDD (Reslient Distributed Dataset)
distributed_song_log = sc.parallelize(log_of_songs)

def convert_song_to_lowercase(song):
    return song.lower()

convert_song_to_lowercase("Havana")

distributed_song_log.map(convert_song_to_lowercase)


# collect() Converts from an RDD to a list- this is an expensive operation, because it requires gathering all the data from all the nodes
lower_case_songs=distributed_song_log.map(convert_song_to_lowercase).collect()
print(lower_case_songs)

# collect() Converts from an RDD to a list - we only do this if the data is very small (ex: a list of 9 songs)
distributed_song_log.collect()

distributed_song_log.map(lambda song: song.lower()).collect()
distributed_song_log.map(lambda x: x.lower()).collect()