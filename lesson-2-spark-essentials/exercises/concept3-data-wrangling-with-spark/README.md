
# Data Wrangling with Spark

Step through the code in the debugger to understand what the code does and how it works.

These first section imports libraries, instantiates a SparkSession, and then reads in the data set


```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import desc
from pyspark.sql.functions import asc
from pyspark.sql.functions import sum as Fsum

import datetime

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
```


```python
spark = SparkSession \
    .builder \
    .appName("Wrangling Data") \
    .getOrCreate()
```


```python
path = "../../data/sparkify_log_small.json"
user_log = spark.read.json(path)
```

# Data Exploration 

The next cells explore the data set.


```python
user_log.take(5)
```




    [Row(artist='Showaddywaddy', auth='Logged In', firstName='Kenneth', gender='M', itemInSession=112, lastName='Matthews', length=232.93342, level='paid', location='Charlotte-Concord-Gastonia, NC-SC', method='PUT', page='NextSong', registration=1509380319284, sessionId=5132, song='Christmas Tears Will Fall', status=200, ts=1513720872284, userAgent='"Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1985.125 Safari/537.36"', userId='1046'),
     Row(artist='Lily Allen', auth='Logged In', firstName='Elizabeth', gender='F', itemInSession=7, lastName='Chase', length=195.23873, level='free', location='Shreveport-Bossier City, LA', method='PUT', page='NextSong', registration=1512718541284, sessionId=5027, song='Cheryl Tweedy', status=200, ts=1513720878284, userAgent='"Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1985.143 Safari/537.36"', userId='1000'),
     Row(artist='Cobra Starship Featuring Leighton Meester', auth='Logged In', firstName='Vera', gender='F', itemInSession=6, lastName='Blackwell', length=196.20526, level='paid', location='Racine, WI', method='PUT', page='NextSong', registration=1499855749284, sessionId=5516, song='Good Girls Go Bad (Feat.Leighton Meester) (Album Version)', status=200, ts=1513720881284, userAgent='"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit/537.78.2 (KHTML, like Gecko) Version/7.0.6 Safari/537.78.2"', userId='2219'),
     Row(artist='Alex Smoke', auth='Logged In', firstName='Sophee', gender='F', itemInSession=8, lastName='Barker', length=405.99465, level='paid', location='San Luis Obispo-Paso Robles-Arroyo Grande, CA', method='PUT', page='NextSong', registration=1513009647284, sessionId=2372, song="Don't See The Point", status=200, ts=1513720905284, userAgent='"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1985.143 Safari/537.36"', userId='2373'),
     Row(artist=None, auth='Logged In', firstName='Jordyn', gender='F', itemInSession=0, lastName='Jones', length=None, level='free', location='Syracuse, NY', method='GET', page='Home', registration=1513648531284, sessionId=1746, song=None, status=200, ts=1513720913284, userAgent='"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2062.94 Safari/537.36"', userId='1747')]




```python
user_log.printSchema()
```

    root
     |-- artist: string (nullable = true)
     |-- auth: string (nullable = true)
     |-- firstName: string (nullable = true)
     |-- gender: string (nullable = true)
     |-- itemInSession: long (nullable = true)
     |-- lastName: string (nullable = true)
     |-- length: double (nullable = true)
     |-- level: string (nullable = true)
     |-- location: string (nullable = true)
     |-- method: string (nullable = true)
     |-- page: string (nullable = true)
     |-- registration: long (nullable = true)
     |-- sessionId: long (nullable = true)
     |-- song: string (nullable = true)
     |-- status: long (nullable = true)
     |-- ts: long (nullable = true)
     |-- userAgent: string (nullable = true)
     |-- userId: string (nullable = true)
    



```python
user_log.describe().show()
```

    +-------+-----------------+----------+---------+------+------------------+--------+-----------------+-----+------------+------+-------+--------------------+------------------+--------+------------------+--------------------+--------------------+------------------+
    |summary|           artist|      auth|firstName|gender|     itemInSession|lastName|           length|level|    location|method|   page|        registration|         sessionId|    song|            status|                  ts|           userAgent|            userId|
    +-------+-----------------+----------+---------+------+------------------+--------+-----------------+-----+------------+------+-------+--------------------+------------------+--------+------------------+--------------------+--------------------+------------------+
    |  count|             8347|     10000|     9664|  9664|             10000|    9664|             8347|10000|        9664| 10000|  10000|                9664|             10000|    8347|             10000|               10000|                9664|             10000|
    |   mean|            461.0|      null|     null|  null|           19.6734|    null|249.6486587492503| null|        null|  null|   null|1.504695369588739...|         4436.7511|Infinity|          202.8984|  1.5137859954164E12|                null|1442.4413286423842|
    | stddev|            300.0|      null|     null|  null|25.382114916132597|    null|95.00437130781461| null|        null|  null|   null|  8.47314252131657E9|2043.1281541827557|     NaN|18.041791154505876|3.2908288623601213E7|                null| 829.8909432082613|
    |    min|              !!!|     Guest|   Aakash|     F|                 0| Acevedo|          1.12281| free|Aberdeen, WA|   GET|  About|       1463503881284|                 9|      #1|               200|       1513720872284|"Mozilla/5.0 (Mac...|                  |
    |    max|ÃÂlafur Arnalds|Logged Out|     Zoie|     M|               163|  Zuniga|        1806.8371| paid|    Yuma, AZ|   PUT|Upgrade|       1513760702284|              7144|wingless|               404|       1513848349284|Mozilla/5.0 (comp...|               999|
    +-------+-----------------+----------+---------+------+------------------+--------+-----------------+-----+------------+------+-------+--------------------+------------------+--------+------------------+--------------------+--------------------+------------------+
    



```python
user_log.describe("artist").show()
```

    +-------+-----------------+
    |summary|           artist|
    +-------+-----------------+
    |  count|             8347|
    |   mean|            461.0|
    | stddev|            300.0|
    |    min|              !!!|
    |    max|ÃÂlafur Arnalds|
    +-------+-----------------+
    



```python
user_log.describe("sessionId").show()
```

    +-------+------------------+
    |summary|         sessionId|
    +-------+------------------+
    |  count|             10000|
    |   mean|         4436.7511|
    | stddev|2043.1281541827557|
    |    min|                 9|
    |    max|              7144|
    +-------+------------------+
    



```python
user_log.count()
```




    10000




```python
user_log.select("page").dropDuplicates().sort("page").show()
```


```python
user_log.select(["userId", "firstname", "page", "song"]).where(user_log.userId == "1046").collect()
```

# Calculating Statistics by Hour


```python
get_hour = udf(lambda x: datetime.datetime.fromtimestamp(x / 1000.0). hour)
```


```python
user_log = user_log.withColumn("hour", get_hour(user_log.ts))
```


```python
user_log.head()
```


```python
songs_in_hour = user_log.filter(user_log.page == "NextSong").groupby(user_log.hour).count().orderBy(user_log.hour.cast("float"))
```


```python
songs_in_hour.show()
```


```python
songs_in_hour_pd = songs_in_hour.toPandas()
songs_in_hour_pd.hour = pd.to_numeric(songs_in_hour_pd.hour)
```


```python
plt.scatter(songs_in_hour_pd["hour"], songs_in_hour_pd["count"])
plt.xlim(-1, 24);
plt.ylim(0, 1.2 * max(songs_in_hour_pd["count"]))
plt.xlabel("Hour")
plt.ylabel("Songs played");
```

# Drop Rows with Missing Values

As you'll see, it turns out there are no missing values in the userID or session columns. But there are userID values that are empty strings.


```python
user_log_valid = user_log.dropna(how = "any", subset = ["userId", "sessionId"])
```


```python
user_log_valid.count()
```


```python
user_log.select("userId").dropDuplicates().sort("userId").show()
```


```python
user_log_valid = user_log_valid.filter(user_log_valid["userId"] != "")
```


```python
user_log_valid.count()
```

# Users Downgrade Their Accounts

Find when users downgrade their accounts and then flag those log entries. Then use a window function and cumulative sum to distinguish each user's data as either pre or post downgrade events.


```python
user_log_valid.filter("page = 'Submit Downgrade'").show()
```


```python
user_log.select(["userId", "firstname", "page", "level", "song"]).where(user_log.userId == "1138").collect()
```


```python
flag_downgrade_event = udf(lambda x: 1 if x == "Submit Downgrade" else 0, IntegerType())
```


```python
user_log_valid = user_log_valid.withColumn("downgraded", flag_downgrade_event("page"))
```


```python
user_log_valid.head()
```


```python
from pyspark.sql import Window
```


```python
windowval = Window.partitionBy("userId").orderBy(desc("ts")).rangeBetween(Window.unboundedPreceding, 0)
```


```python
user_log_valid = user_log_valid.withColumn("phase", Fsum("downgraded").over(windowval))
```


```python
user_log_valid.select(["userId", "firstname", "ts", "page", "level", "phase"]).where(user_log.userId == "1138").sort("ts").collect()
```
