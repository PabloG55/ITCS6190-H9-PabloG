# Hands-on L9: Report

**Name:** Pablo Garces
**Student ID:** 801377963
**Email:** <pgarces@charlotte.edu>

---

## Seed and commands

Seed used for `datagen.py`: `801377963`

The commands you ran, in order. If you deviated from the steps in the README, say where and
why.

```bash
python3 datagen.py 801377963

lsof -ti:8000 | xargs kill      #Freed port 8000, taken by another process.

docker compose up -d            #Failed: the H5 cluster was still running (see Problems).
cd ../ITCS6190-H5-PabloG && docker compose down
cd ../ITCS6190-H9-PabloG && docker compose up -d

docker cp main.py spark-master:/opt/spark/work-dir/
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/work-dir/main.py \
  /opt/spark/work-dir/shared/input \
  /opt/spark/work-dir/shared/output

#Repeated the docker cp + spark-submit above after finishing the four tasks.

docker exec -it spark-master /opt/spark/bin/pyspark --master spark://spark-master:7077

#Run some queries for the results analsies.

docker compose down
```

---

## Results

For each task, your output and one or two sentences on what it says about your data.

### Task 1: hashtag trends (DataFrame API)

```text
+---------+-----+
|hashtag  |count|
+---------+-----+
|#bug     |1571 |
|#love    |1536 |
|#ai      |1054 |
|#tech    |1034 |
|#darkmode|1016 |
|#update  |643  |
|#cleanui |636  |
|#ux      |627  |
|#mood    |618  |
|#social  |445  |
+---------+-----+
```

We can see how groups get form, in this case from the top 10 we have 3 groups the top ones 1.5k+,
the ones with 1k, and the rest. This could be very insigightful information to determine trending
topics.

How many distinct spellings did your most frequent hashtag have before `lower` and `trim`?

Only two. By running the same exact code but removing the `lower`, the `trim` and the `limit`,
I was able to get the data:

```text
+-------+-----+
|hashtag|count|
+-------+-----+
|#bug   |1372 |
|#love  |1292 |
|#LOVE  |244  |
|#BUG   |199  |
+-------+-----+
```

### Task 2: engagement by age group (Spark SQL)

```text
+---------+-----+---------+------------+
|age_group|posts|avg_likes|avg_retweets|
+---------+-----+---------+------------+
|Senior   |1504 |63.68    |15.47       |
|Adult    |1400 |50.22    |12.43       |
|Teen     |2096 |49.85    |12.58       |
+---------+-----+---------+------------+
```

We can see that the group that posts the most are the Teens, but surpringly they dont get as much
engagement as the Seniors. Adults and Teens get a similar amount of engament on their posts. With
these data you can conclude that posting more doesnt mean more engament.

### Task 3: sentiment versus engagement (Spark SQL)

```text
+---------+-----+---------+------------+
|sentiment|posts|avg_likes|avg_retweets|
+---------+-----+---------+------------+
|Negative |1087 |25.91    |6.47        |
|Neutral  |2277 |50.2     |12.39       |
|Positive |1636 |78.31    |19.42       |
+---------+-----+---------+------------+
```

We can see a clear difference between the different sentiments, in all fields, neutral and positive
post are more common and they end up having more engament than the negative ones.

Does sentiment pay on your data? Say it with the numbers.

Yes, positive posts have way more engament than the neutral and the negative ones, in both types,
likes and retweets. So in this case creating a positve post is more likely to have more activity
from users.

### Task 4: top verified users (DataFrame API)

```text
+---------------+-----------+--------------+-----+
|username       |total_likes|total_retweets|reach|
+---------------+-----------+--------------+-----+
|@techie39      |20453      |4876          |25329|
|@designer30    |11215      |2970          |14185|
|@daily_vibes3  |10685      |2907          |13592|
|@critic6       |10597      |2512          |13109|
|@pixel_pusher20|10450      |2489          |12939|
+---------------+-----------+--------------+-----+
```

The differnece from the top 1 and the others is large with almost twice likes as the second one.
Then it starts to getting flat and going down.

---

## Two languages, one plan

Paste the two `explain()` outputs of task 2, the SQL version and the DataFrame version:

SQL version:

```text
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- Sort [avg_likes#53 DESC NULLS LAST], true, 0
   +- Exchange rangepartitioning(avg_likes#53 DESC NULLS LAST, 200), ENSURE_REQUIREMENTS, [plan_id=697]
      +- HashAggregate(keys=[age_group#10], functions=[count(1), avg(likes#4), avg(retweets#5)])
         +- Exchange hashpartitioning(age_group#10, 200), ENSURE_REQUIREMENTS, [plan_id=694]
            +- HashAggregate(keys=[age_group#10], functions=[partial_count(1), partial_avg(likes#4), partial_avg(retweets#5)])
               +- Project [likes#4, retweets#5, age_group#10]
                  +- BroadcastHashJoin [user_id#1], [user_id#8], Inner, BuildRight, false, false
                     :- Filter isnotnull(user_id#1)
                     :  +- FileScan csv [user_id#1,likes#4,retweets#5] Batched: false, DataFilters: [isnotnull(user_id#1)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/opt/spark/work-dir/shared/input/posts.csv], PartitionFilters: [], PushedFilters: [IsNotNull(user_id)], ReadSchema: struct<user_id:string,likes:int,retweets:int>
                     +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, string, false]),false), [plan_id=689]
                        +- Filter isnotnull(user_id#8)
                           +- FileScan csv [user_id#8,age_group#10] Batched: false, DataFilters: [isnotnull(user_id#8)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/opt/spark/work-dir/shared/input/users.csv], PartitionFilters: [], PushedFilters: [IsNotNull(user_id)], ReadSchema: struct<user_id:string,age_group:string>
```

DataFrame version:

```text
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- Sort [avg_likes#133 DESC NULLS LAST], true, 0
   +- Exchange rangepartitioning(avg_likes#133 DESC NULLS LAST, 200), ENSURE_REQUIREMENTS, [plan_id=747]
      +- HashAggregate(keys=[age_group#10], functions=[count(1), avg(likes#4), avg(retweets#5)])
         +- Exchange hashpartitioning(age_group#10, 200), ENSURE_REQUIREMENTS, [plan_id=744]
            +- HashAggregate(keys=[age_group#10], functions=[partial_count(1), partial_avg(likes#4), partial_avg(retweets#5)])
               +- Project [likes#4, retweets#5, age_group#10]
                  +- BroadcastHashJoin [user_id#1], [user_id#8], Inner, BuildRight, false, false
                     :- Filter isnotnull(user_id#1)
                     :  +- FileScan csv [user_id#1,likes#4,retweets#5] Batched: false, DataFilters: [isnotnull(user_id#1)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/opt/spark/work-dir/shared/input/posts.csv], PartitionFilters: [], PushedFilters: [IsNotNull(user_id)], ReadSchema: struct<user_id:string,likes:int,retweets:int>
                     +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, string, false]),false), [plan_id=739]
                        +- Filter isnotnull(user_id#8)
                           +- FileScan csv [user_id#8,age_group#10] Batched: false, DataFilters: [isnotnull(user_id#8)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/opt/spark/work-dir/shared/input/users.csv], PartitionFilters: [], PushedFilters: [IsNotNull(user_id)], ReadSchema: struct<user_id:string,age_group:string>
```

Your comparison: are the two plans the same? Which operator is the join and which kind of
join did Spark choose, where are the `Exchange` operators and why are they needed, and what
does this tell you about choosing between SQL and the DataFrame API?

Yes, the two plans are the same operator by operator. The join is `BroadcastHashJoin ...
Inner, BuildRight`. Spark picked a broadcast join because `users.csv` is small, so it sends
a copy of the users table to every executor (`BroadcastExchange`). In this way, the posts don't
have to be shuffled for the join.

There are two `Exchange` operators. `Exchange hashpartitioning(age_group, 200)` is located
between the two `HashAggregate` steps, because each executor only has part of the data, so
rows with the same age group have to be moved to the same executor to finish the average.
`Exchange rangepartitioning(avg_likes DESC, 200)` is located before the final `Sort`, because
the rows have to be reorganized by range to sort them globally.

Both versions are identical, with no performance advantage for either one: SQL and DataFrames
both go through the Catalyst optimizer and end up with the same physical plan. So in this case
it does not matter which language you choose to use.

---

## Views

What did `createOrReplaceTempView("posts")` do, and what did it not do? Was any data copied?
What happens to the views when the program ends, and how would you keep a table for the
next session?

It just registered posts in the SparkSession catalog to be able to use spark.sql queries. It
doesnt do anything with the data it justs points to the same exact dataframe. This Temporary view
only lives as long as the SparkSession, so if it stops it will be gone. To save it we will have to use
 `df.write.saveAsTable("posts")` to actually write the data.

---

## Problems and fixes

Anything that went wrong and what resolved it. Paste the actual error message. If nothing
went wrong, say so.

Two things went wrong.

First, `docker compose up -d` failed because I had never stopped the cluster from Hands-on L5,
so its containers were still using the same names:

```text
Error response from daemon: Conflict. The container name "/spark-master" is already in use by container "f3bac861a65dedda9fbb419f3c23363dfdae0c02941727714b74b78f6ba1246c". You have to remove (or rename) that container to be able to reuse that name.
```

I went to the `ITCS6190-H5-PabloG` folder and ran `docker compose down`, which removed
it's containers. After that, `docker compose up -d` in this folder started all three
containers. Before that I had also freed port 8000 with `lsof -ti:8000 | xargs kill`
because I knew that another process was using it.

Second, my first run of `main.py` failed with a syntax error because I forgot a closing
parenthesis in one of the tasks:

```text
SyntaxError: '(' was never closed
```

I added the missing `)`, copied the file again with `docker cp` and re-ran `spark-submit`.
After that, all four tasks ran and the program finished with exit code 0, writing the four
outputs under `shared-folder/output/`.

---
