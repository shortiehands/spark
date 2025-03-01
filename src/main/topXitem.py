import pyspark
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import broadcast, rank, countDistinct
from pyspark.sql.window import Window


# Initialising a spark session
def process_data(spark, input_path, reference_path, output_path, topx):

    spark = SparkSession.builder.appName('TopXItemsByLocation').getOrCreate()

    def create_rdd(file_name):
        # Read parquet file
        df = spark.read.parquet(file_name)
        # Create RDD df
        rdd = df.rdd
        return rdd

    rdd = create_rdd(input_path)
    reference_rdd = create_rdd(reference_path)

    ref_rdd_kv = reference_rdd.map(lambda row: (row['geographical_location_oid'], row['geographical_location']))

    unique_rdd = rdd.map(lambda row: ((row['geographical_location_oid'], row['detection_oid']), row['item_name']), 1)\
                .reduceByKey(lambda a, b: a)\
                .map(lambda row: ((row[0][0], row[1]), 1)) 
    
    # Debugging statement
    print(f"Unique rdd \n {unique_rdd.collect()}")
    
    aggregated_rdd = unique_rdd.reduceByKey(lambda a, b: a + b)
    
    # Debugging statement
    print(f"aggregated rdd \n {aggregated_rdd.collect()}")
    
    # Sort items by total count in descending order
    sorted_rdd = aggregated_rdd.sortBy(lambda x: x[1], ascending=False)

# Debugging statement
    print(f"sorted rdd \n {sorted_rdd.collect()}")
    
    assign_rank = sorted_rdd.zipWithIndex()\
                    .map(lambda x: (x[0][0][0], x[0][0][1], x[1] + 1))
    
    # Debugging statement
    print(f"assign_rank \n {assign_rank.collect()}")

    assign_rank_kv = assign_rank.map(lambda x: (x[0], (x[1], x[2])))

    # Joining with reference RDD
    joined_rdd = assign_rank_kv.join(ref_rdd_kv)

    # Debugging statement
    print(f"joined rdd before mapping location \n {joined_rdd.collect()}")

    # Extracting location, rank and item name
    final_rdd = joined_rdd.map(lambda x: (x[1][1], x[1][0][1], x[1][0][0]))  

    # Debugging statement
    print(f"final rdd \n {final_rdd.collect()}")

    # Filter top x items
    top_x = final_rdd.filter(lambda x: x[1] <= topx)
    
    # Convert to DataFrame before writing Parquet
    df_final = top_x.map(lambda row: Row(geographical_location=row[0], item_rank=row[1], item_name=row[2])) \
                     .toDF() \
                     .orderBy("item_rank")
    
    # Write final result to Parquet
    df_final.write.mode("overwrite").parquet(output_path)

    return df_final.collect()
