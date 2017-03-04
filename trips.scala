
import org.apache.spark.sql.types._
import org.apache.spark.sql.Column
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row

import org.apache.spark.sql.functions

def time[T](proc: => T): T = {
    val start=System.nanoTime()
    val res = proc // call the code
    val end = System.nanoTime()
    println("Time elapsed: " + (end-start)/1000 + " microsecs")
    println("Time elapsed: " + (end-start)/1000/1000 + " millisecs")
    println("Time elapsed: " + (end-start)/1000/1000/1000 + " secs")
    println("Time elapsed: " + (end-start)/1000/1000/1000/60 + " min")
    res
}

val schema = StructType(Array(
    StructField("VendorID", DataTypes.StringType,false),
    StructField("tpep_pickup_datetime", DataTypes.TimestampType,false),
    StructField("tpep_dropoff_datetime", DataTypes.TimestampType,false),
    StructField("passenger_count", DataTypes.IntegerType,false),
    StructField("trip_distance", DataTypes.DoubleType,false),
    StructField("pickup_longitude", DataTypes.DoubleType,false),
    StructField("pickup_latitude", DataTypes.DoubleType,false),
    StructField("RatecodeID", DataTypes.IntegerType,false),
    StructField("store_and_fwd_flag", DataTypes.StringType,false),
    StructField("dropoff_longitude", DataTypes.DoubleType,false),
    StructField("dropoff_latitude", DataTypes.DoubleType,false),
    StructField("payment_type", DataTypes.IntegerType,false),
    StructField("fare_amount", DataTypes.DoubleType,false),
    StructField("extra", DataTypes.DoubleType,false),
    StructField("mta_tax", DataTypes.DoubleType,false),
    StructField("tip_amount", DataTypes.DoubleType,false),
    StructField("tolls_amount", DataTypes.DoubleType,false),
    StructField("improvement_surcharge", DataTypes.DoubleType,false),
    StructField("total_amount", DataTypes.DoubleType, false)
))

val tripsDF = spark.read.schema(schema).option("header", true).csv("yellow_tripdata_2016-01.csv")
val trips = tripsDF.where($"pickup_longitude" =!= 0 && $"pickup_latitude" =!= 0 && $"dropoff_longitude" =!= 0 && $"dropoff_latitude" =!= 0).cache()

// STEP 1: Find max latitude  to use it to create buckets later, since longitude is depending on that

val max_latitude_row = trips.agg(max($"pickup_latitude"), max($"dropoff_latitude")).first()
val max_latitude = math.max(max_latitude_row.getDouble(0), max_latitude_row.getDouble(1))

// STEP 2: Create buckets

val EARTH_R = 6371
val CHUNK_SIZE = 0.101

def calcDistance(latitude1: Column, longitude1: Column, latitude2: Column, longitude2: Column): Column = {
  asin(sqrt(pow((toRadians(latitude2) - toRadians(latitude1)) / 2, 2.0) + cos(toRadians(latitude1)) * cos(toRadians(latitude2)) * pow((toRadians(longitude2) - toRadians(longitude1)) / 2, 2.0))) * 2 * EARTH_R
}

def calcLatitudeDelta(): Double = {
  math.toDegrees(2 * math.asin(math.sqrt(math.pow(CHUNK_SIZE / (2 * EARTH_R), 2.0))))
}

def calcLongitudeDelta(at_latitude: Double): Double = {
  math.toDegrees(2 * math.asin(math.sqrt(math.pow(CHUNK_SIZE / (2 * EARTH_R), 2.0) / math.pow(math.cos(math.toRadians(at_latitude)), 2.0))))
}

val latitude_bucket_delta = calcLatitudeDelta()
val longitude_bucket_delta = calcLongitudeDelta(max_latitude)

val bucket_trips = trips.withColumn("pickup_latitude_bucket", floor($"pickup_latitude" / latitude_bucket_delta)).withColumn("pickup_longitude_bucket", floor($"pickup_longitude" / longitude_bucket_delta)).withColumn("dropoff_latitude_bucket", floor($"dropoff_latitude" / latitude_bucket_delta)).withColumn("dropoff_longitude_bucket", floor($"dropoff_longitude" / longitude_bucket_delta))

// STEP 3: Explode Data

def explodeLatitude(ds: Dataset[Row]): Dataset[Row] = {
  val explZero = ds.withColumn("new_lat", $"dropoff_latitude_bucket").drop($"dropoff_latitude_bucket").withColumnRenamed("new_lat", "dropoff_latitude_bucket")
  val explMinus = ds.withColumn("new_lat", $"dropoff_latitude_bucket" - 1).drop($"dropoff_latitude_bucket").withColumnRenamed("new_lat", "dropoff_latitude_bucket")
  val explPlus = ds.withColumn("new_lat", $"dropoff_latitude_bucket" + 1).drop($"dropoff_latitude_bucket").withColumnRenamed("new_lat", "dropoff_latitude_bucket")
  explZero.union(explMinus).union(explPlus)
}

def explodeLongitude(ds: Dataset[Row]): Dataset[Row] = {
  val explZero = ds.withColumn("new_long", $"dropoff_longitude_bucket").drop($"dropoff_longitude_bucket").withColumnRenamed("new_long", "dropoff_longitude_bucket")
  val explMinus = ds.withColumn("new_long", $"dropoff_longitude_bucket" - 1).drop($"dropoff_longitude_bucket").withColumnRenamed("new_long", "dropoff_longitude_bucket")
  val explPlus = ds.withColumn("new_long", $"dropoff_longitude_bucket" + 1).drop($"dropoff_longitude_bucket").withColumnRenamed("new_long", "dropoff_longitude_bucket")
  explZero.union(explMinus).union(explPlus)
}

val exploded_trips = explodeLongitude(explodeLatitude(bucket_trips))

// STEP 4: Perform join

val result = exploded_trips.as("trip_there").join(exploded_trips.as("trip_back"), (
  $"trip_there.dropoff_latitude_bucket" === $"trip_back.pickup_latitude_bucket"
  && $"trip_there.dropoff_longitude_bucket" === $"trip_back.pickup_longitude_bucket"
  && $"trip_back.dropoff_latitude_bucket" === $"trip_there.pickup_latitude_bucket"
  && $"trip_back.dropoff_longitude_bucket" === $"trip_there.pickup_longitude_bucket"

  && ($"trip_there.tpep_dropoff_datetime".cast("long") < $"trip_back.tpep_pickup_datetime".cast("long"))
  && (($"trip_there.tpep_dropoff_datetime".cast("long") + (8 * 60 * 60)) > $"trip_back.tpep_pickup_datetime".cast("long"))
  && (calcDistance($"trip_there.dropoff_latitude", $"trip_there.dropoff_longitude", $"trip_back.pickup_latitude", $"trip_back.pickup_longitude") < 0.1)
  && (calcDistance($"trip_back.dropoff_latitude", $"trip_back.dropoff_longitude", $"trip_there.pickup_latitude", $"trip_there.pickup_longitude") < 0.1)
))

time(result.count())


// scala> time(result.count())
// Time elapsed: 277963817 microsecs
// Time elapsed: 277963 millisecs
// Time elapsed: 277 secs
// Time elapsed: 4 min
// res2: Long = 5964009
//
// scala> time(result.count())
// Time elapsed: 256241173 microsecs
// Time elapsed: 256241 millisecs
// Time elapsed: 256 secs
// Time elapsed: 4 min
// res21: Long = 5964009
