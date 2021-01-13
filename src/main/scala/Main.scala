import org.apache.spark.sql.types.{CalendarIntervalType, DateType, FloatType, IntegerType, LongType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.functions.{col, hour, minute, second, from_unixtime, to_date, unix_timestamp}

object Main {
  def main(args: Array[String]) = {
    val spark = org.apache.spark.sql.SparkSession.builder
      .master("local") //# Change it as per your cluster
      .appName("Spark CSV Reader")
      .getOrCreate;

    val sc = new StructType()
          .add(StructField("VendorID", IntegerType, false))
          .add(StructField("tpep_pickup_datetime", TimestampType, false))
          .add(StructField("tpep_dropoff_datetime", TimestampType, false))
          .add(StructField("passenger_count", IntegerType, false))
          .add(StructField("trip_distance", FloatType, false))
          .add(StructField("RatecodeID", IntegerType, false))
          .add(StructField("store_and_fwd_flag", StringType, false))
          .add(StructField("PULocationID", StringType, false))
          .add(StructField("DOLocationID", StringType, false))
          .add(StructField("payment_type", StringType, false))
          .add(StructField("fare_amount", StringType, false))
          .add(StructField("extra", StringType, false))
          .add(StructField("mta_tax", StringType, false))
          .add(StructField("tip_amount", StringType, false))
          .add(StructField("tolls_amount", StringType, false))
          .add(StructField("improvement_surcharge", StringType, false))
          .add(StructField("total_amount", StringType, false))
          .add(StructField("congestion_surcharge", StringType, false))


    val taxiDataFrame = spark.read.option("header", "true").schema(sc).csv("yellow_tripdata_2019-09.csv"/*"smaller.csv"*/)/*.limit(10)*/
    val selectedColumns  = taxiDataFrame.select("tpep_pickup_datetime", "tpep_dropoff_datetime")
    
    val allSelectedColumns = taxiDataFrame.select("tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count", "trip_distance", "fare_amount", "tip_amount")

    val timeFmt = "yyyy-MM-dd'T'HH:mm:ss.SSS"

    val columnsWithTripDuration = selectedColumns
       .withColumn("trip_duration",
        ((unix_timestamp(col("tpep_dropoff_datetime"), timeFmt) -
        unix_timestamp(col("tpep_pickup_datetime"), timeFmt)) / 60).cast(IntegerType))

    /*columnsWithTripDuration.write.format("parquet").save("/data/df")
    val df = spark.read.parquet("/data/df/")*/

    val c = columnsWithTripDuration.cache()

    c.select("trip_duration").groupBy("trip_duration").count().orderBy(desc("count")).show(5)

    allSelectedColumns.describe().show()

    spark.stop()
  }
}
