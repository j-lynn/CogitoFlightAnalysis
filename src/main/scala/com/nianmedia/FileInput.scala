package com.nianmedia
import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.sql.SparkSession

object FileInput {
  case class DP(Gforce: Double, AOA: Integer, IAS:Double)
     def main(args: Array[String]): Unit ={
       println("Inputing File Data...")
       Logger.getLogger("org").setLevel(Level.ERROR)
       val spark = SparkSession
         .builder
         .appName("FlightInput")
         .master("local[*]")
         .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
         .getOrCreate()

       // Read each line of my book into an Dataset
       import spark.implicits._
       val schemaDP = spark.read
         .option("header", "true")
         .option("inferSchema", "true")
         .csv("G:/My Drive/API/2024-06-13-N981LA-SN13651-16.4.0.9609-USER_LOG_DATA.csv")
         .as[DP]

       schemaDP.printSchema()

       schemaDP.createOrReplaceTempView("data")

       val filter = spark.sql("SELECT Gforce, IAS, AOA, Palt FROM data WHERE Palt < 1100 and Gforce > 1.2" )

       val results = filter.collect()

       println("G   IAS   AOA   P Alt")
       results.foreach(println)


       spark.stop()
     }
}
