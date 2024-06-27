package com.nianmedia
import org.apache.log4j.{Level, Logger}
import org.apache.spark._

object FileInput {
  case class Flight(value: String)
     def main(args: Array[String]): Unit ={
       println("Inputing File Data...")
       Logger.getLogger("org").setLevel(Level.ERROR)
       val sc = new SparkContext("local[*]", "HelloWorld")

       val lines = sc.textFile("data/csv_import_small_sample.csv")
       val numLines = lines.count()

       println("Hello! The data file has " + numLines + " lines.")

       sc.stop()
     }
}
