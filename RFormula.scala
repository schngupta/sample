package com.scalaapp
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.feature.RFormula
import org.apache.spark.sql.SparkSession


object RFormula {

  def main(args: Array[String]): Unit =
  {
    Logger.getLogger("org").setLevel(Level.ERROR)

    //Create a SparkContext to initialize Spark
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("CreditScoreFormula")
    val sc = new SparkContext(conf)

    val spark = SparkSession.builder().appName("Spark In Action").master("local").getOrCreate()

    val dataset = spark.createDataFrame(Seq(
      (7, "US", 18, 1),
      (8, "CA", 12, 0),(9, "NZ", 15, 0),(10, "NZ", 15, 1),(11, "NZ", 15, 0)
    )).toDF("id", "country", "hour", "clicked")

    val formula = new RFormula().setFormula("clicked ~ country + hour").setFeaturesCol("features").setLabelCol("label")
    val output = formula.fit(dataset).transform(dataset)
    output.show()
  }
}
