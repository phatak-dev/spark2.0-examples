package com.madhukaraphatak.examples.sparktwo.ml

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{OneHotEncoderEstimator, StringIndexer}
import org.apache.spark.sql.SparkSession

object MultiColumnTransformation {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder.
      master("local")
      .appName("example")
      .getOrCreate()


    val salaryDf = sparkSession.read.format("csv").option("header", "true").load("src/main/resources/adult.csv")

    val stringColumns = Array("workclass", "occupation", "sex")

    val outputColumns = stringColumns.map(_ + "_onehot")

    val indexers = stringColumns.map(column => {
      val indexer = new StringIndexer()
      indexer.setInputCol(column)
      indexer.setOutputCol(column + "_index")
    })

    val singleOneHotEncoder = new OneHotEncoderEstimator()
    singleOneHotEncoder.setInputCols(stringColumns.map(_ + "_index"))
    singleOneHotEncoder.setOutputCols(outputColumns)

    val pipeline = new Pipeline()
    pipeline.setStages(indexers ++ Array(singleOneHotEncoder))

    val outputDf = pipeline.fit(salaryDf).transform(salaryDf)

    outputDf.select(outputColumns.head, outputColumns.tail: _*).show()

  }
}
