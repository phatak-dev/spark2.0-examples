package com.madhukaraphatak.examples.sparktwo.ml

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{OneHotEncoder, OneHotEncoderEstimator, StringIndexer}
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

    // serial transformations
    val singleColumnOneHotEncoders = stringColumns.map(column => {
      val oneHotEncoder = new OneHotEncoder()
      oneHotEncoder.setInputCol(column+"_index")
      oneHotEncoder.setOutputCol(column+"_onehot")
      oneHotEncoder
    })

    val serialPipeLine = new Pipeline()
    serialPipeLine.setStages(indexers ++ singleColumnOneHotEncoders)
    val serialOutput = serialPipeLine.fit(salaryDf).transform(salaryDf)
    serialOutput.show()

    // multi column transformations

    val singleOneHotEncoder = new OneHotEncoderEstimator()
    singleOneHotEncoder.setInputCols(stringColumns.map(_ + "_index"))
    singleOneHotEncoder.setOutputCols(outputColumns)

    val pipeline = new Pipeline()
    pipeline.setStages(indexers ++ Array(singleOneHotEncoder))

    pipeline.fit(salaryDf)


  }
}
