package com.madhukaraphatak.examples.sparktwo.ml

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{OneHotEncoderEstimator, StringIndexer, VectorAssembler}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.SparkSession

object ParallelCrossValidation {

  def main(args: Array[String]): Unit = {


    val sparkSession = SparkSession.builder.
      master("local[*]")
      .appName("example")
      .getOrCreate()


    val salaryDf = sparkSession.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("src/main/resources/adult.csv")

    val stringColumns = Array("workclass", "occupation", "sex", "education", "martial_status", "relationship",
      "race", "native_country")

    val numericalColumns = Array("age", "fnlwgt", "capital_loss", "capital_gain")

    val labelColumn = "salary"
    val outputColumns = stringColumns.map(_ + "_onehot")

    val indexers = stringColumns.map(column => {
      val indexer = new StringIndexer()
      indexer.setInputCol(column)
      indexer.setHandleInvalid("keep")
      indexer.setOutputCol(column + "_index")
    })

    val singleOneHotEncoder = new OneHotEncoderEstimator()
    singleOneHotEncoder.setInputCols(stringColumns.map(_ + "_index"))
    singleOneHotEncoder.setOutputCols(outputColumns)

    val vectorAssembler = new VectorAssembler()
    vectorAssembler.setInputCols(outputColumns ++ numericalColumns)
    vectorAssembler.setOutputCol("features")

    val labelIndexer = new StringIndexer()
    labelIndexer.setInputCol("salary")
    labelIndexer.setOutputCol("label")

    val logisticRegression = new LogisticRegression()


    val pipeline = new Pipeline()
    pipeline.setStages(indexers ++ Array(singleOneHotEncoder)
      ++ Array(vectorAssembler) ++ Array(labelIndexer) ++ Array(logisticRegression))

    val paramMap = new ParamGridBuilder()
      .addGrid(logisticRegression.maxIter, Array(1, 2, 3)).build()


    val crossValidator = new CrossValidator()
    crossValidator.setEstimator(pipeline)
    crossValidator.setEvaluator(new BinaryClassificationEvaluator())
    crossValidator.setEstimatorParamMaps(paramMap)
    crossValidator.setParallelism(3)

    crossValidator.fit(salaryDf)

  }

}
