package com.madhukaraphatak.examples.sparktwo.multisource

import org.apache.spark.sql.{DataFrame, SparkSession}


object MultiSourceLoad {

  def loadData(sparkSession: SparkSession,url:String,user:String,password:String):(DataFrame,
    DataFrame,DataFrame,DataFrame) = {
     val transactionDf = sparkSession
       .read
      .format("jdbc")
      .option("url",url)
      .option("user",user)
      .option("password",password)
      .option("dbtable","transactions_tellius")
      .load()

    val demographicsDf = sparkSession
       .read
      .format("jdbc")
      .option("url",url)
      .option("user",user)
      .option("password",password)
      .option("dbtable","demographics_tellius")
      .load()

    val marketingDf = sparkSession
        .read
        .format("csv")
        .option("header","true")
        .load("/Users/madhu/Downloads/customer360/marketing_tellius.csv")

    val creditDeptDf = sparkSession
        .read
        .format("csv")
        .option("header","true")
        .load("/Users/madhu/Downloads/customer360/credit_dept_tellius.csv")

    (transactionDf,demographicsDf,marketingDf,creditDeptDf)
  }

  def main(args: Array[String]): Unit = {

    val url = args(0)
    val user = args(1)
    val password = args(2)

    val sparkSession = SparkSession.builder.
      master("local")
      .appName("example")
      .getOrCreate()


    val (transactionDf, demographicsDf, marketingDf, creditDeptDf) = loadData(sparkSession,url,user,password)

    transactionDf.printSchema()

    demographicsDf.printSchema()

    marketingDf.printSchema()

    creditDeptDf.printSchema()


  }
}
