package com.madhukaraphatak.examples.sparktwo.multisource

import com.madhukaraphatak.examples.sparktwo.multisource.MultiSourceLoad.loadData
import org.apache.spark.sql.SparkSession

object DoubleCounting {

  def main(args: Array[String]): Unit = {

    val url = args(0)
    val user = args(1)
    val password = args(2)

    val sparkSession = SparkSession.builder.
      master("local[*]")
      .appName("example")
      .getOrCreate()

    val (transactionDf, demographicsDf, marketingDf, creditDeptDf) = loadData(sparkSession, url, user, password)

    val dataModel = transactionDf.
      join(demographicsDf,Seq("customer_id"))
      .join(marketingDf,Seq("customer_id","transaction_id"))
      .join(creditDeptDf, "customer_id")

    //count sales by point_redemption_method
    creditDeptDf.groupBy("point_redemption_method").count().show()
    // count from data model
    dataModel.groupBy("point_redemption_method").count().show()




  }

}
