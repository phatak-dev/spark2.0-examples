package com.madhukaraphatak.examples.sparktwo

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Literal, Multiply}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

/**
  * User Defined Optimization
  */
object CustomOptimizationExample {

  object MultiplyOptimizationRule extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan transformAllExpressions {
      case Multiply(left,right) if right.isInstanceOf[Literal] &&
        right.asInstanceOf[Literal].value.asInstanceOf[Double] == 1.0 =>
        println("optimization of one applied")
        left
    }
  }


  def main(args: Array[String]) {

    val sparkSession = SparkSession.builder.
      master("local")
      .appName("example")
      .getOrCreate()


    val df = sparkSession.read.option("header","true").csv("src/main/resources/sales.csv")
    val multipliedDF = df.selectExpr("amountPaid * 1")
    println(multipliedDF.queryExecution.optimizedPlan.numberedTreeString)

    //add our custom optimization
    sparkSession.experimental.extraOptimizations = Seq(MultiplyOptimizationRule)
    val multipliedDFWithOptimization = df.selectExpr("amountPaid * 1")
    println("after optimization")

    println(multipliedDFWithOptimization.queryExecution.optimizedPlan.numberedTreeString)
  }
}
