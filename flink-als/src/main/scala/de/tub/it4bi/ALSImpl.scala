package de.tub.it4bi

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.{DataSet, _}
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.ml.common.ParameterMap
import org.apache.flink.ml.recommendation.ALS

/**
  * Created by zis on 17/04/17.
  *
  * Sample parameters:
  * --input "flink-als/data/ratings.csv"
  * --testing "flink-als/data/testing.csv"
  * --itemFactors "/tmp/itemFactors"
  * --userFactors "/tmp/userFactors"
  */

object ALSImpl {

  def main(args: Array[String]) {

    //params
    val params: ParameterTool = ParameterTool.fromArgs(args)

    //env
    val env = ExecutionEnvironment.getExecutionEnvironment

    // make parameters available in the web interface
    env.getConfig.setGlobalJobParameters(params)

    // Read input data set from a csv file
    if (params.has("input")) {
      val inputDS: DataSet[(Int, Int, Double)] = env
        .readCsvFile[(Int, Int, Double)](params.get("input"),ignoreFirstLine = true)

      // Setup the ALS learner
      val als = ALS()
        .setIterations(10)
        .setNumFactors(10)
        .setBlocks(100)
      //.setTemporaryPath("hdfs://tempPath")

      // Set the other parameters via a parameter map
      val parameters = ParameterMap()
        .add(ALS.Lambda, 0.9)
        .add(ALS.Seed, 42L)

      // Calculate the factorization
      als.fit(inputDS, parameters)

      // ********* For Testing the model *************** //
      // Read the testing data set from a csv file
      // val testingDS: DataSet[(Int, Int)] = env.readCsvFile[(Int, Int)](params.get("testing"))
      // Calculate the ratings according to the matrix factorization
      // val predictedRatings = als.predict(testingDS)
      // ********* For Testing the model *************** //

      // item Factors
      val itemFactors = als
        .factorsOption.get._2
        .map { t => new OutputFactor(t.id, "I", t.factors) }

      // user Factors
      val userFactors = als
        .factorsOption.get._1
        .map { t => new OutputFactor(t.id, "U", t.factors) }

      // prepare output
      if (params.has("itemFactors") && params.has("userFactors")) {
        itemFactors.writeAsText(params.get("itemFactors"), WriteMode.OVERWRITE)
        userFactors.writeAsText(params.get("userFactors"), WriteMode.OVERWRITE)
      } else {
        println("Printing results to stdout. Use --itemFactors and --userFactors to specify output locations.")
        itemFactors.print()
        userFactors.print()
      }
      env.execute("ALS Training")
    } else {
      println("Use --input to specify file input.")
    }
  }

  /**
    * case class to represent the user or item features
    *
    * @param id
    * @param typeOfFeature
    * @param factors
    */
  case class OutputFactor(id: Int, typeOfFeature: String, factors: Array[Double]) {
    override def toString: String = s"$id,$typeOfFeature,${factors.mkString(";")}"
  }
}
