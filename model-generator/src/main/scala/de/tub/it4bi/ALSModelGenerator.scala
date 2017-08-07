package de.tub.it4bi


import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode

import scala.util.Random

/**
  * Generates a random ALS model from the given parameters
  * Note: This model is only for testing the latency and throughput. Not for quality.
  * 1. Number of users
  * 2. Number of items
  * 3. Number of latent factors
  */

object ALSModelGenerator {

  /**
    * Helper method to produce a row in ALS model
    *
    * @param id
    * @param category
    * @param latentFactors
    * @return row in ALS model
    */
  def randomALS(id: Int, category: String, latentFactors: Int): String = {
    val factors = for (i <- 1 to latentFactors)
      yield Random.nextDouble() / Random.nextDouble() * latentFactors
    id + "," + category + "," + factors.mkString(";")
  }

  def main(args: Array[String]) {

    // set up the execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment
    val params: ParameterTool = ParameterTool.fromArgs(args)
    env.getConfig.setGlobalJobParameters(params)

    val numUsers = params.getRequired("numUsers").toInt
    val numItems = params.getRequired("numItems").toInt
    val latentFactors = params.getRequired("latentFactors").toInt
    val p = params.getInt("parallelism", 2)

    val userVectors = env
      .fromCollection(1 to numUsers)
      .map(u => randomALS(u, "U", latentFactors))
      .setParallelism(p)
    val itemVectors = env
      .fromCollection(1 to numItems)
      .map(i => randomALS(i, "I", latentFactors))
      .setParallelism(p)

    val model = userVectors.union(itemVectors).setParallelism(p)

    // prepare output
    params.has("output") match {
      case true =>
        model.writeAsText(params.get("output"), WriteMode.OVERWRITE)
        env.execute("[ALS] Model Generator ")
      case false =>
        println("Printing results to stdout. Use --output to specify output location")
        model.print()
    }
  }
}
