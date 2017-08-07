package de.tub.it4bi

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode

import scala.annotation.tailrec
import scala.util.Random

/**
  * Generates a random SVM model from the given parameters
  * Note: This model is only for testing the latency and throughput. Not for quality.
  * 1. Number of features
  * 2. Range
  */

object SVMModelGenerator {

  /**
    * Helper method to produce a row rangePartitioned SVM model
    *
    * @param bucket
    * @param range
    * @return row in SVM model
    */
  def randomSVM(bucket: Int, range: Int): String = {
    val startKey = bucket * range
    val endKey = startKey + (range - 1)

    val factors = for (i <- startKey to endKey)
      yield {
        val currentFactor = Random.nextBoolean() match {
          case true => 0
          case false => between(-10, 10, Random)
        }
        i + ":" + currentFactor
      }

    bucket + "," + factors.mkString(";")
  }

  /*
   * Pick a random number between to double values, inclusive.
   */
  @tailrec def between(low: Double, high: Double, r: Random): Double = {
    if (low == high) {
      low
    } else {
      val mid = low + (high / 2 - low / 2)
      if (r.nextBoolean) between(low, mid, r) else between(mid, high, r)
    }
  }

  def main(args: Array[String]) {

    // set up the execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment
    val params: ParameterTool = ParameterTool.fromArgs(args)
    env.getConfig.setGlobalJobParameters(params)

    val numFeatures = params.getRequired("numFeatures").toInt
    val range = params.getRequired("range").toInt
    val p = params.getInt("parallelism", 2)

    val model = env
      .fromCollection(0 to numFeatures / range)
      .map(bucket => randomSVM(bucket, range))
      .setParallelism(p)

    // prepare output
    params.has("output") match {
      case true =>
        model.writeAsText(params.get("output"), WriteMode.OVERWRITE)
        env.execute("[SVM] Model Generator ")
      case false =>
        println("Printing results to stdout. Use --output to specify output location")
        model.print()
    }
  }
}
