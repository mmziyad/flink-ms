package de.tub.it4bi

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.{DataSet, _}
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.ml.common.ParameterMap
import org.apache.flink.ml.recommendation.ALS

/**
  * ALS job producing user and item Factors
  */

object ALSImpl {

  def main(args: Array[String]) {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val params: ParameterTool = ParameterTool.fromArgs(args)
    env.getConfig.setGlobalJobParameters(params)

    // only comma or tab are supported.
    val fieldDelimiter = params.get("fieldDelimiter", "comma") match {
      case "tab" => "\t"
      case "comma" => ","
    }
    val ignoreFirstLine = params.getBoolean("ignoreFirstLine", true)

    if (params.has("input")) {
      val inputDS: DataSet[(Int, Int, Double)] = env.readCsvFile[(Int, Int, Double)](
        filePath = params.get("input"),
        fieldDelimiter = fieldDelimiter,
        ignoreFirstLine = ignoreFirstLine)

      // Setup the ALS learner
      val als = ALS()
        .setIterations(params.getInt("iterations", 10))
        .setNumFactors(params.getInt("numFactors", 10))

      if (params.has("blocks")) {
        als.setBlocks(params.getInt("blocks"))
      }
      if (params.has("temporaryPath")) {
        als.setTemporaryPath(params.get("temporaryPath"))
      }

      // Set the other parameters via a parameter map
      val parameters = ParameterMap()
        .add(ALS.Lambda, params.getDouble("lambda", 0.9))
        .add(ALS.Seed, params.getLong("seed", 42L))

      // Calculate the factorization
      als.fit(inputDS, parameters)

      // item Factors
      val itemFactors = als.factorsOption.get._2.map { t => new OutputFactor(t.id, "I", t.factors) }
      // user Factors
      val userFactors = als.factorsOption.get._1.map { t => new OutputFactor(t.id, "U", t.factors) }

      // prepare output
      if (params.has("itemFactors") && params.has("userFactors")) {
        itemFactors.writeAsText(params.get("itemFactors"), WriteMode.OVERWRITE)
        userFactors.writeAsText(params.get("userFactors"), WriteMode.OVERWRITE)
        env.execute("[ALS] model-training")
      } else {
        println("Printing results to stdout. Use --itemFactors and --userFactors to specify output locations.")
        println("==== USER FACTORS ====")
        userFactors.print()
        println("==== ITEM FACTORS ====")
        itemFactors.print()
      }
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
  case class OutputFactor(id: Long, typeOfFeature: String, factors: Array[Double]) {
    override def toString: String = s"$id,$typeOfFeature,${factors.mkString(";")}"
  }

}
