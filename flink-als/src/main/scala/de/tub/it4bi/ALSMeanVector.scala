package de.tub.it4bi

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.{DataSet, _}
import org.apache.flink.core.fs.FileSystem.WriteMode

/**
  * Calculate the mean user or item vector from ALS output
  */
object ALSMeanVector {

  def main(args: Array[String]) {
    // setup the environment
    val env = ExecutionEnvironment.getExecutionEnvironment
    val params: ParameterTool = ParameterTool.fromArgs(args)
    env.getConfig.setGlobalJobParameters(params)

    val factorType = params.getRequired("type") match {
      case "item" => "I"
      case "user" => "U"
      case _ => throw new IllegalArgumentException("specify type as either 'item' or 'user'.")
    }

    // read input user or item model
    val inputDS: DataSet[String] = env.readTextFile(filePath = params.get("input"))

    // derive user or item  vectors
    val vectors: DataSet[Array[Double]] = inputDS.map(row => row.split(",")(2).split(";").map(_.toDouble))

    // calculate the mean vector
    val meanVector: DataSet[String] = vectors
      .map(vector => (vector, 1l))
      .reduce((left, right) => (left._1.zip(right._1).map { case (x, y) => x + y }, left._2 + right._2))
      .map(out => out._1.map(aggregate => aggregate / out._2).mkString(";"))
      .map(x => "MEAN" + "," + factorType + "," + x) // to load to kafka topic

    // prepare output
    params.has("output") match {
      case true =>
        meanVector.writeAsText(params.get("output"), WriteMode.OVERWRITE)
        env.execute("[ALS] mean-vector")
      case false =>
        println("Printing results to stdout. Use --output to specify output location")
        meanVector.print()
    }
  }
}
