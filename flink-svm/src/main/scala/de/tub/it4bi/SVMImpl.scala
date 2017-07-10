package de.tub.it4bi

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.ml.RichExecutionEnvironment
import org.apache.flink.ml.classification.SVM
import org.apache.flink.ml.common.LabeledVector

/**
  * SVM training job producing the model features
  */
object SVMImpl {
  def main(args: Array[String]): Unit = {
    val params: ParameterTool = ParameterTool.fromArgs(args)
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.getConfig.setGlobalJobParameters(params)

    val pathToTrainingFile = params.getRequired("training")
    // Read the training data set, from a LibSVM formatted file
    val trainingDS: DataSet[LabeledVector] = env.readLibSVM(pathToTrainingFile)

    // Create the SVM learner
    val svm = SVM()
      .setBlocks(params.getInt("blocks", 10))
      .setIterations(params.getInt("iteration", 10))

    // Learn the SVM model
    svm.fit(trainingDS)

    svm.weightsOption match {
      case Some(weights) =>
        val model: DataSet[(Int, Double)] = weights
          .flatMap(_.data.zipWithIndex)
          .map(x => (x._2 + 1, x._1))

        val range = params.getInt("range", 1000)
        var finalModel: DataSet[String] = null

        params.getBoolean("partition") match {
          case true => finalModel = model
            .map(x => (x._1 / range, x._1, x._2))
            .groupBy(0)
            .reduceGroup(x => rangePartition(x))
          case false => finalModel = model.map(x => x._1 + "," + x._2)
        }
        if (params.has("output")) {
          finalModel.writeAsText(params.get("output"), WriteMode.OVERWRITE)
          env.execute("SVM Fitting")
        } else {
          println("Printing result to stdout. Use --output to specify output path.")
          finalModel.print()
        }
      case None => println("Perform SVM Fitting first!")
    }
  }

  /**
    * creates String reprsenation of a range of keys
    * @param iterator
    * @return String representation of all keys in a range
    */
  def rangePartition(iterator: Iterator[(Int, Int, Double)]): String = {
    val rangeOfKeys = iterator.toList
    val sb: StringBuilder = new StringBuilder
    val key: String = rangeOfKeys.head._1.toString
    sb.append(key).append(",")
    rangeOfKeys.foreach(x => sb.append(x._2 + ":" + x._3 + ";"))
    sb.deleteCharAt(sb.length - 1)
    sb.toString()
  }
}
