package de.tub.it4bi

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.ml.RichExecutionEnvironment
import org.apache.flink.ml.classification.SVM
import org.apache.flink.ml.common.LabeledVector

/**
  * Created by zis on 08/06/17.
  */
object SVMImpl {
  def main(args: Array[String]): Unit = {

    val params: ParameterTool = ParameterTool.fromArgs(args)
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.getConfig.setGlobalJobParameters(params)

    val pathToTrainingFile = params.getRequired("training")
    val pathToTestingFile = params.get("testing")

    // Read the training data set, from a LibSVM formatted file
    val trainingDS: DataSet[LabeledVector] = env.readLibSVM(pathToTrainingFile)

    // Create the SVM learner
    val svm = SVM()
      .setBlocks(10)

    // Learn the SVM model
    svm.fit(trainingDS)

    svm.weightsOption match {
      case Some(weights) =>
        val model: DataSet[String] = weights
          .flatMap(_.data.zipWithIndex)
          .map(x => (x._2 + 1) + "," + x._1)

        if (params.has("output")) {
          model.writeAsText(params.get("output"), WriteMode.OVERWRITE)
          env.execute("SVM Fitting")
        } else {
          println("Printing result to stdout. Use --output to specify output path.")
          model.print()
        }

      case None => println("Perform SVM Fitting first!")
    }

    /*
      // Read the testing data set
      val testingDS: DataSet[LabeledVector] = env.readLibSVM(pathToTestingFile)

      // Calculate the predictions for the testing data set
      val evaluationnDS: DataSet[(Double, Double)] = svm.evaluate(testingDS.map(x => (x.vector, x.label)))
      val predictionDS: DataSet[(Vector, Double)] = svm.predict(testingDS.map(_.vector))

      evaluationnDS.print()
    */


  }
}
