/*
package msLsh

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.mllib.linalg.{Vector, Vectors}

object Main {
  def main(args: Array[String]): Unit = {

	val sc = new SparkContext(new SparkConf)

	val meanShift = msLsh.MsLsh

	val data = sc.textFile(args(0)).map(_.split(",").map(_.toDouble))
					.zipWithIndex
					.map{ case(data, id) => (id, Vectors.dense(data))}.cache

	val model = meanShift.train(sc, data, k=args(1).toInt, epsilon1=args(2).toDouble, epsilon2=args(3).toDouble, yStarIter=args(4).toInt, cmin=args(5).toInt, normalisation=args(6).toBoolean, w=1, nbseg=100, nbblocs1=args(7).toInt, nbblocs2=args(8).toInt, nbLabelIter=args(9).toInt)  

	meanShift.savelabeling(model(0),"/myPath/label")
	meanShift.saveClusterInfo(model(0),"/myPath/clusterInfo")

	}
}
*/