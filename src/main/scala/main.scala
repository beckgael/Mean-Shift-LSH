package msLsh

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.mllib.linalg.{Vector, Vectors}

object Main {
  def main(args: Array[String]): Unit = {

	val sc = new SparkContext(new SparkConf(true))

	val meanShift = msLsh.MsLsh

	val data = sc.textFile("/pathToMyData/"+args(10)).map(_.split(",").map(_.toDouble))
					.zipWithIndex
					.map{ case(data,id) => (id, Vectors.dense(data))}.cache

	val model = meanShift.train(sc, data, k=args(0).toInt, threshold_cluster1=args(1).toDouble, threshold_cluster2=args(2).toDouble, yStarIter=args(3).toInt, cmin=args(4).toInt, normalisation=args(5).toBoolean, w=1, nbseg=100, nbblocs1=args(6).toInt, nbblocs2=args(7).toInt, nbLabelIter=args(8).toInt, nbPart=args(9).toInt)  


	meanShift.savelabeling(model(0),"/myPath/label")
	meanShift.saveClusterInfo(model(0),"/myPath/clusterInfo")

	}
}