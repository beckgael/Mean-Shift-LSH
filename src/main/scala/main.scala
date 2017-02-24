package msLsh


import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.mllib.linalg.{Vector, Vectors}


object Main {
  def main(args: Array[String]): Unit = {

	val sc = new SparkContext(new SparkConf(true)
									/*
									.setMaster("spark://c4bb457f5100:7077")
									.set("spark.cores.max","12")
									.set("spark.eventLog.enabled","true")
                                  	.set("spark.eventLog.dir","/var/data/log/JobsLog")
                                  	*/)

	val meanShift = msLsh.MsLsh

	val data = sc.textFile("/home/kybe/Documents/defi/ds/"+args(5)).map(_.split(",").take(5).map(_.toDouble))
					.zipWithIndex
					.map(y => (y._2.toString,Vectors.dense(y._1))).cache

	val model = meanShift.train(
							sc,
	                          data,
	                          k=args(0).toInt,
	                          threshold_cluster1=args(1).toDouble,
	                          threshold_cluster2=args(2).toDouble,
	                          yStarIter=args(3).toInt,
	                          cmin=args(4).toInt,
	                          normalisation=true,
	                          w=1,
	                          nbseg=100,
	                          nbblocs1=1,
	                          nbblocs2=1,
	                          nbLabelIter=1,
	                          nbPart=2)  

	println("*******************")
	println("*******************")
	println("*******************")
	println("*******************")
	println("*******************")
	println("*******************")
	println("*******************")
	println("*******************")
	println("*******************")
	println("*******************")

	meanShift.savelabeling(model(0),"/home/kybe/Documents/res/label")
	meanShift.saveClusterInfo(sc, model(0),"/home/kybe/Documents/res/clusterInfo")


}
}