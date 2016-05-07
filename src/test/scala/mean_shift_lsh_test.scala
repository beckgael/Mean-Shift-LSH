import org.scalatest.FlatSpec
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import msLsh._

abstract class specTest extends FlatSpec

class SetSpec extends specTest {

  val conf = new SparkConf().setAppName("test")
  							.setMaster("local[4]")

  val sc = new SparkContext(conf)
  val dp = sc.defaultParallelism

  val rdd0 = sc.parallelize(Array(
		  ("1",Vectors.dense(1.0,1.0,1.0)),
		  ("2",Vectors.dense(2.0,2.0,2.0)),
		  ("3",Vectors.dense(3.0,3.0,3.0))))

  val ms1 = msLsh.MsLsh

  val center = Vectors.dense(Array(2.0,2.0,2.0))
  val labels0 = Array("0","0","0")

  // No matter how many iterations we use, we should get one cluster,
  // centered at the mean of the points

  "train" should "give a map of centroids and data's cluster label" in {
    var model = ms1.train( sc,
				  			rdd0,
				  			k=1,
				  			threshold_cluster1=100,
				  			threshold_cluster2=100,
				  			yStarIter=1,
				            cmin=0,
				          	normalisation=true,
				            w=1,
				            nbseg=100,
				            nbblocs1=1,
				            nbblocs2=1,
				            nbPart=dp)

    model.clustersCenter.foreach(println)
    var labels = model.rdd.map(_._1).collect

    assert(model.clustersCenter("0") == center )
    assert(labels(0) == labels0(0))

    model = ms1.train( sc,
			  			rdd0,
			  			k=1,
			  			threshold_cluster1=100,
			  			threshold_cluster2=100,
			  			yStarIter=5,
			            cmin=0,
			          	normalisation=true,
			            w=1,
			            nbseg=100,
			            nbblocs1=1,
			            nbblocs2=1,
			            nbPart=dp)

    model.clustersCenter.foreach(println)
    labels = model.rdd.map(_._1).collect

    assert(model.clustersCenter("0") == center )
    assert(labels(1) == labels0(1))

    model = ms1.train( sc,
			  			rdd0,
			  			k=1,
			  			threshold_cluster1=100,
			  			threshold_cluster2=100,
			  			yStarIter=10,
			            cmin=0,
			          	normalisation=true,
			            w=1,
			            nbseg=100,
			            nbblocs1=1,
			            nbblocs2=1,
			            nbPart=dp)

    model.clustersCenter.foreach(println)

    labels = model.rdd.map(_._1).collect
    assert(model.clustersCenter("0") == center )
    assert(labels(2) == labels0(2))

  }

  val vector0 = Vectors.dense(Array(0.5,0.5,0.5))

  "prediction" should "predict vector0 belongs to cluster with centroid (1,1,1)" in {
    
    val model = ms1.train( sc,
			  			rdd0,
			  			k=1,
			  			threshold_cluster1=0.000001,
			  			threshold_cluster2=0.000001,
			  			yStarIter=1,
			            cmin=0,
			          	normalisation=true,
			            w=1,
			            nbseg=100,
			            nbblocs1=1,
			            nbblocs2=1,
			            nbPart=dp)

    val res = model.predict(vector0)
    val centroid0 = model.clustersCenter(res)
    assert(centroid0 == Vectors.dense(Array(1.0,1.0,1.0)))
  }

}