/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://ww.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * @author Beck Gaël
 */

package msLsh

import scala.util.Random
import scala.util.Sorting.quickSort
import scala.collection.mutable.{ArrayBuffer, ListBuffer, HashMap}
import spire.implicits._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, HashPartitioner}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.mllib.util._
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.feature.StandardScaler
import java.io.FileWriter
/**
 * Mean-Shift-LSH clustering 
 * This algorithm could be used to analyse complex multivariate multidimensional data.
 * It can also be apply in order to analyse image, to use this features it is recommanded to convert image from RGB space to L*u*v* space
 */


/**
 * The major class where MS-LSH algorithm and prediction fonction are implemented
 */
class MsLsh private (private var k:Int, private var threshold_cluster1:Double, private var threshold_cluster2:Double, private var yStarIter:Int, private var cmin:Int, private var normalisation:Boolean, private var w:Int, private var nbseg:Int, private var nbblocs1:Int, private var nbblocs2:Int, private var nbLabelIter:Int) extends Serializable {  

  def this() = this(50, 0.001, 0.05, 10, 0, true, 1, 100, 100, 50, 5)
  
  /**
   * Set normalisation boolean
   */
  def set_boolnorm(bool1:Boolean) : this.type = {
    this.normalisation = bool1
    this
  }
    
  /**
   * Set w
   */
  def set_w(newW:Int) : this.type = {
    this.w = newW
    this
  }
  
  /**
   * Set image analysis boolean
   */
  def set_nbseg(nbseg1:Int) : this.type = {
    this.nbseg = nbseg1
    this
  }
  
  /**
   * Set image analysis boolean
   */
  def set_nbblocs1(bloc1:Int) : this.type = {
    this.nbblocs1 = bloc1
    this
  }
    
  /**
   * Set image analysis boolean
   */
  def set_nbblocs2(bloc2:Int) : this.type = {
    this.nbblocs2 = bloc2
    this
  }
  
  /**
   * Set k value
   */
  def set_k(kval:Int) : this.type = {
    this.k = kval
    this
  }
  
  /**
   * Set threshold 1 for labeling step
   */
  def set_threshold_cluster1(threshold_cluster1_val:Double) : this.type = {
    this.threshold_cluster1 = threshold_cluster1_val
    this
  }  

  /**
   * Set threshold 2 for labeling step
   */
  def set_threshold_cluster2(threshold_cluster2_val:Double) : this.type = {
    this.threshold_cluster2 = threshold_cluster2_val
    this
  }
  
  /**
   * Set iteration number for ascend gradient step
   */
  def set_yStarIter(yStarIter_val:Int) : this.type = {
    this.yStarIter = yStarIter_val
    this
  }
  
  /**
   * Set minimal cardinality for cluster
   */
  def set_cmin(cmin_val:Int) : this.type = {
    this.cmin = cmin_val
    this
  }  
    
  /**
   * Set number of time we makke labelizing step 
   */
  def set_nbLabelIter(nbLabelIter_val:Int) : this.type = {
    this.nbLabelIter = nbLabelIter_val
    this
  }  
  
  /**
   * Get k value
   */
  def get_k = this.k
    
  /**
   * Get nbblocs1 value
   */
  def get_nbblocs1 = this.nbblocs1
      
  /**
   * Get nbblocs2 value
   */
  def get_nbblocs2 = this.nbblocs2
  
  /**
   * Get threshold 1 value
   */
  def get_threshold_cluster1 = this.threshold_cluster1
    
  /**
   * Get threshold 2 value
   */
  def get_threshold_cluster2 = this.threshold_cluster2
  
  /**
   * Get number of iteration for gradient ascend
   */
  def get_yStarIter = this.yStarIter
  
  /**
   * Get minimal cardinality value
   */
  def get_cmin = this.cmin
      
  /**
   * Get number of labelizing iteration
   */
  def get_nbLabelIter = this.nbLabelIter
  
  /**
   * Mean Shift LSH accomplish his clustering work
   */
  def run(sc:SparkContext, data:RDD[(Long,Vector)]) : ArrayBuffer[Mean_shift_lsh_model] = {

    /**
    * Initialisation 
    */
    data.cache
    val size =  data.count().toInt
    val maxK = (size/nbblocs1).toInt -1 
    val dp = sc.defaultParallelism

    if (size < cmin) throw new IllegalStateException("Exception : cmin > data size")
    if (maxK <= k) throw new IllegalStateException("Exception : You set a too high K value")
    /**
    * Dataset Normalisation
    */
    val (normalizedOrNotRDD, maxArray, minArray) = if(normalisation) Fcts.scaleRdd(data) else (data, Array.empty[Double], Array.empty[Double])    
    val dim = normalizedOrNotRDD.first._2.size
    val maxMinArray = Array(maxArray, minArray) 
    
    /**
    * Gradient ascent/Research of Y* 
    */
    val b = Random.nextDouble * w
    val tabHash0 = sc.broadcast(Fcts.tabHash(nbseg,dim))
  
    var rdd_LSH = normalizedOrNotRDD.map{ case(id, vector) => (id, vector, vector, Fcts.hashfunc(vector,w,b,tabHash0.value))}
                        .repartition(nbblocs1)
    var rdd_res : RDD[(Long, Vector, Vector, Double)] = sc.emptyRDD
    data.unpersist(true)
   
    for( ind <- 0 until yStarIter  ) {
      val rdd_LSH_ord =  rdd_LSH.sortBy(_._4).mapPartitions( x => {
        val approxKNN = x.toArray
        approxKNN.map{ case(id, originalVector, mod, hashV) => {
          val distKNNFromCurrentPoint = approxKNN.map{ case(_, originalVector2, mod2, hashV2) => (originalVector2, Vectors.sqdist(mod, originalVector2))}
          quickSort(distKNNFromCurrentPoint)(Ordering[(Double)].on(_._2))
          (id, originalVector, Fcts.computeCentroid(distKNNFromCurrentPoint.take(k), k))
        }}.iterator
      }
      ,true)
      if(ind < yStarIter){
        rdd_LSH = rdd_LSH_ord.map{ case(id, originalVector, mod) => (id, originalVector, mod, Fcts.hashfunc(mod, w, b, tabHash0.value))}
      }
      // rdd_res[(Index,NewVect,OrigVect,lshValue)]
      else rdd_res = rdd_LSH_ord.map{ case(id, originalVector, mod) => (id, mod, originalVector, Fcts.hashfunc(mod, w, b, tabHash0.value))}
    }

    val readyToLabelization = if( nbblocs2 == 1 ) rdd_res.map{ case(id, mod, originalVector, hashV) => (id, mod, originalVector)} else rdd_res.sortBy(_._4)                      .map{ case(id, mod, originalVector, hashV) => (id, mod, originalVector)}.coalesce(nbblocs2, shuffle = false)
    if(nbLabelIter > 1){ readyToLabelization.cache }

  val models = ArrayBuffer.empty[Mean_shift_lsh_model]

  /**
   * Labelization function which gathered mods together into clusters
   */
  val labelizing = () => 
  {
    
    val clusterByLshBucket = readyToLabelization.mapPartitionsWithIndex( (ind,it) => {
      var stop = 1
      var labeledData = ListBuffer.empty[(Int, (Long, Vector, Vector))]
      var bucket = it.toBuffer
      var mod1 = bucket(Random.nextInt(bucket.size))._2
      var clusterID = (ind+1)*10000
      while ( stop != 0 ) {
          val rdd_Clust_i_ind = bucket.filter{ case(idx, mod, originalVector) => { Vectors.sqdist(mod, mod1) <= threshold_cluster1 }}
          val rdd_Clust_i2_ind = rdd_Clust_i_ind.map{ case(idx, mod, originalVector) => (clusterID, (idx, mod, originalVector))}
          labeledData ++= rdd_Clust_i2_ind
          // We keep Y* whose distance is greather than threshold
          bucket --= rdd_Clust_i_ind
          stop = bucket.size.toInt
          if(stop != 0) { mod1 = bucket(Random.nextInt(bucket.size))._2 }
          clusterID += 1
      }
      labeledData.toIterator
    })

    /**
    * Gives Y* labels to original data
    */
    val rdd_Ystar_labeled = clusterByLshBucket.map{ case(clusterID, (idx, mod, originalVector)) => (clusterID, (idx, originalVector, mod)) }
                                              .partitionBy(new HashPartitioner(dp))
                                              .cache
    
    val centroidMapOrig = HashMap(rdd_Ystar_labeled.map{ case(clusterID, (idx, originalVector, mod)) => (clusterID , originalVector.toArray) }
                                         .reduceByKey(_ + _)
                                         .sortBy(_._1)
                                         .collect:_*)


    val numElemByCLust = rdd_Ystar_labeled.countByKey.toArray.sortBy(_._1)


    val centroids = ArrayBuffer.empty[(Int, Vector, Int)]

    // Form the array of clusters centroids
    for( (clusterID, cardinality) <- numElemByCLust )
      centroids += ((clusterID, Vectors.dense(centroidMapOrig(clusterID).map(_ / cardinality)), cardinality.toInt))

    /**
     * Fusion cluster with centroid < threshold
     */

    val newCentroids = ArrayBuffer.empty[(Int, Vector)]
    val numElemByCluster = ArrayBuffer.empty[(Int, Int)]
    val oldToNewLabelMap = HashMap.empty[Int, Int]
    var randomCentroidVector = centroids(Random.nextInt(centroids.size))._2
    var newClusterID = 0
    var stop2 = 1
    while (stop2 != 0) {
      val closestClusters = centroids.filter{ case(clusterID, vector, clusterCardinality) => { Vectors.sqdist(vector,randomCentroidVector) <= threshold_cluster2 }}
      // We compute the mean of the cluster
      val gatheredCluster = closestClusters.map{ case(clusterID, vector, clusterCardinality) => (vector.toArray, clusterCardinality)}
                                      .reduce( (a, b) => (a._1 + b._1, a._2 + b._2) )
      newCentroids += ( (newClusterID, Vectors.dense(gatheredCluster._1.map(_/closestClusters.size)) ) )
      numElemByCluster += ( (newClusterID, gatheredCluster._2) )
      for( (clusterID, _, _) <- closestClusters ) {
        oldToNewLabelMap += (clusterID -> newClusterID)
      }
      centroids --= closestClusters
      // We keep Y* whose distance is greather than threshold
      stop2 = centroids.size
      if(stop2 != 0) randomCentroidVector = centroids(Random.nextInt(centroids.size))._2
      newClusterID += 1
    }

    /**
    * Fusion of cluster which cardinality is smaller than cmin 
    */
    val tab_inf_cmin = numElemByCluster.filter( _._2 <= cmin)
    val stop_size = tab_inf_cmin.size
    val indexOfSmallerClusters = tab_inf_cmin.map(_._1).toBuffer
    val map_ind_all = numElemByCluster.toMap
    val toGatherCentroids = newCentroids.zipWithIndex.map{ case((clusterID, vector), idx) => (idx, clusterID, vector, map_ind_all(clusterID), clusterID)}.toBuffer
  
    while( indexOfSmallerClusters.size != 0 )
    {
      for ( (idxx, currentClusterID, vector2, sizecurrent, _) <- toGatherCentroids )
      {
        if( sizecurrent < cmin )
        {
          val parCentroids = toGatherCentroids.par
          val sortedClosestCentroid = parCentroids.map{ case(idx, newClusterID, vector, cardinality, originalClusterID) =>(Vectors.sqdist(vector, vector2), idx, newClusterID, cardinality)}.toArray
          quickSort(sortedClosestCentroid)(Ordering[(Double)].on(_._1))
          var cpt3 = 1
          val newClusterIDsorted = sortedClosestCentroid(cpt3)._3 
          while ( newClusterIDsorted == currentClusterID ) cpt3 += 1
          val (_, _, closestClusterID, closestClusterSize) = sortedClosestCentroid(cpt3)
          val closestClusters = parCentroids.filter{ case(idx, newClusterID, vector, cardinality, originalClusterID) => newClusterID == closestClusterID}
          val littleClusterWithSameCurrentLabel = parCentroids.filter{ case(idx, newClusterID, vector, cardinality, originalClusterID) => newClusterID == currentClusterID}
          val idOfTreatedCluster = ArrayBuffer.empty[Int]
          val newClusterSize = closestClusterSize + sizecurrent
          // Update
          for( (id, newClusterID, vector, cardinality, originalClusterID) <- closestClusters)
          {
            idOfTreatedCluster += newClusterID
            toGatherCentroids(id) = (id, closestClusterID, vector, newClusterSize, originalClusterID)
          }
          for( (id, newClusterID, vector, cardinality, originalClusterID) <- littleClusterWithSameCurrentLabel)
          {
            idOfTreatedCluster += newClusterID
            toGatherCentroids(id) = (id, closestClusterID, vector, newClusterSize, originalClusterID)
          }
          if( sizecurrent + closestClusterSize >= cmin ) indexOfSmallerClusters --= idOfTreatedCluster
        }
        else indexOfSmallerClusters -= idxx
      }      
    }


    val finalCentroids = sc.broadcast(toGatherCentroids.toArray)
    val oldToNewLabelMapBC = sc.broadcast(oldToNewLabelMap)

    val ystarLabeledUpdatedRDD = rdd_Ystar_labeled.map{ case(clusterID, (id, originalVector, mod)) => (oldToNewLabelMapBC.value(clusterID), (id, originalVector, mod)) }
  
    val rddf = ystarLabeledUpdatedRDD.map{ case(clusterID, (id, originalVector, mod)) => {
      var cpt4 = 0
      val (_, newClusterID, _, _, originalClusterID) = finalCentroids.value(cpt4)
      while ( clusterID != originalClusterID ) cpt4 += 1
      (newClusterID, (id, originalVector))
    } }
  
    val partitionedRDDF = rddf.map{ case(clusterID, (id, originalVector)) => (clusterID,(originalVector.toArray)) }.partitionBy(new HashPartitioner(dp)).cache
    val clustersCardinalities = partitionedRDDF.countByKey
    val centroidF = partitionedRDDF.reduceByKey(_ + _)
                        .map{ case(clusterID, reducedVectors) => (clusterID, Vectors.dense(reducedVectors.map(_ / clustersCardinalities(clusterID)))) }
    
    val centroidMap = if( normalisation ) Fcts.descaleRDDcentroid(centroidF, maxMinArray).collect.toMap else centroidF.collect.toMap

    rdd_Ystar_labeled.unpersist()
    val msmodel = new Mean_shift_lsh_model(centroidMap, rddf, maxMinArray)
    //rddf.unpersist()
    msmodel
    
  }

  for( ind00 <- 0 until nbLabelIter) {
    models += labelizing()    
    if( ind00 == nbLabelIter - 1 ) tabHash0.destroy
  }

  readyToLabelization.unpersist()
  models
  } 
}

object MsLsh {

  /**
   * Trains a MS-LSH model using the given set of parameters.
   *
   * @param sc : SparkContext`
   * @param data : an RDD[(String,Vector)] where String is the ID and Vector the rest of data
   * @param k : number of neighbours to look at during gradient ascent
   * @parem threshold_cluster1 : threshold under which we give the same label to two points
   * @parem threshold_cluster2 : threshold under which we give the same label to two close clusters
   * @param yStarIter : Number of iteration for modes search
   * @param cmin : threshold under which we fusion little cluster with the nearest cluster
   * @param normalisation : Normalise the dataset (it is recommand to have same magnitude order beetween features)
   * @param w : regularisation term, default = 1
   * @param nbseg : number of segment on which we project vectors ( make sure it is big enought )
   * @param nbblocs1 : number of buckets used to compute modes
   * @param nbblocs2 : number of buckets used to fusion clusters
   *
   */

  def train(sc:SparkContext, data:RDD[(Long,Vector)], k:Int, threshold_cluster1:Double, threshold_cluster2:Double, yStarIter:Int, cmin:Int, normalisation:Boolean, w:Int, nbseg:Int, nbblocs1:Int, nbblocs2:Int, nbLabelIter:Int) : ArrayBuffer[Mean_shift_lsh_model] =
      new MsLsh().set_k(k).set_threshold_cluster1(threshold_cluster1).set_threshold_cluster2(threshold_cluster2).set_yStarIter(yStarIter).set_cmin(cmin).set_boolnorm(normalisation).set_w(w).set_nbseg(nbseg).set_nbblocs1(nbblocs1).set_nbblocs2(nbblocs2).set_nbLabelIter(nbLabelIter).run(sc, data)

  /**
   * Restore RDD original value
   */
  val descaleRDD = (rdd1:RDD[(Int, (Long, Vector))], maxMinArray0:Array[Array[Double]]) => 
  {
    val vecttest = rdd1.first()._2._2
    val size1 = vecttest.size
    val maxArray = maxMinArray0(0)
    val minArray = maxMinArray0(1)
    val rdd2 = rdd1.map{ case(clusterID, (id, vector)) => {
      var tabcoord = Array.empty[Double]
      for( ind0 <- 0 until size1) {
        val coordXi = vector(ind0) * (maxArray(ind0) - minArray(ind0)) + minArray(ind0)
        tabcoord = tabcoord :+ coordXi
      }
      (clusterID, id, Vectors.dense(tabcoord))          
    } }
    rdd2
  }

  /**
   * Get result for image analysis
   * Results look's like RDD.[ID,Centroïd_Vector,cluster_Number]
   */
  def imageAnalysis(msmodel:Mean_shift_lsh_model) : RDD[(Long, Vector, Int)] = descaleRDD(msmodel.labelizedRDD, msmodel.maxMinArray).map{ case(clusterID, id, _) => (id,msmodel.clustersCenter(clusterID), clusterID) }

  /**
   * Save result for an image analysis
   * Results look's like RDD[ID,Centroïd_Vector,cluster_Number]
   */
  def saveImageAnalysis(msmodel:Mean_shift_lsh_model, path:String, numpart:Int=1) : Unit = {
    val rdd_final = descaleRDD(msmodel.labelizedRDD,msmodel.maxMinArray).map(x => (x._2.toInt,msmodel.clustersCenter(x._1),x._1))
    rdd_final.coalesce(numpart).sortBy(_._1).saveAsTextFile(path)  
  }

  /**
   * Get an RDD[ID,cluster_Number]
   */
  def getlabeling(msmodel:Mean_shift_lsh_model) : RDD[(Long, Int)] = msmodel.labelizedRDD.map{ case(clusterID, (id, _)) => (id, clusterID) }

  /**
   * Save labeling as (ID,cluster_Number)
   */
  def savelabeling(msmodel:Mean_shift_lsh_model,path:String,numpart:Int=1) : Unit = {
    msmodel.labelizedRDD.map( x => (x._2._1.toInt,x._1)).sortBy(_._1, true, numpart).saveAsTextFile(path)
  }

  /**
   * Save clusters's label, cardinality and centroid
   */
  def saveClusterInfo(msmodel:Mean_shift_lsh_model,path:String) : Unit = {
    val centroidsWithID = msmodel.clustersCenter.toArray
    val cardClust = msmodel.labelizedRDD.countByKey 
    val strToWrite = centroidsWithID.map{ case(clusterID, centroid) => (clusterID.toInt ,cardClust(clusterID), centroid) }.sortBy(_._1).mkString("\n")
    val fw = new FileWriter(path, true)
    fw.write(strToWrite)
    fw.close
  }

  /*
   * Prediction function which tell in which cluster a vector should belongs to
   */
  def prediction(v:Vector, mapCentroid:Map[Int,Vector]) : Int = mapCentroid.map{ case(clusterID, centroid) => (clusterID,Vectors.sqdist(v, centroid)) }.toArray.sortBy(_._2).head._1
} 


