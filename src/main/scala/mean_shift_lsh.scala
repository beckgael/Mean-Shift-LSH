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
package msLsh

import scala.util.Random
import scala.collection.mutable.{ArrayBuffer, ListBuffer, HashMap, HashSet}
import spire.implicits._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, HashPartitioner}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.mllib.util._
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.feature.StandardScaler
import java.io.FileWriter
/**
 * @author Beck Gaël
 * Mean-Shift-LSH clustering 
 * This algorithm could be used to analyse complex multivariate multidimensional data.
 * It can also be apply in order to analyse image, to use this features it is recommanded to convert image from RGB space to L*u*v* space
 * The major class where MS-LSH algorithm and prediction fonction are implemented
 */
class MsLsh (var k: Int, var epsilon1: Double, var epsilon2: Double, var epsilon3: Double, var ratioToStop: Double, var yStarIter: Int, var cmin: Int, var normalisation: Boolean, var w: Double, var nbseg: Int, var nbblocs1: Int, var nbblocs2: Int, var nbLabelIter: Int) extends Serializable {
  
  /**
   * Mean Shift LSH accomplish his clustering work
   */
  def run(sc: SparkContext, data: RDD[(Long, Vector)]) : ArrayBuffer[Mean_shift_lsh_model] = 
  {
    /**
    * Initialisation 
    */
    data.cache
    val size =  data.count.toInt
    val maxK = (size / nbblocs1).toInt -1 
    val dp = sc.defaultParallelism

    if (size < cmin) throw new IllegalStateException("Exception : cmin > data size")
    if (maxK <= k) throw new IllegalStateException("Exception : You set a too high K value")
    /**
    * Dataset Normalisation
    */
    val (normalizedOrNotRDD, maxArray, minArray) = if( normalisation ) Fcts.scaleRdd(data) else (data, Array.empty[Double], Array.empty[Double])    
    val dim = normalizedOrNotRDD.first._2.size
    val maxMinArray = (maxArray, minArray) 
    
    /**
    * Gradient ascent / Research of Y* 
    */
    val b = Random.nextDouble * w
    var hashTab = Fcts.tabHash(nbseg, dim)
  
    val rdd_LSH = normalizedOrNotRDD.map{ case (id, vector) => (id, vector, vector, Fcts.hashfunc(vector, w, b, hashTab), false) }
    data.unpersist(true)
    val lineageRDDs = ArrayBuffer.empty[RDD[(Long, Vector, Vector, Double, Boolean)]]
    lineageRDDs += rdd_LSH
   
    var stopIter = false
    var ind = 0

    // We fix the number of iterations
    if( ratioToStop == 1.0 )
    {
      for( ind2 <- 0 until yStarIter )
      {
        // Increase convergence time
        //hashTab = Fcts.tabHash(nbseg, dim)
        val rdd_LSH_ord =  lineageRDDs(ind).sortBy({ case (_, _, _, hashValue, _) => hashValue}, ascending=true, nbblocs1).mapPartitions(it => {
          val approxKNN = it.toArray
          approxKNN.map{ case (id, originalVector, mod, hashV, stop) => {
            val distKNNFromCurrentPoint = approxKNN.map{ case (_, originalVector2, mod2, hashV2, _) => (originalVector2, Vectors.sqdist(mod, originalVector2)) }.sortBy(_._2)
            val newMod = Fcts.computeCentroid(distKNNFromCurrentPoint.take(k), k)
            (id, originalVector, newMod, stop)
          }}.toIterator
        })
        lineageRDDs += rdd_LSH_ord.map{ case (id, originalVector, mod, stop) => (id, originalVector, mod, Fcts.hashfunc(mod, w, b, hashTab), stop) }
      }
    }
    else
    { 
      while( ind < yStarIter && ! stopIter )
      {
        // Increase convergence time
        //hashTab = Fcts.tabHash(nbseg, dim)
        val rdd_LSH_ord =  lineageRDDs(ind).sortBy({ case (_, _, _, hashValue, _) => hashValue}, ascending=true, nbblocs1).mapPartitions(it => {
          val approxKNN = it.toArray
          approxKNN.map{ case (id, originalVector, mod, hashV, _) => {
            val distKNNFromCurrentPoint = approxKNN.map{ case (_, originalVector2, mod2, hashV2, _) => (originalVector2, Vectors.sqdist(mod, originalVector2)) }.sortBy(_._2)
            val newMod = Fcts.computeCentroid(distKNNFromCurrentPoint.take(k), k)
            val stop = Vectors.sqdist(mod, newMod) <= epsilon1
            (id, originalVector, newMod, stop)
          }}.toIterator
        })
        lineageRDDs += rdd_LSH_ord.map{ case (id, originalVector, mod, stop) => (id, originalVector, mod, Fcts.hashfunc(mod, w, b, hashTab), stop) }.cache
        val unconvergedPoints = lineageRDDs(ind + 1).filter{ case (_, _, _, _, stop) => stop == false }.count
        lineageRDDs(ind).unpersist(false)
        stopIter = unconvergedPoints <= ratioToStop * size
        ind += 1
      }
    }

    val readyToLabelization = if( nbblocs2 == 1 ) lineageRDDs.last.map{ case (id, mod, originalVector, hashV, _) => (id, mod, originalVector)}.coalesce(1)
                              else lineageRDDs.last.sortBy({ case (_, _, _, hashValue, _) => hashValue}, ascending=true, nbblocs2).map{ case (id, mod, originalVector, hashV, _) => (id, mod, originalVector)}
    
    if( nbLabelIter > 1 ) readyToLabelization.cache

    val models = ArrayBuffer.empty[Mean_shift_lsh_model]

    /**
     * Labelization function which gathered mods together into clusters
     */
    val labelizing = () => 
    {
      val clusterByLshBucket = readyToLabelization.mapPartitionsWithIndex( (ind, it) => {
        val labeledData = ListBuffer.empty[(Int, (Long, Vector, Vector))]
        val bucket = it.toBuffer
        var mod1 = bucket(Random.nextInt(bucket.size))._2
        var clusterID = (ind + 1) * 10000
        while ( bucket.size != 0 )
        {
            val closestCentroids = bucket.filter{ case (_, mod, originalVector) => { Vectors.sqdist(mod, mod1) <= epsilon2 } }
            val closestCentroids2 = closestCentroids.map{ case (id, mod, originalVector) => (clusterID, (id, mod, originalVector))}
            labeledData ++= closestCentroids2
            bucket --= closestCentroids
            if( bucket.size != 0 ) mod1 = bucket(Random.nextInt(bucket.size))._2
            clusterID += 1
        }
        labeledData.toIterator
      })

      /**
      * Gives Y* labels to original data
      */
      val rdd_Ystar_labeled = clusterByLshBucket.map{ case (clusterID, (id, mod, originalVector)) => (clusterID, (id, originalVector, mod)) }
                                                .partitionBy(new HashPartitioner(dp))
                                                .cache
      
      val centroidMapOrig = rdd_Ystar_labeled.map{ case (clusterID, (_, originalVector, mod)) => (clusterID , originalVector.toArray) }.reduceByKeyLocally(_ + _)

      val numElemByCLust = rdd_Ystar_labeled.countByKey

      val centroids = ArrayBuffer.empty[(Int, Vector, Long)]

      // Form the array of clusters centroids
      for( (clusterID, cardinality) <- numElemByCLust )
        centroids += ( (clusterID, Vectors.dense(centroidMapOrig(clusterID).map(_ / cardinality)), cardinality) )

      /**
       * Fusion cluster with centroid < threshold
       */

      val newCentroids = ArrayBuffer.empty[(Int, Vector)]
      val numElemByCluster = HashMap.empty[Int, Long]
      val oldToNewLabelMap = HashMap.empty[Int, Int]
      var randomCentroidVector = centroids(Random.nextInt(centroids.size))._2
      var newClusterID = 0
      while ( ! centroids.isEmpty ) {
        val closestClusters = centroids.filter{ case (clusterID, vector, clusterCardinality) => { Vectors.sqdist(vector, randomCentroidVector) <= epsilon3 }}
        // We compute the mean of the cluster
        val gatheredCluster = closestClusters.map{ case (clusterID, vector, clusterCardinality) => (vector.toArray, clusterCardinality) }.reduce( (a, b) => (a._1 + b._1, a._2 + b._2) )
        newCentroids += ( (newClusterID, Vectors.dense(gatheredCluster._1.map(_ / closestClusters.size))) )
        numElemByCluster += ( newClusterID -> gatheredCluster._2 )
        for( (clusterID, _, _) <- closestClusters ) {
          oldToNewLabelMap += ( clusterID -> newClusterID )
        }
        centroids --= closestClusters
        // We keep Y* whose distance is greather than threshold
        if( centroids.size != 0 ) randomCentroidVector = centroids(Random.nextInt(centroids.size))._2
        newClusterID += 1
      }

      /**
      * Fusion of cluster which cardinality is smaller than cmin 
      */
      val clusterIDsOfSmallerOne = numElemByCluster.filter{ case (clusterID, cardinality) => cardinality <= cmin }.keys.toBuffer
      val toGatherCentroids = newCentroids.zipWithIndex.map{ case ((clusterID, vector), id) => (id, clusterID, vector, numElemByCluster(clusterID), clusterID)}//.par
      val littleClusters = toGatherCentroids.filter{ case (_, _, _, cardinality, _) => cardinality <= cmin }.toBuffer

      while( ! clusterIDsOfSmallerOne.isEmpty )
      {
        val (idx, currentClusterID, origVector, sizeCurrent, _) = littleClusters(Random.nextInt(littleClusters.size)) 
        val sortedClosestCentroid = toGatherCentroids.map{ case (id, newClusterID, vector, cardinality, _) => (id, vector, Vectors.sqdist(vector, origVector), newClusterID, cardinality) }.sortBy{ case (_, _, dist, _, _) => dist }
        val (idx2, vector, _, closestClusterID, closestClusterSize) = sortedClosestCentroid.find(_._4 != currentClusterID).get
        var totSize = sizeCurrent + closestClusterSize
        val lookForNN = ArrayBuffer(vector, origVector)
        val oldClusterIDs = ArrayBuffer(currentClusterID, closestClusterID)
        val idxToReplace = HashSet(idx, idx2)
        while( totSize <= cmin )
        {
          val (idxK, vectorK, _, clusterIDK, cardinalityK) = lookForNN.map(v => toGatherCentroids.map{ case (id, newClusterID, vector, cardinality, _) => (id, vector, Vectors.sqdist(vector, origVector), newClusterID, cardinality) }.filter{ case (_, _, _, newClusterID, _) => ! oldClusterIDs.contains(newClusterID) }.sortBy{ case (_, _, dist, _, _) => dist }.head).sortBy{ case (_, _, dist, _, _) => dist }.head
          lookForNN += vectorK
          oldClusterIDs += clusterIDK
          idxToReplace += idxK
          totSize += cardinalityK
        }

        idxToReplace ++= toGatherCentroids.filter{ case (_, newClusterID, _, _, _) => oldClusterIDs.contains(newClusterID) }.map{ case (id, _, _, _, _) => id }

        for( idxR <- idxToReplace )
        {
          val (idR, _, vectorR, cardinalityR, originalClusterIDR) = toGatherCentroids(idxR)
          if( totSize > cmin)
          {
            clusterIDsOfSmallerOne -= originalClusterIDR
            littleClusters -= ( (idR, originalClusterIDR, vectorR, cardinalityR, originalClusterIDR) )
          }
          toGatherCentroids(idxR) = (idxR, closestClusterID, vectorR, totSize, originalClusterIDR)
        }
      }

      val newCentroidIDByOldOneMap = toGatherCentroids.map{ case (id, newClusterID, vector, cardinality, originalClusterID) => (originalClusterID, newClusterID) }.toMap

      val newCentroidIDByOldOneMapBC = oldToNewLabelMap.map{ case (k, v) => (k, newCentroidIDByOldOneMap(v)) }
    
      val partitionedRDDF = rdd_Ystar_labeled.map{ case (clusterID, (id, originalVector, mod)) => (newCentroidIDByOldOneMapBC(clusterID), (id, originalVector)) }.partitionBy(new HashPartitioner(dp))
    
      val partitionedRDDFforStats = partitionedRDDF.map{ case (clusterID, (id, originalVector)) => (clusterID, (originalVector.toArray)) }.cache
      val clustersCardinalities = partitionedRDDFforStats.countByKey
      val centroidF = partitionedRDDFforStats.reduceByKey(_ + _)
                          .map{ case (clusterID, reducedVectors) => (clusterID, Vectors.dense(reducedVectors.map(_ / clustersCardinalities(clusterID)))) }
      
      val centroidMap = if( normalisation ) Fcts.descaleRDDcentroid(centroidF, maxMinArray).collect.toMap else centroidF.collect.toMap

      val msmodel = new Mean_shift_lsh_model(centroidMap, clustersCardinalities, partitionedRDDF, maxMinArray)
      rdd_Ystar_labeled.unpersist(true)
      partitionedRDDFforStats.unpersist(true)
      msmodel 
    }

    for( ind <- 0 until nbLabelIter)
      models += labelizing()    

    readyToLabelization.unpersist(false)
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
   * @param epsilon1 : threshold under which we stop iteration in gradient ascent
   * @param epsilon2 : threshold under which we give the same label to two points
   * @param epsilon3 : threshold under which we give the same label to two close clusters
   * @param ratioToStop : % of data that have NOT converged in order to stop iteration in gradient ascent
   * @param yStarIter : Number of iteration for modes search
   * @param cmin : threshold under which we fusion little cluster with the nearest cluster
   * @param normalisation : Normalise the dataset (it is recommand to have same magnitude order beetween features)
   * @param w : regularisation term, default = 1
   * @param nbseg : number of segment on which we project vectors ( make sure it is big enought )
   * @param nbblocs1 : number of buckets used to compute modes
   * @param nbblocs2 : number of buckets used to fusion clusters
   * @param nbLabelIter : number of iteration for the labelisation step, it determines the number of final models
   *
   */

  def train(sc:SparkContext, data:RDD[(Long,Vector)], k:Int, epsilon1:Double, epsilon2:Double, epsilon3:Double, ratioToStop:Double, yStarIter:Int, cmin:Int, normalisation:Boolean, w:Double, nbseg:Int, nbblocs1:Int, nbblocs2:Int, nbLabelIter:Int) : ArrayBuffer[Mean_shift_lsh_model] =
      new MsLsh(k, epsilon1, epsilon2, epsilon3, ratioToStop, yStarIter, cmin, normalisation, w, nbseg, nbblocs1, nbblocs2, nbLabelIter).run(sc, data)

  /**
   * Restore RDD original value
   */
  val descaleRDD = (toDescaleRDD:RDD[(Int, (Long, Vector))], maxMinArray:(Array[Double], Array[Double])) => 
  {
    val vecttest = toDescaleRDD.first._2._2
    val size1 = vecttest.size
    val maxArray = maxMinArray._1
    val minArray = maxMinArray._2
    toDescaleRDD.map{ case (clusterID, (id, vector)) => {
      var tabcoord = new Array[Double](size1)
      for( ind0 <- 0 until size1 ) {
        val coordXi = vector(ind0) * (maxArray(ind0) - minArray(ind0)) + minArray(ind0)
        tabcoord(ind0) = coordXi
      }
      (clusterID, id, Vectors.dense(tabcoord))          
    } }
  }

  /**
   * Get result for image analysis
   * Results look's like RDD.[ID,Centroïd_Vector,cluster_Number]
   */
  def imageAnalysis(msmodel:Mean_shift_lsh_model) : RDD[(Long, Vector, Int)] = 
    descaleRDD(msmodel.labelizedRDD, msmodel.maxMinArray).map{ case (clusterID, id, _) => (id, msmodel.clustersCenter(clusterID), clusterID) }

  /**
   * Save result for an image analysis
   * Results look's like RDD[ID,Centroïd_Vector,cluster_Number]
   */
  def saveImageAnalysis(msmodel:Mean_shift_lsh_model, path:String, numpart:Int=1) : Unit =
    descaleRDD(msmodel.labelizedRDD, msmodel.maxMinArray).map{ case (clusterID, id, vector) => (id, msmodel.clustersCenter(clusterID), clusterID) }.sortBy(_._1, ascending=true, numpart).saveAsTextFile(path)

  /**
   * Get an RDD[ID,cluster_Number]
   */
  def getlabeling(msmodel:Mean_shift_lsh_model) : RDD[(Long, Int)] = msmodel.labelizedRDD.map{ case (clusterID, (id, _)) => (id, clusterID) }

  /**
   * Save labeling as (ID,cluster_Number)
   */
  def savelabeling(msmodel:Mean_shift_lsh_model, path:String, numpart:Int=1) =
    msmodel.labelizedRDD.map{ case (clusterID, (id, vector)) => (id, clusterID) }.sortBy(_._1, ascending=true, numpart).saveAsTextFile(path)

  /**
   * Save clusters's label, cardinality and centroid
   */
  def saveClusterInfo(msmodel:Mean_shift_lsh_model, path:String) : Unit = {
    val centroidsWithID = msmodel.clustersCenter.toArray
    val cardClust = msmodel.clustersCardinalities 
    val strToWrite = centroidsWithID.map{ case (clusterID, centroid) => (clusterID ,cardClust(clusterID), centroid) }.sortBy(_._1).mkString("\n")
    val fw = new FileWriter(path, true)
    fw.write(strToWrite)
    fw.close
  }

  /*
   * Prediction function which tell in which cluster a vector should belongs to
   */
  def prediction(v:Vector, mapCentroid:Map[Int,Vector]) : Int = mapCentroid.map{ case (clusterID, centroid) => (clusterID, Vectors.sqdist(v, centroid)) }.toArray.sortBy(_._2).head._1
}