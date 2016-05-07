/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
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
import scala.collection.mutable.ArrayBuffer
import spire.implicits._
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.mllib.rdd.MLPairRDDFunctions._
import org.apache.spark.mllib.util._
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.storage.StorageLevel
import org.apache.spark
import java.io._
/**
 * Mean-Shift-LSH clustering 
 * This algorithm could be used to analyse complex multivariate multidimensional data.
 * It can also be apply in order to analyse image, to use this features it is recommanded to convert image from RGB space to L*u*v* space
 */

/**
 * The class which transform vector to scalar thank's to LSH
 */
case class LshHash extends Serializable  {      
  def hashfunc(x:Vector, w:Double, b:Double, tabHash1:Array[Array[Double]]) : Double = {
    var tabHash = Array.empty[Double]
    val x1 = x.toArray
    for( ind <- tabHash1.indices) {
      var sum = 0.0
      for( ind2 <- x1.indices) {
        sum = sum + (x1(ind2)*tabHash1(ind)(ind2))
        }     
      tabHash =  tabHash :+ (sum+b)/w
    }
    tabHash.reduce(_+_)
  }
}
/**
 * The class which compute centroïds
 */
case class Bary0 extends Serializable {

  /**
   * Function which compute centroïds
   */
  def bary(tab1:Array[(Vector,Double)],k:Int) : Vector = {
    var tab2 = tab1.map(_._1.toArray)
    var bary1 = tab2.reduce(_+_)
    bary1 = bary1.map(_/k)
    Vectors.dense(bary1)
  }
}

/**
 * The major class where MS-LSH algorithm and prediction fonction are implemented
 */
class MsLsh private (
  private var k:Int,
  private var threshold_cluster1:Double,
  private var threshold_cluster2:Double,
  private var yStarIter:Int,
  private var cmin:Int,
  private var normalisation:Boolean,
  private var w:Int,
  private var nbseg:Int,
  private var nbblocs1:Int,
  private var nbblocs2:Int,
  private var nbLabelIter:Int,
  private var numPart:Int ) extends Serializable {  

  def this() = this(50,0.001,0.05,10,0,true,1,100,100,50,5,100)
  
  /**
   * Set number of partition for coalesce and partioner
   */
  def set_numPart(nP:Int) : this.type = { 
    this.numPart = nP
    this
  }
  
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
  def set_w(ww:Int) : this.type = {
    this.w = ww
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
   * Create a tab with random vector where component are taken on normal law N(0,1) for LSH
   */
  def tabHash(nb:Int, dim:Int) = {
    var tabHash0 : Array[Array[Double]] = Array()
    for( ind <- 0 until nb) {
      var vechash1 : Array[Double] = Array()
      for( ind <- 0 until dim) {
        val nG = Random.nextGaussian
        vechash1 = vechash1 :+ nG
      }
      tabHash0 = tabHash0 :+ vechash1
    }
  tabHash0
  }
  
  /**
   * Scale data to they fit on range [0,1]
   * Return a tuple where :
   *   First element is the scale rdd
   *   Second element is the array of max value for each component
   *   Third element is the array of min value for each component
   * Theses array are used in order to descale RDD
   */
  def scaleRdd(rdd1:RDD[(String,Vector)]) : (RDD[(String,Vector)],Array[Double],Array[Double]) = {
    val vecttest = rdd1.first()._2
    val size1 = vecttest.size
    var minArray : Array[Double] = Array()
    var maxArray : Array[Double] = Array()
    for( ind0 <- 0 until size1) {
      var vectmin = rdd1.takeOrdered(1)(Ordering[(Double)].on(x => x._2(ind0)))
      var vectmax = rdd1.top(1)(Ordering[(Double)].on(x => x._2(ind0)))
      val min0 = vectmin(0)._2(ind0)
      val max0 = vectmax(0)._2(ind0)
      minArray = minArray :+ min0
      maxArray = maxArray :+ max0
    }
    val rdd2 = rdd1.map( x => {
      var tabcoord : Array[Double] = Array()
      for( ind0 <- 0 until size1) {
        val coordXi = (x._2(ind0)-minArray(ind0))/(maxArray(ind0)-minArray(ind0))
        tabcoord = tabcoord :+ coordXi
      }
      (x._1,Vectors.dense(tabcoord))
    })
    (rdd2,maxArray,minArray)
  }

  /**
   * Restore centroid's original value
   */
  def descaleRDDcentroid(rdd1:RDD[(String,Vector)],maxMinArray0:Array[Array[Double]]) : RDD[(String,Vector)] = {
    val vecttest = rdd1.first()._2
    val size1 = vecttest.size
    val maxArray = maxMinArray0(0)
    val minArray = maxMinArray0(1)
    val rdd2 = rdd1.map( x => {
      var tabcoord : Array[Double] = Array()
      for( ind0 <- 0 until size1) {
        val coordXi = x._2(ind0)*(maxArray(ind0)-minArray(ind0))+minArray(ind0)
        tabcoord = tabcoord :+ coordXi
      }
      (x._1,Vectors.dense(tabcoord))         
    })
    rdd2
  }


  /**
   * Mean Shift LSH accomplish his clustering work
   */
  def run( data:RDD[(String,Vector)], sc:SparkContext ) : ArrayBuffer[Mean_shift_lsh_model] = {

    var data0 : RDD[(String,Vector)] = sc.emptyRDD
    /**
    * Initialisation 
    */
    data.cache
    val size =  data.count().toInt
    var maxMinArray : Array[Array[Double]] = Array()
    var dim = 0
    val maxK = (size/nbblocs1).toInt -1 
  
    if (size < cmin) { throw new IllegalStateException("Exception : cmin > data size") }
    if (maxK <= k) { throw new IllegalStateException("Exception : You set a too high K value") }
    /**
    * Dataset Normalisation
    */
    if (normalisation) { 
      val parsedData00 = scaleRdd(data)
      data0 = parsedData00._1
      maxMinArray = Array(parsedData00._2,parsedData00._3) 
      dim = data0.first._2.size
    }

    else {
      dim = data.first._2.size
      data0 = data
    }
    
    /**
    * Gradient ascent/Research of Y* 
    */
    val hasher0 = new LshHash
    val centroider0 = new Bary0

    val ww = sc.broadcast(w)
    val b = sc.broadcast(Random.nextDouble * w )
    val tabHash0 = sc.broadcast(tabHash(nbseg,dim))
    val hasher = sc.broadcast(hasher0)
    val centroider = sc.broadcast(centroider0)
  
    var rdd_LSH = data0.map(x => (x._1,x._2,x._2,hasher.value.hashfunc(x._2,ww.value,b.value,tabHash0.value)))
                        .repartition(nbblocs1)
    var rdd_res : RDD[(String,Vector,Vector,Double)] = sc.emptyRDD
    data.unpersist()
    rdd_LSH.cache.foreach(x=>{})

    val deb1 = System.nanoTime
   
    for( ind <- 1 to yStarIter  ) {
      val rdd_LSH_ord =  rdd_LSH.sortBy(_._4).mapPartitions( x => {
        val bucket = x.toArray
        bucket.map(y => {
          val array2 = bucket.map( w => (w._2,Vectors.sqdist(y._3,w._2)))
          quickSort(array2)(Ordering[(Double)].on(_._2))
          (y._1,y._2,centroider.value.bary(array2.take(k),k))
        }).iterator
      }
      ,true)
      if(ind < yStarIter){
        val rdd_LSH_unpersist = rdd_LSH
        rdd_LSH = rdd_LSH_ord.map(x => (x._1,x._2,x._3,hasher.value.hashfunc(x._3,ww.value,b.value,tabHash0.value)))
        rdd_LSH.cache.foreach(x=>{})
        rdd_LSH_unpersist.unpersist()
      }
      // rdd_res[(Index,NewVect,OrigVect,lshValue)]
      else rdd_res = rdd_LSH_ord.map(x => (x._1,x._3,x._2,hasher.value.hashfunc(x._3,ww.value,b.value,tabHash0.value)))
    }

    val rdd00 = rdd_res.sortBy(_._4)
                      .map(x=>(x._1,x._2,x._3))
                      .coalesce(nbblocs2,shuffle=false)
                      .cache

    val fin1 = System.nanoTime
    val res1 = (fin1-deb1)/1e9

    val accum = sc.accumulator(0)
    val accum2 = sc.accumulator(0)


    def labelizing(rdd1: RDD[(String,Vector,Vector)]) : Mean_shift_lsh_model = {
      
      val rdd0 = rdd1.mapPartitionsWithIndex( (ind,it) => {
        var stop = 1
        var labeledData = ArrayBuffer.empty[(String,(String,Vector,Vector))]
        var bucket = it.toBuffer
        accum2 += bucket.size
        var vector1 = bucket(Random.nextInt(bucket.size))._2
        var ind1 = (ind+1)*10000
        while ( stop != 0 ) {
            val rdd_Clust_i_ind = bucket.filter( x => { Vectors.sqdist(x._2,vector1) <= threshold_cluster1 })
            val rdd_Clust_i2_ind = rdd_Clust_i_ind.map( x => (ind1.toString,x))
            labeledData ++= rdd_Clust_i2_ind
            // We keep Y* whose distance is greather than threshold
            bucket --= rdd_Clust_i_ind
            stop = bucket.size.toInt
            accum2 += bucket.size
            if(stop != 0) { vector1 = bucket(Random.nextInt(bucket.size))._2 }
            ind1 += 1
            accum += 1
        }
        labeledData.toIterator
      })

      /**
      * Gives Y* labels to original data
      */
      val rdd_Ystar_labeled = rdd0.map(x=>(x._1,(x._2._1,x._2._3,x._2._2)))
                                  .partitionBy(new spark.HashPartitioner(numPart)).cache
      
      val numElemByCLust = rdd_Ystar_labeled.countByKey.toArray
      quickSort(numElemByCLust)(Ordering[(Int)].on(_._1.toInt))

      var centroidArray = rdd_Ystar_labeled.map(x=>(x._1,x._2._2.toArray)).reduceByKey(_+_).collect
      quickSort(centroidArray)(Ordering[(Int)].on(_._1.toInt))

      var centroidArray1 = ArrayBuffer.empty[(String,Vector,Int)]

      // Form the array of clusters centroids
      for( ind <- 0 until numElemByCLust.size) {
        centroidArray1 += (( numElemByCLust(ind)._1,
                            Vectors.dense(centroidArray(ind)._2.map(_/numElemByCLust(ind)._2)),
                            numElemByCLust(ind)._2.toInt
                          ))
      }

      /**
       * Fusion cluster with centroid < threshold
       */

      var stop2 = 1
      var vector3 = centroidArray1(0)._2
      var newIndex = 0
      var centroidArray2 = ArrayBuffer.empty[(String,Vector)]
      val numElemByCLust2 = ArrayBuffer.empty[(String,Int)]
      val oldToNewLabelMap = scala.collection.mutable.Map.empty[String,String]
      while (stop2 != 0) {
        val closeClusters = centroidArray1.filter( x => { Vectors.sqdist(x._2,vector3) <= threshold_cluster2 })
        // We compute the mean of the cluster
        val newCluster0 = closeClusters.map(x=>(x._2.toArray,x._3))
                                        .reduce( (a,b) => (a._1+b._1,a._2+b._2) )
        centroidArray2 += ( (newIndex.toString, Vectors.dense(newCluster0._1.map(_/closeClusters.size)) ) )
        numElemByCLust2 += ( (newIndex.toString, newCluster0._2) )
        for( ind2 <- 0 until closeClusters.size) {
          oldToNewLabelMap += (closeClusters(ind2)._1 -> newIndex.toString)
        }
        centroidArray1 --= closeClusters
        // We keep Y* whose distance is greather than threshold
        stop2 = centroidArray1.size
        if(stop2 != 0) { vector3 = centroidArray1(0)._2 }
        newIndex += 1
      }

      /**
      * Fusion of cluster which cardinality is smaller than cmin 
      */
      var tab_inf_cmin = numElemByCLust2.filter( _._2 <= cmin)
      var stop_size = tab_inf_cmin.size
      var tab_ind_petit = tab_inf_cmin.map(_._1).toBuffer
      val map_ind_all = numElemByCLust2.toMap
      val tabbar00 = centroidArray2.zipWithIndex.map(x=>(x._2,x._1._1,x._1._2,map_ind_all(x._1._1),x._1._1)).toArray
      var tabbar01 = tabbar00.toBuffer
    
      while(tab_ind_petit.size != 0) {
        for (cpt2 <- tabbar01.indices) {
        if(tabbar01(cpt2)._4 < cmin){
          val labelcurrent = tabbar01(cpt2)._2
          val sizecurrent = tabbar01(cpt2)._4
          var tabproche0 = tabbar01.map(y=>(Vectors.sqdist(y._3,tabbar01(cpt2)._3),y._1,y._2,y._4)).toArray
          quickSort(tabproche0)(Ordering[(Double)].on(_._1))
          var cpt3 = 1
          while (tabproche0(cpt3)._3 == labelcurrent) {cpt3 += 1}
          val plusproche = tabproche0(cpt3)
          val labelplusproche = plusproche._3
          val sizeplusproche = plusproche._4
          val tab00 = tabbar01.filter(x=> x._2==labelplusproche)
          val tab01 = tabbar01.filter(x=> x._2==labelcurrent)
          var tabind0 = ArrayBuffer.empty[String]
          // Update
          for( ind8 <- tab00.indices) {
          tabind0 = tabind0 :+ tab00(ind8)._2         
          tabbar01.update(tab00(ind8)._1,(tab00(ind8)._1,labelplusproche,tab00(ind8)._3,sizeplusproche+sizecurrent,tab00(ind8)._5))
          }
          for( ind8 <- tab01.indices) {
          tabind0 = tabind0 :+ tab01(ind8)._2         
          tabbar01.update(tab01(ind8)._1,(tab01(ind8)._1,labelplusproche,tab01(ind8)._3,sizeplusproche+sizecurrent,tab01(ind8)._5))
          }
          if(sizecurrent+sizeplusproche >= cmin){ tab_ind_petit --= tabind0 }
        }
        else { tab_ind_petit -= tabbar01(cpt2)._1.toString }
        }       
      }


      val tabbar000 = sc.broadcast(tabbar01.toArray)
      val oldToNewLabelMapb = sc.broadcast(oldToNewLabelMap)

      val rdd_Ystar_labeled2 = rdd_Ystar_labeled.map(x=>(oldToNewLabelMapb.value(x._1),x._2))
    
      val rddf = rdd_Ystar_labeled2.map(x=>{
        var cpt4 = 0
        while ( x._1 != tabbar000.value(cpt4)._5) {cpt4 += 1}
        (tabbar000.value(cpt4)._2.toString,(x._2._1,x._2._2))
      }).cache
    
      val k0 = rddf.countByKey
      var numClust_Ystarer = k0.size 
      val centroidF = rddf.map(x=>(x._1,(x._2._2.toArray))).reduceByKey(_+_)
                          .map(x=>(x._1,Vectors.dense(x._2.map(_/k0(x._1)))))
      
      val centroidMap = descaleRDDcentroid(centroidF,maxMinArray).collect.toMap

      rdd_Ystar_labeled.unpersist()
      val msmodel = new Mean_shift_lsh_model(centroidMap,rddf,maxMinArray)
      //rddf.unpersist()
      msmodel
      
    }

  var models = ArrayBuffer.empty[Mean_shift_lsh_model]

  val deb2 = System.nanoTime

  for( ind00 <- 0 until nbLabelIter) {
    models += labelizing(rdd00)    
    if(ind00 == 0) {
      ww.destroy()
      b.destroy()
      tabHash0.destroy()
      hasher.destroy()
      centroider.destroy()
    }
  }

  val params = "size : " + size.toString + "\n" +
           "k: " + k.toString + "\n" +
           "threshold_cluster1 : " + threshold_cluster1.toString + "\n" + 
           "threshold_cluster2 : " + threshold_cluster2.toString + "\n" +
           "cmin : " + cmin.toString + "\n" + 
           "nbblocs1 : " + nbblocs1.toString + "\n" + 
           "nbblocs2 : " + nbblocs2.toString + "\n" +
           "nbseg : " + nbseg.toString + "\n" + 
           "yStarIter : " + yStarIter.toString + "\n" + 
           "normalisation : " + normalisation.toString + "\n" +
           "w : " + w.toString + "\n" +
           "iterMoyLabeling : " + accum.value.toDouble/(nbblocs2*nbLabelIter) + "\n" +
           "nbElemParcouru : " + accum2.value.toDouble/nbLabelIter + "\n"

  val fin2 = System.nanoTime
  val rep = ((fin2-deb2)/1e9)/nbLabelIter
  val stats1 = params + "Montée de gradient (s) : " + res1.toString + "\n" +
               "Labélisation (s) : " + rep.toString

  val file = new File("upgrade/3200k1")
  val bw = new BufferedWriter(new FileWriter(file))
  bw.write(stats1 + "\n")
  bw.close()
  
  rdd00.unpersist()
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
   * @param nbPart : Level of parallelism outside the gradient ascent
   *
   */

  def train(
    sc:SparkContext,
    data:RDD[(String,Vector)],
    k:Int,
    threshold_cluster1:Double,
    threshold_cluster2:Double,
    yStarIter:Int,
    cmin:Int,
    normalisation:Boolean,
    w:Int,
    nbseg:Int,
    nbblocs1:Int,
    nbblocs2:Int,
    nbLabelIter:Int,
    nbPart:Int) : ArrayBuffer[Mean_shift_lsh_model] = {
      new MsLsh().set_k(k)
        .set_threshold_cluster1(threshold_cluster1)
        .set_threshold_cluster2(threshold_cluster2)
        .set_yStarIter(yStarIter)
        .set_cmin(cmin)
        .set_boolnorm(normalisation)
        .set_w(w)
        .set_nbseg(nbseg)
        .set_nbblocs1(nbblocs1)
        .set_nbblocs2(nbblocs2)
        .set_nbLabelIter(nbLabelIter)
        .set_numPart(nbPart)
        .run(data,sc)
  }

  /**
   * Restore RDD original value
   */
  def descaleRDD(rdd1:RDD[(String,(String,Vector))],maxMinArray0:Array[Array[Double]]) : RDD[(String,String,Vector)] = {
    val vecttest = rdd1.first()._2._2
    val size1 = vecttest.size
    val maxArray = maxMinArray0(0)
    val minArray = maxMinArray0(1)
    val rdd2 = rdd1.map( x => {
      var tabcoord = Array.empty[Double]
      for( ind0 <- 0 until size1) {
        val coordXi = x._2._2(ind0)*(maxArray(ind0)-minArray(ind0))+minArray(ind0)
        tabcoord = tabcoord :+ coordXi
      }
      (x._1,x._2._1,Vectors.dense(tabcoord))          
    })
    rdd2
  }

  /**
   * Get result for image analysis
   * Results look's like RDD.[ID,Centroïd_Vector,cluster_Number]
   */
  def imageAnalysis(msmodel:Mean_shift_lsh_model) : RDD[(Int,Vector,String)] = {
    val rddf = descaleRDD(msmodel.rdd,msmodel.maxMinArray)
    val rdd_final = rddf.map(x=>(x._2.toInt,msmodel.clustersCenter(x._1),x._1)) 
    rdd_final
  }

  /**
   * Save result for an image analysis
   * Results look's like RDD[ID,Centroïd_Vector,cluster_Number]
   */
  def saveImageAnalysis(msmodel:Mean_shift_lsh_model, folder:String,numpart:Int=1) : Unit = {
    val rdd_final = descaleRDD(msmodel.rdd,msmodel.maxMinArray).map(x=>(x._2.toInt,msmodel.clustersCenter(x._1),x._1))
    rdd_final.coalesce(numpart,true).sortBy(_._1).saveAsTextFile(folder)  
  }

  /**
   * Get an RDD[ID,cluster_Number]
   */
  def getlabeling(msmodel:Mean_shift_lsh_model) : RDD[(String,String)] = {
    val rddf = msmodel.rdd.map(x=>(x._2._1,x._1))
    rddf
  }

  /**
   * Save labeling as (ID,cluster_Number)
   */
  def savelabeling(msmodel:Mean_shift_lsh_model,folder:String,numpart:Int=1) : Unit = {
    msmodel.rdd.map(x=>(x._2._1.toInt,x._1)).sortBy(_._1,true,numpart).saveAsTextFile(folder)
  }

  /**
   * Save clusters's label, cardinality and centroid
   */
  def saveClusterInfo(sc1:SparkContext, msmodel:Mean_shift_lsh_model,folder:String) : Unit = {
    val array1 = msmodel.clustersCenter.toArray
    val cardClust = msmodel.rdd.countByKey 
    val rdd1 = sc1.parallelize(array1,1).map(x=>(x._1,cardClust(x._1),x._2))
    rdd1.sortBy(_._1.toInt).saveAsTextFile(folder)
  }

  /*
   * Prediction function which tell in which cluster a vector should belongs to
   */
  def prediction(v:Vector,mapCentroid:Map[String,Vector]) : String = {
    val distC = mapCentroid.map(x=>(x._1,Vectors.sqdist(v,x._2)))
    val tabC = distC.toArray
    quickSort(tabC)(Ordering[Double].on(_._2))
    tabC(0)._1
  }
} 


