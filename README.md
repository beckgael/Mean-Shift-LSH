# Distributed Nearest Neighbours Mean Shift with Locality Sensitive Hashing DNNMS-LSH

This algorithm was created during an internship at Computer Science Laboratory (Laboratoire d'Informatique de Paris Nord, LIPN) at the University of Paris 13, with Lebbah Mustapha, Duong Tarn, Azzag Hanene and Beck Gaël. Its purpose is to provide an efficient distributed implementation to cluster large multivariate multidimensional data sets (Big Data)  Nearest neighbor mean shift (NNMS) defines clusters in terms of locally density regions in the data density. The main advantages of NNMS are that it can **detect automatically** the number of clusters in the data set and **detect non-ellipsoidal** clusters, in contrast to k-means clustering. Exact nearest neighbors calculations in the standard NNMS prevent from being used on Big Data so we introduce approximate nearest neighbors via Locality Sensitive Hashing (LSH), which are based on random scalar projections of the data. To further improve the scalability, we implement NNMS-LSH in the distributed Spark/Scala ecosystem.      

### Parameters

* **k** is the number of neighbours to look at in order to compute centroid.
* **nbseg**  is the number of segments on which the data vectors are projected during LSH. Its value should usually be larger than 20, depending on the data set.
* **nbblocs1**  is a crucial parameter as larger values give faster but less accurate LSH approximate nearest neighbors, and as smaller values give slower but more accurate approximations.
* **nbblocs2**  as larger values give faster but less accurate LSH approximate nearest neighbors, and as smaller values give slower but more accurate approximations.
* **cmin**  is the threshold under which clusters with fewer than cmin members are merged with the next nearest cluster.
* **normalisation** is a flag if the data should be first normalized (X-Xmin)/(Xmax-Xmin)  before clustering.
* **w** is a uniformisation constant for LSH.
* **npPart** is the default parallelism outside the gradient ascent.
* **yStarIter** is the maximum number of iterations in the gradient ascent in the mean shift update.
* **threshold_cluster1** is the threshold under which two final mean shift iterates are considered to be in the same cluster.
* **threshold_cluster2** is the threshold under which two final clusters are considered to be the same.

## Usage

### Multivariate multidimensional clustering
Unlike the image analysis which has a specific data pre-processing before the mean shift, for general multivariate multidimensional data sets, it is recommended to normalize data so that each variable has a comparable magnitude to the other variables to improve the performance in the distance matrix computation used to determine nearest neighbors.

### Image analysis

To carry out image analysis, it is recommended to convert the usual color formats (e.g. RGB, CYMK) to the L*u*v* color space as the close values in the L*u*v*-space correspond more to visual perceptions of color proximity, as well adding the row and column indices (x,y). Each pixel is transformed to a 5-dimensional vector (x,y,L*, u*, v*) which is then input into the mean shift clustering. 
```scala

  val sc = new SparkContext(conf)
  val defp = sc.defaultParallelism
  val meanShift = msLsh.MsLsh
  val data = sc.textFile("myIndexedData.csv",defp)
  val parsedData = data.map(x => x.split(',')).map(y => (y(0),Vectors.dense(y.tail.map(_.toDouble)))).cache
  
  val model = meanShift.train(  sc,
                          parsedData,
                          k=60,
                          threshold_cluster1=0.05,
                          threshold_cluster2=0.05,
                          yStarIter=10,
                          cmin=0,
                          normalisation=true,
                          w=1,
                          nbseg=100,
                          nbblocs1=50,
                          nbblocs2=50,
                          nbLabelIter=1,
                          nbPart=defp)  
                          
  // Save result for an image as (ID, Vector, ClusterNumber)
  meanShift.saveImageAnalysis(model, "MyImageResultDirectory",1)

  // Save result as (ID, ClusterNumber)
  meanShift.savelabeling(model, "MyResultDirectory", 1)

  // Save centroids result as (NumCluster, cardinality, CentroidVector)
  meanShift.saveClusterInfo(sc, model, "centroidDirectory")

```

#### Image segmentation example

The picture on top left corner is the #117 from Berkeley Segmentation Dataset and Benchmark repository. Others are obtained with :
* **nbblocs1** : 200 (top right) , 500 (bottom left), 1000 (bottom right) 
* **nbblocs2** : 1
* **k** : 50
* **threshold_cluster1** : 0.05
* **threshold_cluster2** : 0.05 // doesn't matter with nbblocs2 = 1
* **yStarIter** : 10
* **nbLabelIter** : 1
* **w** : 1
* **cmin** : 200


![alt text][logo]

[logo]: http://img11.hostingpics.net/pics/393309flower.png

## Maintenance
Please feel free to report any bugs, recommendations or requests to beck.gael@gmail.com to improve this algorithm.
