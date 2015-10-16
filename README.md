# Nearest Neighbours Mean Shift LSH

This algorithm was create during an internship at Laboratoire d'Informatique de Paris Nord with Lebbah Mustapha, Duong Tarn, Azzag Hanene and Beck Gaël.
It's purpose is clustering of multivariate multidimensional dataset, especialy image.
Mean Shift strenght is automatic detection of cluster number and contrary to K-means it can detect non eliptical clusters.

## Recomandations

It is recommand to normalize data unless your data has already features with the same order of magnitude due to distance computation.
The nbseg parameter should be big enough, under 20 results could be wrong.
The nbblocs parameter is the bottleneck of this algorithm, the bigger it is the faster it computate but you risk to loose in quality.

## Image analysis recommandation
In order to do image analysis it is recommand to convert data from RGB to Luv space and adding space index.

## Usage
This algorithm is build to work with indexed dataset. Usage is preety simple. Prepare your parsed dataset giving him index and rest of data.

```scala

  val sc = new SparkContext(conf)
  val defp = sc.defaultParallelism
  val meanShift = msLsh.MsLsh
  val data = sc.textFile("mydataIndexed.csv",numPart)
  var parsedData = data.map(x => x.split(',')).map(y => new Data_Object(y(0),Vectors.dense(y.tail.map(_.toDouble)))).cache
  
  val model = meanShift.train(  sc,
                          parsedData,
                          k=60,
                          threshold_clust=0.05,
                          iterForYstar=10,
                          cmin=0,
                          normalisation=true,
                          w=1,
                          nbseg=100,
                          nbblocs=50,
                          nbPart=defp)  
                          
  // Save result for an image as (ID,Vector,ClusterNumber)
  meanShift.saveImageAnalysis(model,"MyImageResultDirectory",1)

  // Save result as (ID,ClusterNumber)
  meanShift.savelabeling(model,"MyResultDirectory",1)

  // Save centroids result as (NumCluster,cardinality,CentroidVector)
  meanShift.saveCentroid(sc,model,"centroidDirectory")

```

