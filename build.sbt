name := "Neirest Neighbours Mean Shift LSH"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
	"org.apache.spark" %% "spark-core" % "1.5.1" % "provided",
	"org.apache.spark"  %% "spark-mllib"  % "1.5.1",
	"org.scalatest" % "scalatest_2.10" % "2.2.4" % "test"
)
