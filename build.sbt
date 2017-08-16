name := "Mean-Shift-LSH"

version := "1.1"

organization := "Clustering4Ever"

bintrayRepository := "Clustering4Ever"

//bintrayOrganization := Some("clustering4ever")

scalaVersion := "2.11.8"

val sparkVersion = "2.1.1"

libraryDependencies ++= Seq(
	"org.apache.spark" %% "spark-core" % sparkVersion % "provided",
	"org.apache.spark"  %% "spark-mllib"  % sparkVersion % "provided",
	"org.scalatest" % "scalatest_2.10" % "2.2.4" % "test"
)

licenses += ("Apache-2.0", url("https://www.apache.org/licenses/LICENSE-2.0.html"))

bintrayOrganization in bintray := None

credentials += Credentials(Path.userHome / ".bintray" / ".credentials")