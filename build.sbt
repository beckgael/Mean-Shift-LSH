name := "Mean-Shift-LSH"

version := "1.1"

//organization := "spartakus"

//bintrayOrganization := Some("spark-clustering-notebook")

scalaVersion := "2.10.5"

val sparkVersion = "1.6.2"

libraryDependencies ++= Seq(
	"org.apache.spark" %% "spark-core" % sparkVersion % "provided",
	"org.apache.spark"  %% "spark-mllib"  % sparkVersion % "provided",
	"org.scalatest" % "scalatest_2.10" % "2.2.4" % "test"
)

licenses += ("Apache-2.0", url("https://www.apache.org/licenses/LICENSE-2.0.html"))

bintrayOrganization in bintray := None

credentials += Credentials(Path.userHome / ".bintray" / ".credentials")