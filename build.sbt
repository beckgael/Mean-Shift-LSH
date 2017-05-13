name := "Neirest-Neighbours-Mean-Shift-LSH"

version := "1.0.5"

organization := "spartakus"

bintrayOrganization := Some("spartakus")

scalaVersion := "2.10.5"

val sparkVersion = "1.4.1"

resolvers += Resolver.bintrayRepo("otherUser", "maven")

libraryDependencies ++= Seq(
	"org.apache.spark" %% "spark-core" % sparkVersion,
	"org.apache.spark"  %% "spark-mllib"  % sparkVersion,
	"org.scalatest" % "scalatest_2.10" % "2.2.4" % "test"
)

licenses += ("Apache-2.0", url("https://www.apache.org/licenses/LICENSE-2.0.html"))

credentials += Credentials(Path.userHome / ".bintray" / ".credentials")

resolvers += Resolver.jcenterRepo

publishMavenStyle := false

bintrayRepository := "neirest-neighbours-mean-shift-lsh"

//bintrayOrganization in bintray := None