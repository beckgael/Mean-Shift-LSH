//import bintray.Keys._

sbtPlugin := true

name := "Neirest-Neighbours-Mean-Shift-LSH"

version := "1.1"

organization := "beck"

//bintrayOrganization := Some("spartakus")

scalaVersion := "2.10.5"

val sparkVersion = "1.6.2"

//resolvers += Resolver.jcenterRepo

//resolvers += Resolver.bintrayRepo("beckgael", "Mean-Shift-LSH")

libraryDependencies ++= Seq(
	"org.apache.spark" %% "spark-core" % sparkVersion,
	"org.apache.spark"  %% "spark-mllib"  % sparkVersion,
	"org.scalatest" % "scalatest_2.10" % "2.2.4" % "test"
)

licenses += ("Apache-2.0", url("https://www.apache.org/licenses/LICENSE-2.0.html"))

credentials += Credentials(Path.userHome / ".bintray" / ".credentials")

publishMavenStyle := true

publishArtifact := true

//bintrayPublishSettings

//repository in bintray := "sbt-plugins"

//publishMavenStyle := false

//bintrayRepository := "bintray-beckgael-Mean-Shift-LSH"

bintrayOrganization in bintray := None