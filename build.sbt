name := "newcouch-perf-comparison"

organization := "com.aquto"

version := "0.0.1"

scalaVersion := "2.11.2"

parallelExecution in Test := false

resolvers ++= Seq(
  "Scala Tools Repo Releases" at "http://scala-tools.org/repo-releases",
  "Typesafe Repo Releases" at "http://repo.typesafe.com/typesafe/releases/",
  "Twitter" at "http://maven.twttr.com"
)

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % "2.3.7",
  "com.typesafe.akka" %% "akka-testkit" % "2.3.7" % "test",
  "io.reactivex" %% "rxscala" % "0.22.0",
  "com.couchbase.client" % "java-client" % "2.1.1",
  "com.yammer.metrics" % "metrics-core" % "2.1.3"
)

libraryDependencies +=
  "com.couchbase.client" % "couchbase-client" % "1.3.2" excludeAll(
    ExclusionRule(organization = "com.sun.jdmk"),
    ExclusionRule(organization = "com.sun.jmx"),
    ExclusionRule(organization = "org.springframework")
  )

testOptions in Test += Tests.Argument("junitxml")
