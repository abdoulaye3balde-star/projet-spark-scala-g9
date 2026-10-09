ThisBuild / scalaVersion := "2.12.18"
ThisBuild / version      := "1.0.0"
ThisBuild / organization := "com.ecommerce"

val sparkVersion = "3.5.1"

lazy val root = (project in file("."))
  .settings(
    name := "EcommerceAnalytics",
    scalacOptions ++= Seq("-encoding", "UTF-8", "-deprecation", "-feature"),
    libraryDependencies ++= Seq(
      // "provided" : Spark est fourni par spark-submit, il n'est donc pas embarqué dans le JAR
      "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
      "org.apache.spark" %% "spark-sql"  % sparkVersion % "provided",
      "com.typesafe"      % "config"     % "1.4.3",
      "org.scalatest"    %% "scalatest"  % "3.2.18" % Test
    ),
    // Pour `sbt run` en local : remet les dépendances "provided" dans le classpath
    Compile / run := Defaults.runTask(
      Compile / fullClasspath, Compile / run / mainClass, Compile / run / runner
    ).evaluated,
    Compile / mainClass := Some("com.ecommerce.analytics.MainApp"),
    // Java 17 : Spark doit pouvoir ouvrir certains modules internes du JDK
    run / fork  := true,
    Test / fork := true,
    run / javaOptions ++= Seq(
      "--add-opens=java.base/java.lang=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
      "--add-opens=java.base/java.io=ALL-UNNAMED",
      "--add-opens=java.base/java.net=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/java.util=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
      "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
      "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
      "-Xmx3g",
      "-Dfile.encoding=UTF-8"
    ),
    Test / javaOptions ++= (run / javaOptions).value,
    // JAR exécutable : sbt assembly  ->  target/scala-2.12/ecommerce-analytics.jar
    assembly / mainClass       := Some("com.ecommerce.analytics.MainApp"),
    assembly / assemblyJarName := "ecommerce-analytics.jar",
    assembly / test            := {},
    assembly / assemblyMergeStrategy := {
      case "reference.conf"          => MergeStrategy.concat
      case PathList("META-INF", _*)  => MergeStrategy.discard
      case _                         => MergeStrategy.first
    }
  )
