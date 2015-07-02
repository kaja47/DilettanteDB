
scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
	"org.scala-lang" % "scala-reflect" % "2.11.7",
	"org.scala-lang" % "scala-compiler" % "2.11.7"
)

javaOptions += "-Xmx8G"

javaHome := Some(file("/usr/lib/jvm/java-8-openjdk-amd64/"))

fork := true
