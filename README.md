Example for readind & writing into Mongodb with Spark Scala
===========================================================

Package for saagie : sbt clean assembly and get the package in target.

Usage in local :

 - sbt clean assembly
 - spark-submit --class=io.saagie.example.moongo.Main example-spark-scala-read-and-write-from-mongo-assembly-1.1.jar  --mongoUri " mongodb://username:password@host:27017/database"

Usage in Saagie :

 - sbt clean assembly (in local, to generate jar file)
 - create new Spark Job
 - upload the jar (target/scala-2.11/example-spark-scala-read-and-write-from-mongo-assembly-1.1.jar)
 - Replace MyClass with the full class name (ex : io.saagie.example.mongo.Main)
 - copy URI from mongo connection details panel replace username and password with corresponding values (you may use environnment variables) and put it as argument after --mongoUri
 - choose Spark 2.1.0
 - choose resources you want to allocate to the job
 - create and launch.
