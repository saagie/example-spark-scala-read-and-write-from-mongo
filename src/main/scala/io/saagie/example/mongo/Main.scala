package io.saagie.example.mongo

import com.mongodb.spark._
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}
import scopt.OptionParser


object Main {

  val logger: Logger = LogManager.getLogger(getClass)

  case class CLIParams(mongoUri: String = "")

  case class Address(building: String, coord: Array[Double], street: String, zipcode: String)

  case class Restaurant(address: Address, borough: String, cuisine: String, name: String)


  def main(args: Array[String]): Unit = {

    val parser = parseArgs("Main")

    parser.parse(args, CLIParams()) match {
      case Some(params) =>
        // Creation of SparkSession
        val sparkSession = SparkSession.builder()
          .appName("example-spark-scala-read-and-write-from-mongo")
          // Configuration for writing in a Mongo collection
          .config("spark.mongodb.output.uri", params.mongoUri)
          .config("spark.mongodb.output.collection", "restaurants")
          // Configuration for reading a Mongo collection
          .config("spark.mongodb.input.uri", params.mongoUri)
          .config("spark.mongodb.input.collection", "restaurants")
          // Type of Partitionner to use to transform Documents to dataframe
          .config("spark.mongodb.input.partitioner", "MongoPaginateByCountPartitioner")
          // Number of partitions in the resulting dataframe
          .config("spark.mongodb.input.partitionerOptions.MongoPaginateByCountPartitioner.numberOfPartitions", "1")
          .getOrCreate()


        // Creation of SparContext and SQLContext
        import sparkSession.implicits._

        //Creation of a dataframe containing documents
        val dfRestaurants = Seq(Restaurant(Address("1480", Array(-73.9557413, 40.7720266), "2 Avenue", "10075"), "Manhattan", "Italian", "Vella"), Restaurant(Address("1007", Array(-73.856077, 40.848447), "Morris Park Ave", "10462"), "Bronx", "Bakery", "Morris Park Bake Shop")).toDF().coalesce(1)
        // Writing dataframe in Mongo collection
        MongoSpark.save(dfRestaurants.write.mode(SaveMode.Overwrite))
        logger.info("Writing documents in Mongo : OK")

        // Reading Mongodb collection into a dataframe
        val df = MongoSpark.load(sparkSession)
        logger.info(df.show())
        logger.info("Reading documents from Mongo : OK")

      case None =>
      // arguments are bad, error message will have been displayed
    }
  }

  def parseArgs(appName: String): OptionParser[CLIParams] = {
    new OptionParser[CLIParams](appName) {
      head(appName, "1.1")
      help("help") text "prints this usage text"

      opt[String]("mongoUri") required() action { (data, conf) =>
        conf.copy(mongoUri = data)
      } text "URI of mongo. Example : mongodb://username:password@host:27017/database"
    }
  }
}
