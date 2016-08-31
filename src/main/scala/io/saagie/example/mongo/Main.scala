package io.saagie.example.mongo

import org.apache.log4j.LogManager
import scopt.OptionParser
import org.apache.spark.{SparkConf, SparkContext}
import com.mongodb.spark._
import org.apache.spark.sql.SQLContext


object Main{

  val logger = LogManager.getLogger(this.getClass())

  case class CLIParams(mongoUri: String = "")

  case class Address(building: String,coord :Array[Double],street:String, zipcode: String)
  case class Restaurant(address: Address,borough :String,cuisine:String, name: String)


  def main(args: Array[String]): Unit = {

    val parser = parseArgs("Main")

    parser.parse(args, CLIParams()) match {
      case Some(params) =>
    // Configuration of SparkContext
    val conf = { new SparkConf()
      .setAppName("example-spark-scala-read-and-write-from-mongo")
      // Configuration for writing in a Mongo collection
      .set("spark.mongodb.output.uri", params.mongoUri)
      .set("spark.mongodb.output.collection","restaurants")
      // Configuration for reading a Mongo collection
      .set("spark.mongodb.input.uri", params.mongoUri)
      .set("spark.mongodb.input.collection","restaurants")
      // Type of Partitionner to use to transform Documents to dataframe
      .set("spark.mongodb.input.partitioner","MongoPaginateByCountPartitioner")
      // Number of partitions in the resulting dataframe
      .set("spark.mongodb.input.partitionerOptions.MongoPaginateByCountPartitioner.numberOfPartitions","1")
    }

    // Creation of SparContext and SQLContext
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    //Creation of a dataframe containing documents
    val dfRestaurants = Seq(Restaurant(Address("1480",Array(-73.9557413,40.7720266),"2 Avenue","10075"),"Manhattan","Italian","Vella"),Restaurant(Address("1007",Array(-73.856077,40.848447),"Morris Park Ave","10462"),"Bronx","Bakery","Morris Park Bake Shop")).toDF().coalesce(1)
    // Writing dataframe in Mongo collection
    MongoSpark.save(dfRestaurants.write.mode("overwrite"))
    logger.info("Writing documents in Mongo : OK")

    // Reading Mongodb collection into a dataframe
    val df = MongoSpark.load(sqlContext)
    logger.info(df.show())
    logger.info("Reading documents from Mongo : OK")

      case None =>
      // arguments are bad, error message will have been displayed
    }
  }

  def parseArgs(appName: String): OptionParser[CLIParams] = {
    new OptionParser[CLIParams](appName) {
      head(appName, "1.0")
      help("help") text "prints this usage text"

      opt[String]("mongoUri") required() action { (data, conf) =>
        conf.copy(mongoUri = data)
      } text "URI of mongo. Example : mongodb://username:password@host:27017/database"
    }
  }
}
