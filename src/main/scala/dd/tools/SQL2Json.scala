package dd.tools

import dd.producers.{MySqlProducerConfig, MysqlProducer}

import java.io.{BufferedWriter, FileWriter}
import scala.io.Source
import scala.util.matching.Regex
import scala.util.{Failure, Success, Try}

object SQL2Json extends App {
  private def usage(): Unit = {
    System.err.println("Export all SQL result records into a json file")
    System.err.println("usage: SQL2Json <options>")
    System.err.println("options:")
    System.err.println("\t-mySqlHost=<host>       MySQL server host address")
    System.err.println("\t-mySqlPort=<int>        MySQL server port")
    System.err.println("\t-mySqlUser=<str>        MySQL database user")
    System.err.println("\t-mySqlPassword=<str>    MySQL database password")
    System.err.println("\t-mySqlDbname=<str>      MySQL database name")
    System.err.println("\t-sqlf=<name>            File having the sql statement")
    System.err.println("\t-outJsonFile=<path>     Path to the output Json File")
    System.err.println("\t[-jsonFieldFile=<path>] Path to the configuration Json file that has in each line the pattern:  <column name>=<json field name>-><new json field name>")
    System.err.println("\t                        If the <json field name> does not exist in a record or the content is an json array the resultant field name will be <column name>")
    System.err.println("\t[-sqlEncoding=<str>]    The sql file character encoding. Default is 'utf-8'")
    System.exit(1)
  }

  private val parameters: Map[String, String] = args.foldLeft[Map[String, String]](Map()) {
    case (map, par) =>
      val split = par.split(" *= *", 2)
      if (split.size == 1) map + ((split(0).substring(2), ""))
      else map + ((split(0).substring(1), split(1)))
  }

  println("Parameters:")
  parameters.foreach(param => println(s"\t${param._1}=${param._2}"))
  println()

  if (!Set("mySqlHost", "mySqlPort", "mySqlUser", "mySqlPassword", "mySqlDbname", "sqlf").forall(parameters.contains)) usage()

  private val jFields: Option[Map[String, Map[String, String]]] = parameters.get("jsonFieldFile").map {
    jFile =>
      val source: Source = Source.fromFile(jFile)
      val regex: Regex = " *([^=]+)= *(.+?)-> *([^$]+)".r
      val fMap: Map[String, Map[String, String]] = source.getLines().foldLeft(Map[String, Map[String, String]]()) {
        case (map, line) =>
          line.trim match {
            case regex(colName,jsonName, newJsonName) =>
              val map1: Map[String, String] = map.getOrElse(colName, Map[String, String]())
              map + (colName -> (map1 + (jsonName.trim -> newJsonName.trim)))
            case "" => map
            case _ => throw new IllegalArgumentException(line)
          }
      }
      source.close()
      fMap
  }

  private val conf: MySqlProducerConfig = MySqlProducerConfig(mySqlHost = parameters("mySqlHost"),
    mySqlPort = parameters("mySqlPort").toInt,
    mySqlDbname = parameters("mySqlDbname"),
    mySqlUser = parameters("mySqlUser"),
    mySqlPassword = parameters("mySqlPassword"),
    sqlf = parameters("sqlf"),
    sqlEncoding = parameters.getOrElse("sqlEncoding", "utf-8"),
    jsonFields = jFields,
    repetitiveFields = None,
    repetitiveSep = None)
  private val outJsonFile: String = parameters("outJsonFile")

  exportRecords(conf, outJsonFile) match {
    case Success(_) =>
      println("MySQL records exported successfully!")
      System.exit(0)
    case Failure(exception) =>
      println(s"Exporting MySQL records failed: ${exception.toString}")
      System.exit(-1)
  }

  def exportRecords(conf: MySqlProducerConfig,
                    outJsonFile: String): Try[Unit] = {
    Try {
      val producer: MysqlProducer = new MysqlProducer(conf)
      val writer: BufferedWriter = new BufferedWriter(new FileWriter(outJsonFile))

      writer.write("[")

      producer.getDocuments.foldLeft(0) {
        case (current, document) =>
          if (current % 1000 == 0) println(s"+++$current")
          Try(Tools.doc2json(document)) match {
            case Success(json) =>
              if (current > 0) writer.write(",")
              writer.newLine()
              writer.write(json.toString())
            case Failure(exception) =>
              System.err.println(s"Invalid document to json convertion. Document=$document Message=${exception.toString}")
          }
          current + 1
      }

      writer.newLine()
      writer.write("]")
      writer.close()
    }
  }
}
