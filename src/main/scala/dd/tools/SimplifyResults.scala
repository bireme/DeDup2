package dd.tools

import com.mongodb.client.model.InsertManyOptions
import com.mongodb.client.{MongoClients, MongoCollection, MongoCursor}
import org.bson.Document

import scala.collection.mutable
import scala.jdk.CollectionConverters.*
import scala.util.{Failure, Success, Try}

/**
 * Command-line utility that converts nested MongoDB similarity result documents
 * into a flatter representation.
 *
 * Top-level scalar fields are copied as-is, except `_id`, `dbase_1`, and
 * `dbase_2`. Top-level object fields are replaced by 0, 1, or "?" according
 * to their nested `isSimilar` value. Fields whose names start with `title`
 * are replaced by their nested `similarity` value instead.
 */
object SimplifyResults:
  private val usageMessage: String =
    """usage: SimplifyResults <options>
      |options:
      |	-mongodbHost=<host>               MongoDB server host address
      |	-mongodbPort=<int>                MongoDB server port
      |	-databaseOrigem=<name>            Source MongoDB database
      |	-databaseDestino=<name>           Destination MongoDB database
      |	-colecaoOrigem=<name>             Source MongoDB collection
      |	-colecaoDestino=<name>            Destination MongoDB collection
      |	[--append]                        Append to destination instead of recreating it
      |
      |Aliases accepted:
      |	-mongodbDatabaseOrigem, -mongodbDatabaseDestino,
      |	-mongodbColecaoOrigem, -mongodbColecaoDestino""".stripMargin

  private case class Config(host: String,
                            port: Int,
                            sourceDatabase: String,
                            destinationDatabase: String,
                            sourceCollection: String,
                            destinationCollection: String,
                            append: Boolean)

  /**
   * Entry point used when the utility is executed from the command line.
   *
   * @param args command-line arguments received by the utility
   * @return no value; this method delegates to the main workflow
   */
  def main(args: Array[String]): Unit =
    run(args) match
      case Success(total) => println(s"Simplified $total MongoDB documents successfully!")
      case Failure(exception) =>
        Console.err.println(s"Simplifying MongoDB results failed: ${exception.getMessage}")
        sys.exit(1)

  /**
   * Executes the simplification workflow.
   *
   * @param args command-line arguments received by the utility
   * @return number of simplified documents written
   */
  def run(args: Array[String]): Try[Long] =
    for
      parameters <- parseArgs(args)
      config <- parseConfig(parameters)
      total <- simplify(config)
    yield total

  private def parseArgs(args: Array[String]): Try[Map[String, String]] =
    Tools.parseCommandLineArgs(args)

  private def parseConfig(parameters: Map[String, String]): Try[Config] =
    for
      host <- required(parameters, "mongodbHost")
      port <- required(parameters, "mongodbPort").flatMap(parsePort)
      sourceDatabase <- requiredAny(parameters, "databaseOrigem", "mongodbDatabaseOrigem", "database_origem")
      destinationDatabase <- requiredAny(parameters, "databaseDestino", "mongodbDatabaseDestino", "database_destino")
      sourceCollection <- requiredAny(parameters, "colecaoOrigem", "mongodbColecaoOrigem", "colecao_origem")
      destinationCollection <- requiredAny(parameters, "colecaoDestino", "mongodbColecaoDestino", "colecao_destino")
    yield Config(
      host = host,
      port = port,
      sourceDatabase = sourceDatabase,
      destinationDatabase = destinationDatabase,
      sourceCollection = sourceCollection,
      destinationCollection = destinationCollection,
      append = parameters.contains("append")
    )

  private def required(parameters: Map[String, String],
                       name: String): Try[String] =
    parameters.get(name).filter(_.trim.nonEmpty).map(_.trim) match
      case Some(value) => Success(value)
      case None => Failure(IllegalArgumentException(usageMessage))

  private def requiredAny(parameters: Map[String, String],
                          names: String*): Try[String] =
    names.iterator.flatMap(name => parameters.get(name).map(_.trim).filter(_.nonEmpty)).toSeq.headOption match
      case Some(value) => Success(value)
      case None => Failure(IllegalArgumentException(usageMessage))

  private def parsePort(value: String): Try[Int] =
    Try(value.toInt).flatMap:
      port =>
        if port > 0 then Success(port)
        else Failure(IllegalArgumentException(s"Invalid mongodbPort [$value]. Expected a positive integer."))

  private def simplify(config: Config): Try[Long] =
    val uri = s"mongodb://${config.host}:${config.port}"
    val client = MongoClients.create(uri)

    try
      val source = client
        .getDatabase(config.sourceDatabase)
        .getCollection(config.sourceCollection)
      val destination = client
        .getDatabase(config.destinationDatabase)
        .getCollection(config.destinationCollection)

      if !config.append then destination.drop()

      copySimplified(source, destination)
    finally client.close()

  private def copySimplified(source: MongoCollection[Document],
                             destination: MongoCollection[Document]): Try[Long] =
    val cursor = source.find().iterator()
    val buffer = mutable.Buffer.empty[Document]
    var total = 0L

    try
      while cursor.hasNext do
        buffer.addOne(simplifyDocument(cursor.next()))
        total += 1

        if total % 100000 == 0 then println(s"+++$total")
        if buffer.size >= 1000 then flush(destination, buffer).get

      flush(destination, buffer).map(_ => total)
    finally closeCursor(cursor)

  private[tools] def simplifyDocument(source: Document): Document =
    source.entrySet().asScala.foldLeft(new Document()):
      case (destination, entry) =>
        val key = entry.getKey
        val value = entry.getValue

        if ignoredField(key) then destination
        else if isObject(value) && key.startsWith("title") then destination.append(key, similarityValue(value))
        else if isObject(value) then destination.append(key, simplifiedSimilarityValue(value))
        else destination.append(key, value)

  private def ignoredField(key: String): Boolean =
    key == "_id" || key == "dbase_1" || key == "dbase_2"

  private def isObject(value: Any): Boolean =
    value match
      case _: Document => true
      case _: java.util.Map[?, ?] => true
      case _ => false

  private def simplifiedSimilarityValue(value: Any): Any =
    val isSimilar = value match
      case document: Document => Option(document.get("isSimilar"))
      case map: java.util.Map[?, ?] => Option(map.get("isSimilar"))
      case _ => None

    isSimilar.map(normalizeSimilarityValue).getOrElse("?")

  private def similarityValue(value: Any): Any =
    value match
      case document: Document => Option(document.get("similarity")).getOrElse("")
      case map: java.util.Map[?, ?] => Option(map.get("similarity")).getOrElse("")
      case _ => ""

  private def normalizeSimilarityValue(value: Any): Any =
    value match
      case boolean: java.lang.Boolean => if boolean then 1 else 0
      case string: String =>
        string.trim.toLowerCase match
          case "true" => 1
          case "false" => 0
          case "maybe" => "?"
          case _ => "?"
      case _ => "?"

  private def flush(destination: MongoCollection[Document],
                    buffer: mutable.Buffer[Document]): Try[Unit] =
    Try:
      if buffer.nonEmpty then
        destination.insertMany(buffer.asJava, new InsertManyOptions().ordered(false))
        buffer.clear()

  private def closeCursor(cursor: MongoCursor[Document]): Unit =
    Try(cursor.close())
