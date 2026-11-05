package server

import dd.{NGAnalyzer, SimilarDocs}
import dd.configurators.ConfMain
import dd.finders.{LuceneDocsFinder, ScoredDocument}
import dd.interfaces.Document
import jakarta.servlet.http.{HttpServlet, HttpServletRequest, HttpServletResponse}
import org.apache.lucene.document.{Field, StoredField, TextField, Document as LuceneDocument}
import org.apache.lucene.index.{DirectoryReader, IndexWriter, IndexWriterConfig}
import org.apache.lucene.store.FSDirectory
import org.eclipse.jetty.ee11.servlet.{ServletContextHandler, ServletHolder}
import org.eclipse.jetty.server.Server as JettyServer
import play.api.libs.json.{JsArray, JsNull, JsNumber, JsObject, JsString, JsValue, Json}

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import scala.jdk.CollectionConverters.*
import scala.util.{Failure, Success, Try, Using}

object Services:
  private val DefaultPort = 7071

  /**
   * Starts the HTTP server exposing the DeDup2 JSON services.
   * @param args optional server command-line arguments
   */
  def main(args: Array[String]): Unit =
    val port = parsePort(args).fold(error => throw new IllegalArgumentException(error), identity)
    val confDirectory = Paths.get(System.getProperty("services.confDir", "conf"))
    val indexesDirectory = Paths.get(System.getProperty("services.indexesDir", "indexes"))
    val jetty = JettyServer(port)
    val context = ServletContextHandler(ServletContextHandler.NO_SESSIONS)
    context.setContextPath("/DeDup2")
    context.addServlet(ServletHolder(ServicesServlet(confDirectory, indexesDirectory)), "/services/*")
    jetty.setHandler(context)

    sys.addShutdownHook:
      if jetty.isRunning then jetty.stop()

    jetty.start()
    jetty.join()

  /**
   * Parses the optional server port argument.
   * @param args server command-line arguments
   * @return parsed port or an error message
   */
  private def parsePort(args: Array[String]): Either[String, Int] =
    args.toSeq match
      case Seq() => Right(DefaultPort)
      case Seq(value) if value.startsWith("--port=") =>
        value.stripPrefix("--port=").toIntOption.filter(_ > 0).toRight("--port must be a positive integer")
      case _ => Left("usage: server.Services [--port=<port>]")

private final class ServicesServlet(confDirectory: Path,
                                    indexesDirectory: Path) extends HttpServlet:
  private val ConfigExtension = ".cfg"
  private val SchemaPrefix = "/schema/"
  private val HiddenFields = Set("user", "pswd", "password")
  private val DeletePrefix = "/delete/"
  private val PutPrefix = "/put/"
  private val IdField = "id"
  private val DbaseField = "dbase"
  private val OccurrenceSeparator = "//@//"
  private val JsonContentType = "application/json"
  private val PutUsage =
    s"Use POST /DeDup2/services/put/<database>/<schema>/<id> with Content-Type $JsonContentType"
  private val DuplicatesPath = "/get/duplicates"
  private val DatabaseParameter = "database"
  private val SchemaParameter = "schema"
  private val QuantityParameter = "quantity"
  private val DefaultQuantity = 10
  private val DefaultMaxDocs = 1000
  private val indexLock = new Object

  /** Invalid client request, answered with HTTP 400. */
  private final class RequestError(message: String) extends IllegalArgumentException(message)

  /** Dispatches POST requests to the matching service. */
  override def doPost(request: HttpServletRequest,
                      response: HttpServletResponse): Unit =
    Option(request.getPathInfo).getOrElse("") match
      case path if path.startsWith(PutPrefix) => writePut(request, response, path.stripPrefix(PutPrefix))
      case path if path == PutPrefix.stripSuffix("/") =>
        writeJson(response, HttpServletResponse.SC_BAD_REQUEST, errorJson(PutUsage))
      case other =>
        writeJson(response, HttpServletResponse.SC_NOT_FOUND, errorJson(s"Unknown service: $other"))

  /** Dispatches GET requests to the matching service. */
  override def doGet(request: HttpServletRequest,
                     response: HttpServletResponse): Unit =
    Option(request.getPathInfo).getOrElse("") match
      case "/schemas" => writeList(response, "schemas", schemas())
      case "/indexes" => writeList(response, "indexes", indexes())
      case path if path.startsWith(SchemaPrefix) => writeSchema(response, path.stripPrefix(SchemaPrefix))
      case path if path.startsWith(DeletePrefix) => writeDelete(response, path.stripPrefix(DeletePrefix))
      case DuplicatesPath => writeDuplicates(request, response)
      case path if path == PutPrefix.stripSuffix("/") || path.startsWith(PutPrefix) =>
        response.setHeader("Allow", "POST")
        writeJson(response, HttpServletResponse.SC_METHOD_NOT_ALLOWED, errorJson(PutUsage))
      case other =>
        writeJson(response, HttpServletResponse.SC_NOT_FOUND, errorJson(s"Unknown service: $other"))

  /**
   * Lists the configuration files available in the conf directory.
   * @return sorted configuration names without the .cfg extension
   */
  private def schemas(): Try[Seq[String]] =
    listNames(confDirectory, path => Files.isRegularFile(path) && path.getFileName.toString.endsWith(ConfigExtension))
      .map(_.map(_.stripSuffix(ConfigExtension)))

  /**
   * Lists the Lucene index directories available in the indexes directory.
   * @return sorted index directory names
   */
  private def indexes(): Try[Seq[String]] =
    listNames(indexesDirectory, Files.isDirectory(_))

  /**
   * Writes the JSON content of conf/<schemaName>.cfg.
   * @param response HTTP response
   * @param schemaName configuration name without the .cfg extension
   */
  private def writeSchema(response: HttpServletResponse,
                          schemaName: String): Unit =
    val baseDirectory = confDirectory.toAbsolutePath.normalize
    val configFile = baseDirectory.resolve(schemaName + ConfigExtension).normalize
    val validName = schemaName.nonEmpty && !schemaName.contains('/') && configFile.getParent == baseDirectory

    if !validName then
      writeJson(response, HttpServletResponse.SC_BAD_REQUEST, errorJson(s"Invalid schema name: $schemaName"))
    else if !Files.isRegularFile(configFile) then
      writeJson(response, HttpServletResponse.SC_NOT_FOUND, errorJson(s"Unknown schema: $schemaName"))
    else
      Try(Json.parse(Files.readString(configFile, StandardCharsets.UTF_8))) match
        case Success(content) => writeJson(response, HttpServletResponse.SC_OK, withoutHiddenFields(content))
        case Failure(error) =>
          writeJson(response, HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
                    errorJson(s"Invalid schema $schemaName: ${error.getMessage}"))

  /**
   * Deletes the documents whose id field equals the requested value and
   * writes the resulting STATUS.
   * @param response HTTP response
   * @param arguments request path after /delete/, in the form <index>/<id>
   */
  private def writeDelete(response: HttpServletResponse,
                          arguments: String): Unit =
    val result: Try[Int] =
      arguments.split("/", 2) match
        case Array(indexName, id) if indexName.nonEmpty && id.trim.nonEmpty =>
          indexPath(indexName).flatMap(deleteById(_, id.trim))
        case _ => Failure(IllegalArgumentException(s"Invalid delete request: $arguments"))

    result match
      case Success(deleted) if deleted > 0 =>
        writeJson(response, HttpServletResponse.SC_OK, Json.obj("STATUS" -> "OK"))
      case Success(_) =>
        writeJson(response, HttpServletResponse.SC_NOT_FOUND, Json.obj("STATUS" -> "NOT FOUND"))
      case Failure(error) =>
        Console.err.println(s"Services/delete/$arguments: ${error.getMessage}")
        writeJson(response, HttpServletResponse.SC_INTERNAL_SERVER_ERROR, Json.obj("STATUS" -> "ERROR"))

  /**
   * Resolves an index name to an existing Lucene index inside the indexes directory.
   * @param indexName index directory name
   * @return index path, or a failure when the name or the index is invalid
   */
  private def indexPath(indexName: String): Try[Path] =
    Try:
      val baseDirectory = indexesDirectory.toAbsolutePath.normalize
      val path = baseDirectory.resolve(indexName).normalize
      require(path.getParent == baseDirectory, s"Invalid index name: $indexName")
      require(Files.isDirectory(path), s"Unknown index: $indexName")
      Using.resource(FSDirectory.open(path)): directory =>
        require(DirectoryReader.indexExists(directory), s"Not a Lucene index: $indexName")
      path

  /**
   * Deletes every live document whose stored id field equals the given value.
   *
   * The id field is stored but not indexed, so matching documents are found by
   * scanning the stored fields instead of using a term query.
   *
   * @param path Lucene index path
   * @param id id value to delete
   * @return number of deleted documents
   */
  private def deleteById(path: Path,
                         id: String): Try[Int] =
    indexLock.synchronized:
      Using.Manager: use =>
        val directory = use(FSDirectory.open(path))
        val config = new IndexWriterConfig().setOpenMode(IndexWriterConfig.OpenMode.APPEND)
        val writer = use(new IndexWriter(directory, config))
        val reader = use(DirectoryReader.open(writer))
        val matches: Seq[Int] = documentsWithId(reader, id)

        matches.foreach: docId =>
          if writer.tryDeleteDocument(reader, docId) == -1 then
            throw IllegalStateException(s"Could not delete Lucene document $docId")
        if matches.nonEmpty then writer.commit()
        matches.size

  /**
   * Finds the live documents whose stored id field equals the given value.
   *
   * The id field is stored but not indexed, so the stored fields are scanned
   * instead of running a term query.
   *
   * @param reader index reader
   * @param id id value to look for
   * @return global Lucene document numbers of the matching documents
   */
  private def documentsWithId(reader: DirectoryReader,
                              id: String): Seq[Int] =
    val idOnly = java.util.Set.of(IdField)
    reader.leaves().asScala.toSeq.flatMap: leaf =>
      val leafReader = leaf.reader()
      val liveDocs = leafReader.getLiveDocs
      val storedFields = leafReader.storedFields()
      (0 until leafReader.maxDoc())
        .filter(doc => liveDocs == null || liveDocs.get(doc))
        .filter(doc => Option(storedFields.document(doc, idOnly).get(IdField)).exists(_.trim == id))
        .map(leaf.docBase + _)

  /**
   * Inserts a new document, built from the JSON request body, and writes the
   * resulting STATUS.
   * @param request HTTP request whose body is a JSON object with the document fields
   * @param response HTTP response
   * @param arguments request path after /put/, in the form <database>/<schema>/<id>
   */
  private def writePut(request: HttpServletRequest,
                       response: HttpServletResponse,
                       arguments: String): Unit =
    val result: Try[Boolean] =
      for
        (indexName, schemaName, id) <- putArguments(arguments)
        _ <- requireJsonContent(request)
        path <- requestIndexPath(indexName)
        schema <- schemaFields(schemaName)
        body <- Try(Json.parse(request.getInputStream)).recoverWith:
          case error => Failure(RequestError(s"Invalid JSON body: ${error.getMessage}"))
        fields <- documentFields(body, id, schema)
        inserted <- insertDocument(path, id, schema.searchField, fields)
      yield inserted

    result match
      case Success(true) =>
        writeJson(response, HttpServletResponse.SC_CREATED, Json.obj("STATUS" -> "OK"))
      case Success(false) =>
        writeJson(response, HttpServletResponse.SC_CONFLICT, Json.obj("STATUS" -> "ALREADY EXISTS"))
      case Failure(error: RequestError) =>
        writeJson(response, HttpServletResponse.SC_BAD_REQUEST, Json.obj("STATUS" -> "ERROR", "MESSAGE" -> error.getMessage))
      case Failure(error) =>
        Console.err.println(s"Services/put/$arguments: ${error.getMessage}")
        writeJson(response, HttpServletResponse.SC_INTERNAL_SERVER_ERROR, Json.obj("STATUS" -> "ERROR", "MESSAGE" -> error.getMessage))

  /** Resolves an index named by the client; invalid names answer HTTP 400. */
  private def requestIndexPath(indexName: String): Try[Path] =
    indexPath(indexName).recoverWith:
      case error: IllegalArgumentException => Failure(RequestError(error.getMessage.stripPrefix("requirement failed: ")))

  /**
   * Candidate document found in one of the requested indexes.
   * @param database index where the document was found
   * @param found document with its Lucene score and search field similarity
   */
  private final case class Duplicate(database: String,
                                     found: ScoredDocument)

  /**
   * Finds the documents of the requested indexes that are duplicated or very
   * similar to the document described by the query parameters.
   *
   * Parameters: database (repeatable), schema, quantity (default 10) and the
   * document fields defined in the schema; the schema search field is
   * required. Candidates are those accepted by the schema finder
   * (minSimilarity, auxQuery, maxDocs) and, when the schema defines a
   * heuristic, by that heuristic. They are sorted by similarity and Lucene
   * score, and the first quantity ones are returned.
   *
   * @param request HTTP request with the query parameters
   * @param response HTTP response
   */
  private def writeDuplicates(request: HttpServletRequest,
                              response: HttpServletResponse): Unit =
    val parameters: Seq[(String, Seq[String])] =
      request.getParameterMap.asScala.toSeq.map((name, values) => name -> values.toSeq)
    val result: Try[Seq[Duplicate]] =
      for
        databases <- parameterValues(parameters, DatabaseParameter)
        schemaName <- singleParameter(parameters, SchemaParameter)
        quantity <- quantityParameter(parameters)
        schema <- schemaFields(schemaName)
        paths <- databases.foldLeft(Try(Seq.empty[(String, Path)])): (acc, name) =>
          acc.flatMap(seq => requestIndexPath(name).map(path => seq :+ (name -> path)))
        document <- queryDocument(parameters, schema)
        config <- ConfMain.parseSearchConfig(confDirectory.resolve(schemaName + ConfigExtension).toFile)
        duplicates <- findDuplicates(paths, config, document)
      yield duplicates
        .sortBy(duplicate => (-duplicate.found.similarity, -duplicate.found.score))
        .take(quantity)

    result match
      case Success(duplicates) =>
        writeJson(response, HttpServletResponse.SC_OK, Json.obj(
          "params" -> parametersJson(parameters),
          "total" -> duplicates.size,
          "result" -> duplicates.map(duplicateJson)
        ))
      case Failure(error: RequestError) =>
        writeJson(response, HttpServletResponse.SC_BAD_REQUEST, Json.obj("STATUS" -> "ERROR", "MESSAGE" -> error.getMessage))
      case Failure(error) =>
        Console.err.println(s"Services/get/duplicates?${Option(request.getQueryString).getOrElse("")}: ${error.getMessage}")
        writeJson(response, HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
                  Json.obj("STATUS" -> "ERROR", "MESSAGE" -> Option(error.getMessage).getOrElse(error.toString)))

  /** Reads the non-empty values of a required, possibly repeated, parameter. */
  private def parameterValues(parameters: Seq[(String, Seq[String])],
                              name: String): Try[Seq[String]] =
    val values = parameters.collect { case (`name`, values) => values }.flatten.map(_.trim).filter(_.nonEmpty)
    if values.isEmpty then Failure(RequestError(s"Missing parameter: $name"))
    else Success(values.distinct)

  /** Reads a required parameter that must have exactly one value. */
  private def singleParameter(parameters: Seq[(String, Seq[String])],
                              name: String): Try[String] =
    parameterValues(parameters, name).flatMap:
      case Seq(value) => Success(value)
      case _ => Failure(RequestError(s"Parameter $name must have a single value"))

  /** Reads the optional quantity parameter, a positive integer. */
  private def quantityParameter(parameters: Seq[(String, Seq[String])]): Try[Int] =
    if !parameters.exists(_._1 == QuantityParameter) then Success(DefaultQuantity)
    else
      singleParameter(parameters, QuantityParameter).flatMap: value =>
        value.toIntOption.filter(_ > 0).toRight(RequestError(s"Parameter $QuantityParameter must be a positive integer: $value")).toTry

  /**
   * Builds the document to be compared from the field parameters.
   *
   * Every parameter other than database, schema and quantity must be a field
   * of the schema. A value may hold several occurrences separated by //@//.
   *
   * @param parameters request parameters
   * @param schema fields accepted by the schema
   * @return document with the search field first
   */
  private def queryDocument(parameters: Seq[(String, Seq[String])],
                            schema: SchemaFields): Try[Document] =
    Try:
      val fieldParameters = parameters.filterNot((name, _) => Set(DatabaseParameter, SchemaParameter, QuantityParameter).contains(name))
      val unknown = fieldParameters.map(_._1).filterNot(schema.allowed.contains).sorted
      if unknown.nonEmpty then
        throw RequestError(s"Parameters not in schema: ${unknown.mkString(", ")}. Allowed: ${schema.allowed.toSeq.sorted.mkString(", ")}")

      val fields = fieldParameters.flatMap: (name, values) =>
        values.flatMap(_.split(java.util.regex.Pattern.quote(OccurrenceSeparator), -1)).map(_.trim).filter(_.nonEmpty).map(name -> _)
      val (search, others) = fields.partition(_._1 == schema.searchField)
      if search.isEmpty then
        throw RequestError(s"Parameter ${schema.searchField} is required and must not be empty")

      Document((schema.searchField -> search.map(_._2).mkString(" ")) +: others)

  /**
   * Searches the document in every index and keeps the candidates accepted
   * by the schema heuristic, when there is one.
   *
   * @param indexes requested index names and paths
   * @param config schema search options
   * @param document document to be compared
   * @return candidates found in all indexes
   */
  private def findDuplicates(indexes: Seq[(String, Path)],
                             config: ConfMain.SearchConfig,
                             document: Document): Try[Seq[Duplicate]] =
    val comparators = SimilarDocs.includeIndexedFieldDiceComparator(config.searchField, config.minSimilarity, config.comparators)
    val query = document.fields.head._2

    indexes.foldLeft(Try(Seq.empty[Duplicate])):
      case (acc, (name, path)) =>
        acc.flatMap: found =>
          Try(LuceneDocsFinder(path.toString, config.searchField, config.minSimilarity)).flatMap: finder =>
            try
              finder.findScoredDocs(config.searchField, query, config.auxQuery, config.maxDocs.getOrElse(DefaultMaxDocs)).map: candidates =>
                val accepted = config.heuristic match
                  case None => candidates
                  case Some(heuristic) =>
                    val prepared = comparators.map(_.prepare(document))
                    candidates.filter(candidate => heuristic.isDuplicated(candidate.document, prepared.map(_.compare(candidate.document))))
                found ++ accepted.map(Duplicate(name, _)).toSeq
            finally finder.close()

  /** Echoes the request parameters: one value as a string, several as an array. */
  private def parametersJson(parameters: Seq[(String, Seq[String])]): JsObject =
    JsObject(parameters.map:
      case (name, Seq(value)) => name -> JsString(value)
      case (name, values) => name -> JsArray(values.map(JsString(_))))

  /** Converts a candidate into JSON, with repeated fields joined by //@//. */
  private def duplicateJson(duplicate: Duplicate): JsObject =
    val header = Seq(
      "score" -> duplicate.found.score.toString,
      "similarity" -> duplicate.found.similarity.toString,
      "database" -> duplicate.database
    )
    val reserved = header.map(_._1).toSet
    val fields = duplicate.found.document.fields
      .filterNot((name, _) => reserved.contains(name))
      .groupMap(_._1)(_._2)
    val order = duplicate.found.document.fields.map(_._1).distinct.filter(fields.contains)
    JsObject((header ++ order.map(name => name -> fields(name).mkString(OccurrenceSeparator))).map((name, value) => name -> JsString(value)))

  /** Splits the put request path into index, schema and document id. */
  private def putArguments(arguments: String): Try[(String, String, String)] =
    arguments.split("/", 3) match
      case Array(indexName, schemaName, id) if indexName.nonEmpty && schemaName.nonEmpty && id.trim.nonEmpty =>
        Success((indexName, schemaName, id.trim))
      case _ => Failure(RequestError(s"Invalid put request, expected put/<database>/<schema>/<id>: $arguments"))

  /** Accepts only JSON request bodies. */
  private def requireJsonContent(request: HttpServletRequest): Try[Unit] =
    val contentType = Option(request.getContentType).getOrElse("")
    if contentType.toLowerCase.startsWith(JsonContentType) then Success(())
    else Failure(RequestError(s"Content-Type must be $JsonContentType, got: $contentType"))

  /**
   * Fields accepted by a schema.
   * @param searchField indexed field, required in every document
   * @param allowed every field the schema accepts
   */
  private final case class SchemaFields(searchField: String,
                                        allowed: Set[String])

  /**
   * Reads the fields accepted by conf/<schemaName>.cfg: the finder search
   * field, the comparator fields, the reporter otherFields, dbase and id.
   * @param schemaName configuration name without the .cfg extension
   * @return fields accepted by the schema
   */
  private def schemaFields(schemaName: String): Try[SchemaFields] =
    val baseDirectory = confDirectory.toAbsolutePath.normalize
    val configFile = baseDirectory.resolve(schemaName + ConfigExtension).normalize
    if schemaName.contains('/') || configFile.getParent != baseDirectory then
      Failure(RequestError(s"Invalid schema name: $schemaName"))
    else if !Files.isRegularFile(configFile) then
      Failure(RequestError(s"Unknown schema: $schemaName"))
    else
      Try:
        val json = Json.parse(Files.readString(configFile, StandardCharsets.UTF_8))
        val searchField = (json \ "finder" \ "lucene" \ "searchField").asOpt[String].filter(_.nonEmpty)
          .getOrElse(throw IllegalStateException(s"Schema $schemaName has no finder.lucene.searchField"))
        def elementValues(section: String, key: String): Seq[String] =
          (json \ section).asOpt[JsArray].toSeq.flatMap(_.value).flatMap:
            case element: JsObject => element.values.toSeq.flatMap: specification =>
              (specification \ key).asOpt[String].toSeq ++ (specification \ key).asOpt[Seq[String]].toSeq.flatten
            case _ => Seq.empty
        val allowed = Set(IdField, DbaseField, searchField) ++
          elementValues("comparators", "fieldName") ++ elementValues("reporters", "otherFields")
        SchemaFields(searchField, allowed)

  /**
   * Validates the JSON body against the schema and converts it into field values.
   *
   * Strings and numbers are stored as text; arrays become occurrences joined
   * with //@//; null values are ignored.
   *
   * @param body JSON request body
   * @param id document id taken from the request path
   * @param schema fields accepted by the schema
   * @return document fields, with id first
   */
  private def documentFields(body: JsValue,
                             id: String,
                             schema: SchemaFields): Try[Seq[(String, String)]] =
    Try:
      val obj = body match
        case value: JsObject => value
        case _ => throw RequestError("The JSON body must be an object with the document fields")

      val unknown = obj.keys.toSeq.filterNot(schema.allowed.contains).sorted
      if unknown.nonEmpty then
        throw RequestError(s"Fields not in schema: ${unknown.mkString(", ")}. Allowed: ${schema.allowed.toSeq.sorted.mkString(", ")}")

      def text(field: String, value: JsValue): String =
        value match
          case JsString(str) => str
          case number: JsNumber => number.toString
          case JsArray(values) => values.map(text(field, _)).map(_.trim).filter(_.nonEmpty).mkString(OccurrenceSeparator)
          case _ => throw RequestError(s"Field $field must be a string, a number or an array of them")

      val values = obj.fields.toSeq.collect:
        case (name, value) if value != JsNull => name -> text(name, value)

      values.collectFirst { case (IdField, value) if value.trim != id => value }.foreach: value =>
        throw RequestError(s"Body id '$value' differs from the path id '$id'")
      if !values.exists { case (name, value) => name == schema.searchField && value.trim.nonEmpty } then
        throw RequestError(s"Field ${schema.searchField} is required and must not be empty")

      (IdField -> id) +: values.filterNot(_._1 == IdField)

  /**
   * Adds the document to the index unless a document with the same id exists.
   *
   * The search field is indexed with the NGAnalyzer and stored, and the other
   * fields are only stored, as in Tools.createLuceneIndex.
   *
   * @param path Lucene index path
   * @param id document id
   * @param searchField indexed field
   * @param fields document fields
   * @return true when inserted, false when the id already exists
   */
  private def insertDocument(path: Path,
                             id: String,
                             searchField: String,
                             fields: Seq[(String, String)]): Try[Boolean] =
    indexLock.synchronized:
      Using.Manager: use =>
        val directory = use(FSDirectory.open(path))
        val config = new IndexWriterConfig(new NGAnalyzer()).setOpenMode(IndexWriterConfig.OpenMode.APPEND)
        val writer = use(new IndexWriter(directory, config))
        val exists = Using.resource(DirectoryReader.open(writer))(documentsWithId(_, id).nonEmpty)
        if !exists then
          val document = new LuceneDocument()
          fields.foreach:
            case (name, value) if name == searchField => document.add(new TextField(name, value, Field.Store.YES))
            case (name, value) => document.add(new StoredField(name, value))
          writer.addDocument(document)
          writer.commit()
        !exists

  /**
   * Removes credential elements, at any nesting level, from a JSON value.
   * @param json JSON value to sanitize
   * @return JSON value without the hidden fields
   */
  private def withoutHiddenFields(json: JsValue): JsValue =
    json match
      case JsObject(fields) =>
        JsObject(fields.collect:
          case (name, value) if !HiddenFields.contains(name.toLowerCase) => name -> withoutHiddenFields(value))
      case JsArray(values) => JsArray(values.map(withoutHiddenFields))
      case other => other

  /** Lists the sorted names of the directory entries accepted by the filter. */
  private def listNames(directory: Path,
                        accept: Path => Boolean): Try[Seq[String]] =
    Using(Files.list(directory)):
      _.iterator().asScala
        .filter(accept)
        .map(_.getFileName.toString)
        .toSeq
        .sorted

  /** Writes a JSON object holding a single named array, or an error response. */
  private def writeList(response: HttpServletResponse,
                        element: String,
                        names: Try[Seq[String]]): Unit =
    names match
      case Success(values) => writeJson(response, HttpServletResponse.SC_OK, Json.obj(element -> values))
      case Failure(error) => writeJson(response, HttpServletResponse.SC_INTERNAL_SERVER_ERROR, errorJson(error.getMessage))

  /** Builds the JSON body used for error responses. */
  private def errorJson(message: String): JsObject = Json.obj("error" -> message)

  /** Writes a JSON response with the given HTTP status. */
  private def writeJson(response: HttpServletResponse,
                        status: Int,
                        body: JsValue): Unit =
    response.setStatus(status)
    response.setCharacterEncoding(StandardCharsets.UTF_8.name)
    response.setContentType("application/json")
    response.getWriter.print(Json.stringify(body))
