package server

import jakarta.servlet.http.{HttpServlet, HttpServletRequest, HttpServletResponse}
import org.apache.lucene.index.{DirectoryReader, IndexWriter, IndexWriterConfig}
import org.apache.lucene.store.FSDirectory
import org.eclipse.jetty.ee11.servlet.{ServletContextHandler, ServletHolder}
import org.eclipse.jetty.server.Server as JettyServer
import play.api.libs.json.{JsArray, JsObject, JsValue, Json}

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
  private val IdField = "id"
  private val deleteLock = new Object

  /** Dispatches GET requests to the matching service. */
  override def doGet(request: HttpServletRequest,
                     response: HttpServletResponse): Unit =
    Option(request.getPathInfo).getOrElse("") match
      case "/schemas" => writeList(response, "schemas", schemas())
      case "/indexes" => writeList(response, "indexes", indexes())
      case path if path.startsWith(SchemaPrefix) => writeSchema(response, path.stripPrefix(SchemaPrefix))
      case path if path.startsWith(DeletePrefix) => writeDelete(response, path.stripPrefix(DeletePrefix))
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
    deleteLock.synchronized:
      Using.Manager: use =>
        val directory = use(FSDirectory.open(path))
        val config = new IndexWriterConfig().setOpenMode(IndexWriterConfig.OpenMode.APPEND)
        val writer = use(new IndexWriter(directory, config))
        val reader = use(DirectoryReader.open(writer))
        val idOnly = java.util.Set.of(IdField)
        val matches: Seq[Int] =
          reader.leaves().asScala.toSeq.flatMap: leaf =>
            val leafReader = leaf.reader()
            val liveDocs = leafReader.getLiveDocs
            val storedFields = leafReader.storedFields()
            (0 until leafReader.maxDoc())
              .filter(doc => liveDocs == null || liveDocs.get(doc))
              .filter(doc => Option(storedFields.document(doc, idOnly).get(IdField)).exists(_.trim == id))
              .map(leaf.docBase + _)

        matches.foreach: docId =>
          if writer.tryDeleteDocument(reader, docId) == -1 then
            throw IllegalStateException(s"Could not delete Lucene document $docId")
        if matches.nonEmpty then writer.commit()
        matches.size

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
