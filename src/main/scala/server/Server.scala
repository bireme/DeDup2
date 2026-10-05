package server

import dd.comparators.DiceComparator
import dd.configurators.ConfMain
import dd.finders.LuceneDocsFinder
import dd.interfaces.{Comparator, Document, SimilarityStatus}
import jakarta.servlet.http.{HttpServlet, HttpServletRequest, HttpServletResponse}
import org.eclipse.jetty.ee11.servlet.{ServletContextHandler, ServletHolder}
import org.eclipse.jetty.server.Server as JettyServer
import play.api.libs.json.{JsArray, JsLookupResult, JsObject, JsValue, Json}

import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import java.util.regex.Pattern
import scala.jdk.CollectionConverters.*
import scala.util.{Failure, Success, Try, Using}

object Server:
  private val DefaultPort = 8080

  /**
   * Starts the HTTP server using the configured database registry.
   * @param args optional server command-line arguments
   */
  def main(args: Array[String]): Unit =
    val port = parsePort(args).fold(error => throw new IllegalArgumentException(error), identity)
    val configFile = Paths.get(System.getProperty("server.config", "server/config.cfg"))
    val jetty = JettyServer(port)
    val context = ServletContextHandler(ServletContextHandler.SESSIONS)
    context.setContextPath("/")
    context.addServlet(ServletHolder(SimilarDocsServlet(configFile)), "/*")
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
      case _ => Left("usage: server.Server [--port=<port>]")

private final case class DatabaseDefinition(name: String,
                                            indexPath: Path,
                                            searchField: String,
                                            minSimilarity: Double,
                                            maxDocs: Int,
                                            comparatorFields: Seq[ComparatorField],
                                            configFile: Path)

private final case class ComparatorField(fieldName: String,
                                         comparatorName: String)

private final case class DatabaseRegistry(databases: Seq[DatabaseDefinition],
                                          errors: Seq[String]):
  /**
   * Finds a database definition by its configured name.
   * @param name configured database name
   * @return matching definition, when present
   */
  def find(name: String): Option[DatabaseDefinition] = databases.find(_.name == name)

private object DatabaseRegistry:
  /**
   * Loads and validates all database entries from the registry file.
   * @param configFile registry configuration path
   * @return loaded database registry
   */
  def load(configFile: Path): DatabaseRegistry =
    val absoluteConfig = configFile.toAbsolutePath.normalize
    val projectRoot = Option(absoluteConfig.getParent).flatMap(parent => Option(parent.getParent))
      .getOrElse(throw new IllegalArgumentException(s"Cannot determine project root from $configFile"))
    val confDirectory = projectRoot.resolve("conf").normalize
    val lines = Using.resource(Files.newBufferedReader(absoluteConfig, StandardCharsets.UTF_8))(_.lines().iterator().asScala.toSeq)
    val parsed = lines.zipWithIndex.collect:
      case (line, lineNumber) if line.trim.nonEmpty && !line.trim.startsWith("#") =>
        parseLine(line, lineNumber + 1, confDirectory, projectRoot)

    DatabaseRegistry(
      databases = parsed.collect { case Success(database) => database },
      errors = parsed.collect { case Failure(error) => error.getMessage }
    )

  /** Parses one registry line and its referenced JSON configuration. */
  private def parseLine(line: String,
                        lineNumber: Int,
                        confDirectory: Path,
                        projectRoot: Path): Try[DatabaseDefinition] =
    Try:
      val parts = line.split("=", 2).map(_.trim)
      require(parts.length == 2 && parts.forall(_.nonEmpty), s"server/config.cfg:$lineNumber must use database=config-file.cfg")
      val configFile = confDirectory.resolve(parts(1)).normalize
      require(configFile.startsWith(confDirectory), s"server/config.cfg:$lineNumber has an invalid config path")
      require(Files.isRegularFile(configFile), s"server/config.cfg:$lineNumber does not exist: ${configFile.getFileName}")

      val json = Json.parse(Files.readString(configFile, StandardCharsets.UTF_8))
      val lucene = json \ "finder" \ "lucene"
      val index = requiredString(lucene, "index", configFile)
      val searchField = requiredString(lucene, "searchField", configFile)
      val minSimilarity = (lucene \ "minSimilarity").asOpt[Double]
        .filter(value => value >= 0.0 && value <= 1.0)
        .getOrElse(throw new IllegalArgumentException(s"$configFile has an invalid finder.lucene.minSimilarity"))
      val maxDocs = (lucene \ "maxDocs").asOpt[Int].filter(_ > 0).getOrElse(100)
      val fields = comparatorFields(json, configFile)
      require(fields.nonEmpty, s"$configFile has no comparator fieldName entries")

      val indexPath = projectRoot.resolve(index).normalize
      require(indexPath.startsWith(projectRoot), s"$configFile has an invalid Lucene index path")
      DatabaseDefinition(parts(0), indexPath, searchField, minSimilarity, maxDocs, fields, configFile)

  /** Reads a required non-empty string from a JSON object. */
  private def requiredString(json: JsLookupResult,
                             field: String,
                             configFile: Path): String =
    (json \ field).asOpt[String].filter(_.nonEmpty)
      .getOrElse(throw new IllegalArgumentException(s"$configFile is missing finder.lucene.$field"))

  /** Extracts distinct comparator field definitions from JSON configuration. */
  private def comparatorFields(json: JsValue,
                               configFile: Path): Seq[ComparatorField] =
    (json \ "comparators").asOpt[JsArray]
      .getOrElse(throw new IllegalArgumentException(s"$configFile is missing comparators"))
      .value
      .collect { case comparator: JsObject => comparator }
      .flatMap: comparator =>
        comparator.value.toSeq.flatMap:
          case (comparatorName, specification) =>
            (specification \ "fieldName").asOpt[String]
              .map(fieldName => ComparatorField(fieldName, displayComparatorName(comparatorName)))
      .distinctBy(_.fieldName)
      .toSeq

  /**
   * Converts a configured comparator key to its display name.
   * @param name configured comparator key
   * @return human-readable comparator name
   */
  private def displayComparatorName(name: String): String =
    name match
      case "dice" => "DiceComparator"
      case "exact" => "ExactComparator"
      case "ngram" => "NGramComparator"
      case "regex" => "RegexComparator"
      case "authors" => "AuthorsComparator"
      case "urls" => "UrlsComparator"
      case other => other

private final class SimilarDocsServlet(configFile: Path) extends HttpServlet:
  private val occurrenceSeparator = "//@//"

  /** Renders the search form and any available result. */
  override def doGet(request: HttpServletRequest,
                     response: HttpServletResponse): Unit =
    render(request, response, None)

  /** Processes a search submission or redirects to the form. */
  override def doPost(request: HttpServletRequest,
                      response: HttpServletResponse): Unit =
    if request.getParameter("action") == "search" then render(request, response, Some(search(request)))
    else response.sendRedirect(request.getContextPath + "/")

  /**
   * Executes a similarity search for the submitted request parameters.
   * @param request HTTP search request
   * @return search result or an error
   */
  private def search(request: HttpServletRequest): SearchResult =
    val registry = DatabaseRegistry.load(configFile)
    selectedDatabase(request, registry).fold(
      error => SearchResult.error(error),
      database =>
        val query = parameterValue(request, "query").trim
        if query.isEmpty then SearchResult.error(s"Enter a value for ${database.searchField}.")
        else
          val finder = LuceneDocsFinder(database.indexPath.toString, database.searchField, database.minSimilarity)
          val originalDocument = inputDocument(database, request, query)
          try
            (for
              (_, comparators) <- ConfMain.parseSimilarityConfig(database.configFile.toFile)
              producer <- finder.findDocs(database.searchField, query, None, database.maxDocs)
            yield producer.getDocuments.toList.map(candidateDocument(originalDocument, _, database, comparators))) match
              case Success(candidates) => SearchResult.success(candidates)
              case Failure(error) => SearchResult.error(s"Search failed: ${message(error)}")
          finally finder.close()
    )

  /** Renders the complete HTML response for the current request. */
  private def render(request: HttpServletRequest,
                     response: HttpServletResponse,
                     result: Option[SearchResult]): Unit =
    val registry = Try(DatabaseRegistry.load(configFile)) match
      case Success(value) => value
      case Failure(error) => DatabaseRegistry(Seq.empty, Seq(message(error)))
    val database = selectedDatabase(request, registry).toOption

    response.setCharacterEncoding(StandardCharsets.UTF_8.name)
    response.setContentType("text/html")
    response.getWriter.print(page(registry, database, request, result))

  /** Resolves the selected database or returns a user-facing error. */
  private def selectedDatabase(request: HttpServletRequest,
                               registry: DatabaseRegistry): Either[String, DatabaseDefinition] =
    Option(request.getParameter("database")).filter(_.nonEmpty) match
      case Some(name) => registry.find(name).toRight(s"Unknown database: $name")
      case None => registry.databases.headOption.toRight("No valid databases are configured.")

  /** Builds the page containing the selector, form, and result table. */
  private def page(registry: DatabaseRegistry,
                   database: Option[DatabaseDefinition],
                   request: HttpServletRequest,
                   result: Option[SearchResult]): String =
    val errors = (registry.errors ++ result.toSeq.flatMap(_.error)).map(error => s"<p class=\"error\">${escape(error)}</p>").mkString
    val form = database.map(databaseForm(_, request)).getOrElse("")
    val results = (database, result) match
      case (Some(value), Some(searchResult)) if searchResult.candidates.nonEmpty => resultTable(value, request, searchResult.candidates)
      case (_, Some(searchResult)) if searchResult.error.isEmpty => "<p class=\"empty-result\">No similar documents were found.</p>"
      case _ => ""

    s"""<!doctype html>
       |<html lang=\"pt-BR\"><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">
       |<title>Similar Documents</title><style>
       |:root { --bg: #eef3f5; --surface: #ffffff; --surface-muted: #f7faf9; --text: #1f2933; --muted: #65737e; --border: #cfd9df; --primary: #166c7d; --primary-hover: #0f5a69; --secondary: #596673; --secondary-hover: #48535f; --focus: #d79d2b; --error-bg: #fff1f1; --error-text: #a62626; --shadow: 0 18px 45px rgba(40, 55, 65, .12); }
       |* { box-sizing: border-box; } body { min-height: 100vh; margin: 0; font-family: system-ui, -apple-system, BlinkMacSystemFont, \"Segoe UI\", sans-serif; background: var(--bg); color: var(--text); }
       |.container { width: min(1180px, calc(100% - 40px)); margin: 0 auto; padding: 34px 0 48px; }.app-header { display: flex; align-items: center; gap: 16px; margin-bottom: 22px; }.brand-mark { display: grid; place-items: center; width: 54px; height: 54px; border-radius: 8px; background: var(--primary); color: #fff; font-weight: 800; box-shadow: 0 10px 24px rgba(22, 108, 125, .24); }.eyebrow { margin: 0 0 2px; color: var(--muted); font-size: .78rem; font-weight: 800; letter-spacing: 0; }.app-header h1 { margin: 0; font-size: 2.2rem; line-height: 1.05; letter-spacing: 0; }
       |.panel { background: var(--surface); padding: 28px; border: 1px solid var(--border); border-top: 4px solid var(--primary); border-radius: 8px; box-shadow: var(--shadow); }.database-form { padding-bottom: 18px; border-bottom: 1px solid #e1e8eb; } form, table { width: 100%; } label { display: block; margin: 20px 0 8px; color: #26323a; font-size: .95rem; font-weight: 800; } .comparator-name { color: var(--muted); font-weight: 400; } .database-form label { margin-top: 0; }
       |input, select, button { font: inherit; } input:not([type=\"hidden\"]), select { min-height: 42px; border: 1px solid var(--border); border-radius: 6px; padding: 9px 11px; background: var(--surface); color: var(--text); box-shadow: inset 0 1px 2px rgba(31, 41, 51, .04); } input:not([type=\"hidden\"]) { width: 100%; } #database { width: max-content; max-width: 100%; } input:focus, select:focus { outline: 3px solid rgba(215, 157, 43, .28); outline-offset: 1px; border-color: var(--primary); }
       |.actions { display: flex; justify-content: flex-end; gap: 10px; width: 100%; margin-top: 22px; } button { min-height: 42px; border: 0; border-radius: 6px; padding: 9px 16px; font-size: .92rem; font-weight: 800; cursor: pointer; background: var(--primary); color: #fff; box-shadow: 0 8px 18px rgba(22, 108, 125, .18); transition: background-color 120ms ease, box-shadow 120ms ease, transform 120ms ease; } button:hover { background: var(--primary-hover); box-shadow: 0 10px 22px rgba(22, 108, 125, .22); transform: translateY(-1px); } button:focus-visible { outline: 3px solid rgba(215, 157, 43, .45); outline-offset: 2px; }.secondary { background: var(--secondary); box-shadow: 0 8px 18px rgba(89, 102, 115, .16); }.secondary:hover { background: var(--secondary-hover); }
       |.occurrence-note { margin: 18px 0 0; color: var(--muted); font-size: .9rem; text-align: right; }.occurrence-note code { padding: 2px 5px; border-radius: 4px; background: #e4eff1; color: #26323a; font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-weight: 700; }.results { overflow-x: auto; margin-top: 28px; border: 1px solid var(--border); border-radius: 6px; } table { min-width: 100%; border-collapse: collapse; margin: 0; } th, td { border-bottom: 1px solid #dce5e8; padding: .7rem .8rem; text-align: left; vertical-align: top; } thead th { position: relative; background: #e4eff1; color: #26323a; font-size: .9rem; } tbody tr:nth-child(even) { background: var(--surface-muted); } tbody tr:hover { background: #edf7f7; } table th:first-child { width: 1%; white-space: nowrap; }.similar-cell { background: #e6f6ea; }.different-cell { background: #fff0f0; }.column-resizer { position: absolute; z-index: 1; top: 0; right: -4px; width: 8px; height: 100%; cursor: col-resize; touch-action: none; }.column-resizer:hover, .column-resizer:focus-visible { background: rgba(22, 108, 125, .28); outline: none; } .error { margin: 18px 0 0; padding: 10px 12px; border: 1px solid rgba(166, 38, 38, .18); border-radius: 6px; background: var(--error-bg); color: var(--error-text); font-weight: 700; }.empty-result { margin: 24px 0 0; padding: 12px; border-radius: 6px; background: var(--surface-muted); color: var(--muted); font-weight: 700; }
       |@media (max-width: 760px) { .container { width: min(100% - 24px, 1180px); padding-top: 22px; }.app-header { gap: 12px; }.brand-mark { width: 46px; height: 46px; }.app-header h1 { font-size: 1.8rem; }.panel { padding: 18px; }.actions { justify-content: flex-end; } }
       |</style></head><body><main class=\"container\"><header class=\"app-header\"><div class=\"brand-mark\" aria-hidden=\"true\">SD</div><div><p class=\"eyebrow\">BIREME/ OPAS/ OMS</p><h1>Similar Documents</h1></div></header><section class=\"panel\">
       |<form method=\"get\" class=\"database-form\">${databaseSelector(registry, database)}</form>
       |$errors$form$results</section></main><script>
       |document.querySelectorAll(".results table").forEach((table) => {
       |  const headers = Array.from(table.querySelectorAll("thead th"));
       |  if (headers.length === 0) return;
       |  const initialWidth = Math.ceil(table.getBoundingClientRect().width);
       |  headers.forEach((header) => { header.style.width = `$${Math.ceil(header.getBoundingClientRect().width)}px`; });
       |  table.style.tableLayout = "fixed";
       |  table.style.width = `$${initialWidth}px`;
       |  headers.forEach((header) => {
       |    const handle = document.createElement("span");
       |    handle.className = "column-resizer";
       |    handle.tabIndex = 0;
       |    handle.title = "Resize column";
       |    handle.setAttribute("aria-label", "Resize column");
       |    handle.addEventListener("pointerdown", (event) => {
       |      event.preventDefault();
       |      const startX = event.clientX;
       |      const startWidth = header.getBoundingClientRect().width;
       |      const startTableWidth = table.getBoundingClientRect().width;
       |      const resize = (moveEvent) => {
       |        const width = Math.max(88, startWidth + moveEvent.clientX - startX);
       |        header.style.width = `$${width}px`;
       |        table.style.width = `$${Math.max(table.parentElement.clientWidth, startTableWidth + width - startWidth)}px`;
       |      };
       |      const stop = () => {
       |        document.removeEventListener("pointermove", resize);
       |        document.removeEventListener("pointerup", stop);
       |      };
       |      document.addEventListener("pointermove", resize);
       |      document.addEventListener("pointerup", stop);
       |    });
       |    header.appendChild(handle);
       |  });
       |});
       |</script></body></html>""".stripMargin

  /** Builds the database selection control. */
  private def databaseSelector(registry: DatabaseRegistry,
                               selected: Option[DatabaseDefinition]): String =
    val options = registry.databases.map: database =>
      val selectedAttribute = if selected.contains(database) then " selected" else ""
      s"<option value=\"${escape(database.name)}\"$selectedAttribute>${escape(database.name)}</option>"
    s"<label for=\"database\">database</label><select id=\"database\" name=\"database\" onchange=\"this.form.submit()\">${options.mkString}</select>"

  /** Builds the input form for a configured database. */
  private def databaseForm(database: DatabaseDefinition,
                           request: HttpServletRequest): String =
    val fieldInputs = database.comparatorFields.zipWithIndex.map:
      case (field, index) =>
        val parameter = fieldParameter(index)
        s"<label for=\"$parameter\">${escape(field.fieldName)} <span class=\"comparator-name\">(${escape(field.comparatorName)})</span></label><input id=\"$parameter\" name=\"$parameter\" value=\"${escape(parameterValue(request, parameter))}\">"
    val cleanUrl = s"?database=${URLEncoder.encode(database.name, StandardCharsets.UTF_8)}"
    s"""<form method=\"post\"><input type=\"hidden\" name=\"database\" value=\"${escape(database.name)}\">
       |<input type=\"hidden\" name=\"action\" value=\"search\">
       |<label for=\"query\">${escape(database.searchField)} <span class=\"comparator-name\">(DiceComparator)</span></label><input id=\"query\" name=\"query\" required value=\"${escape(parameterValue(request, "query"))}\">
       |${fieldInputs.mkString}<p class=\"occurrence-note\">O separador padrão para ocorrências múltiplas em um campo é <code>//@//</code>.</p>
       |<div class=\"actions\"><button type=\"submit\">Search</button><button type=\"button\" class=\"secondary\" onclick=\"window.location.assign('${escape(cleanUrl)}')\">Clean</button></div></form>""".stripMargin

  /** Builds the comparison result table. */
  private def resultTable(database: DatabaseDefinition,
                          request: HttpServletRequest,
                          candidates: Seq[CandidateDocument]): String =
    val fields = (database.searchField -> database.searchField) +: database.comparatorFields
      .filterNot(_.fieldName == database.searchField)
      .map(field => field.fieldName -> field.fieldName)
    val header = ("Source" +: "id" +: fields.map(_._2)).map(field => s"<th>${escape(field)}</th>").mkString
    val inputValues = parameterValue(request, "query") +: database.comparatorFields.zipWithIndex
      .filterNot(_._1.fieldName == database.searchField).map { case (_, index) => parameterValue(request, fieldParameter(index)) }
    val inputRow = row("Input", TableCell("", None) +: inputValues.map(TableCell(_, None)))
    val sortedCandidates = candidates.sortBy: candidate =>
      -(fields.count { case (fieldName, _) => candidate.similarities.get(fieldName).contains(true) })
    val documentRows = sortedCandidates.zipWithIndex.map:
      case (candidate, index) =>
        row(
          s"Result ${index + 1}",
          TableCell(fieldValue(candidate.document, "id"), None) +: fields.map { case (fieldName, _) =>
            TableCell(fieldValue(candidate.document, fieldName), candidate.similarities.get(fieldName))
          }
        )
    s"<div class=\"results\"><table><thead><tr>$header</tr></thead><tbody>$inputRow${documentRows.mkString}</tbody></table></div>"

  /** Creates a document from the submitted query and comparator fields. */
  private def inputDocument(database: DatabaseDefinition,
                            request: HttpServletRequest,
                            query: String): Document =
    val comparatorValues = database.comparatorFields.zipWithIndex.flatMap:
      case (field, index) if field.fieldName != database.searchField =>
        occurrenceValues(parameterValue(request, fieldParameter(index))).map(field.fieldName -> _)
      case _ => Seq.empty
    Document((database.searchField -> query) +: comparatorValues)

  /**
   * Splits repeated field values using the configured occurrence separator.
   * @param value serialized repeated field value
   * @return non-empty occurrence values
   */
  private def occurrenceValues(value: String): Seq[String] =
    value.split(Pattern.quote(occurrenceSeparator), -1).toSeq.map(_.trim).filter(_.nonEmpty)

  /** Compares a candidate document against the submitted input document. */
  private def candidateDocument(original: Document,
                                candidate: Document,
                                database: DatabaseDefinition,
                                comparators: Seq[Comparator]): CandidateDocument =
    val comparatorResults = comparators.map(_.compare(original, candidate))
      .map(result => result.fieldName -> result.isSimilar)
      .toMap
    val diceResult = DiceComparator(database.searchField, normalize = true, database.minSimilarity)
      .compare(original, candidate)
    CandidateDocument(candidate, comparatorResults.updated(database.searchField, diceResult.isSimilar))

  /**
   * Builds one HTML table row from its source label and cells.
   * @param source row source label
   * @param values cells included in the row
   * @return rendered HTML row
   */
  private def row(source: String, values: Seq[TableCell]): String =
    val cells = values.map: cell =>
      val colorClass = cell.isSimilar.fold("") {
        case SimilarityStatus.yes => " similar-cell"
        case SimilarityStatus.no => " different-cell"
        case SimilarityStatus.undefined => ""
      }
      s"<td class=\"$colorClass\">${escape(cell.value)}</td>"
    s"<tr><th>${escape(source)}</th>${cells.mkString}</tr>"

  /**
   * Collects all values of a field from a document.
   * @param document source document
   * @param field field name to read
   * @return joined field values
   */
  private def fieldValue(document: Document, field: String): String =
    document.fields.collect { case (`field`, value) => value }.mkString(" | ")

  /**
   * Creates the request parameter name for a comparator field index.
   * @param index comparator field index
   * @return request parameter name
   */
  private def fieldParameter(index: Int): String = s"field_$index"

  /**
   * Reads a request parameter, returning an empty string when absent.
   * @param request HTTP request
   * @param name parameter name
   * @return parameter value or an empty string
   */
  private def parameterValue(request: HttpServletRequest, name: String): String = Option(request.getParameter(name)).getOrElse("")

  /**
   * Escapes text before inserting it into HTML output.
   * @param value text to escape
   * @return HTML-safe text
   */
  private def escape(value: String): String =
    value.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace("\"", "&quot;").replace("'", "&#39;")

  /**
   * Converts an exception into a non-empty display message.
   * @param error exception to format
   * @return displayable error message
   */
  private def message(error: Throwable): String = Option(error.getMessage).filter(_.nonEmpty).getOrElse(error.getClass.getSimpleName)

private final case class CandidateDocument(document: Document,
                                           similarities: Map[String, SimilarityStatus])

private final case class TableCell(value: String,
                                   isSimilar: Option[SimilarityStatus])

private final case class SearchResult(candidates: Seq[CandidateDocument], error: Option[String])

private object SearchResult:
  /**
   * Creates a successful search result.
   * @param candidates matching candidate documents
   * @return successful result containing the candidates
   */
  def success(candidates: Seq[CandidateDocument]): SearchResult = SearchResult(candidates, None)

  /**
   * Creates a failed search result with a displayable message.
   * @param message error message
   * @return failed result containing the message
   */
  def error(message: String): SearchResult = SearchResult(Seq.empty, Some(message))
