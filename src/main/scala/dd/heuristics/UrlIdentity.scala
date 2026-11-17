package dd.heuristics

import dd.comparators.UrlsComparator
import dd.interfaces.SimilarityStatus
import dd.interfaces.SimilarityStatus.{no, undefined, yes}

import java.io.InputStream
import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.nio.charset.{Charset, StandardCharsets}
import java.security.MessageDigest
import java.text.Normalizer
import java.time.Duration
import java.util.HexFormat
import java.util.concurrent.ConcurrentHashMap
import scala.collection.mutable
import scala.util.{Try, Using}
import scala.util.matching.Regex

/**
 * Decides whether two URLs point to the same document. The checks run in
 * layers, from the cheapest to the most expensive, and the first layer able
 * to decide wins:
 *
 *  1. Offline: identifiers read from the URL itself (DOI in the URL, SciELO
 *     {@code pid}, SciELO article id in {@code /j/<journal>/a/<id>/}).
 *  1. Network: both URLs end, after redirects, at the same address (ignoring
 *     scheme, {@code www.}, trailing slash, language and tracking parameters).
 *  1. Network: identifiers published in the page metadata
 *     ({@code citation_doi} and similar, {@code <link rel="canonical">},
 *     {@code og:url}).
 *  1. Network: identical bodies (e.g. PDF) or similar visible text, after
 *     removing scripts, styles and tags (word 3-shingle Jaccard similarity).
 *
 * The result has three states: {@code yes} (same document), {@code no}
 * (different documents) and {@code undefined} (could not be decided, e.g. a
 * download failed). Downloaded pages are cached by URL, requests to the same
 * host are spaced out and 429/5xx answers are retried.
 */
object UrlIdentity:
  /** Identifiers of a document, by kind: doi, scielo_pid, scielo_aid, canonical. */
  type Identifiers = Map[String, String]

  /** Identifier kinds, by decreasing priority; the first kind both sides share decides. */
  private val IdentifierKinds: Seq[String] = Seq("doi", "scielo_pid", "scielo_aid", "canonical")

  /** Minimum visible-text similarity for two pages to be the same document. */
  private[dd] val MinTextSimilarity: Double = 0.9

  /** Query parameters that do not change the document (language and tracking). */
  private val IgnoredParameters: Set[String] = Set("lang", "lng", "tlng", "nrm", "locale", "fbclid", "gclid")

  /** SciELO issue order used by ahead-of-print articles, whose pid changes on publication. */
  private val AheadOfPrintIssue: String = "0050"

  private val ConnectTimeout: Duration = Duration.ofSeconds(10)
  private val RequestTimeout: Duration = Duration.ofSeconds(20)
  private val MaxBodyBytes: Int = 5 * 1024 * 1024
  private val MaxAttempts: Int = 3
  private val RetryDelayMillis: Long = 1000
  private val HostIntervalMillis: Long = 250
  private val MaxCacheSize: Int = 20000
  private val UserAgent: String = "DeDup2/1.0 (bibliographic deduplication)"

  private val DoiRegex: Regex = """(?i)\b(10\.\d{4,9}/[^\s"'<>&?#]+)""".r
  private val ScieloArticleRegex: Regex = """/j/[^/]+/a/([^/?#]+)""".r
  private val ScieloPidRegex: Regex = """(?i)^S\d{4}-[\dX]{4}\d{4}(\d{4})\d{5}$""".r
  private val MetaTagRegex: Regex = """(?is)<meta\s[^>]*>""".r
  private val LinkTagRegex: Regex = """(?is)<link\s[^>]*>""".r
  private val AttributeRegex: Regex = """(?s)([\w:.-]+)\s*=\s*(?:"([^"]*)"|'([^']*)'|([^\s"'>]+))""".r
  private val CharsetRegex: Regex = """(?i)charset\s*=\s*["']?([\w.:-]+)""".r
  private val InvisibleRegex: Regex = """(?is)<(script|style|noscript|template)\b.*?</\1\s*>|<!--.*?-->""".r
  private val TagRegex: Regex = """(?s)<[^>]*>""".r
  private val EntityRegex: Regex = """&(#\d+|#x[\da-fA-F]+|[a-zA-Z]+);""".r

  /** Meta tag names (lower case) whose content holds the document DOI. */
  private val DoiMetaNames: Set[String] =
    Set("citation_doi", "dc.identifier", "dc.identifier.doi", "prism.doi", "bepress_citation_doi", "eprints.id_number")

  /**
   * What is kept of a downloaded URL.
   *
   * @param finalUri address reached after redirects
   * @param finalUrl normalized address reached after redirects
   * @param identifiers identifiers read from the final address and the page metadata
   * @param shingles hashes of the word 3-shingles of the visible text (empty for non-HTML)
   * @param bodyHash SHA-256 of the body
   */
  private[dd] final case class Page(finalUri: URI,
                                    finalUrl: String,
                                    identifiers: Identifiers,
                                    shingles: Set[Int],
                                    bodyHash: String)

  private lazy val httpClient: HttpClient =
    HttpClient.newBuilder()
      .connectTimeout(ConnectTimeout)
      .followRedirects(HttpClient.Redirect.NORMAL)
      .build()

  private val cache: ConcurrentHashMap[String, Option[Page]] = ConcurrentHashMap()
  private val nextRequest: mutable.Map[String, Long] = mutable.Map()

  /**
   * Compares every original URL with every current URL.
   *
   * The offline layer runs on all pairs first; the network layers run only
   * when it decides no pair as the same document.
   *
   * @param originalUrls URLs of the original document
   * @param currentUrls URLs of the current document
   * @return {@code yes} when any pair is the same document; otherwise
   *         {@code no} when any pair is decided as different; otherwise
   *         {@code undefined}
   */
  def status(originalUrls: Seq[String], currentUrls: Seq[String]): SimilarityStatus =
    val pairs: Seq[(String, String)] = for original <- originalUrls; current <- currentUrls yield (original, current)
    val offline: Seq[SimilarityStatus] = pairs.map((original, current) => offlineStatus(original, current))

    if offline.contains(yes) then yes
    else
      val statuses: LazyList[SimilarityStatus] = pairs.zip(offline).to(LazyList).map:
        case ((original, current), `undefined`) => onlineStatus(original, current)
        case (_, decided) => decided

      if statuses.contains(yes) then yes
      else if statuses.contains(no) then no
      else undefined

  /** Layer 1: compares the identifiers written in the URLs, without network access. */
  private[dd] def offlineStatus(original: String, current: String): SimilarityStatus =
    compareIdentifiers(urlIdentifiers(original), urlIdentifiers(current))

  /** Layers 2 to 4: download both URLs and compare address, metadata and text. */
  private def onlineStatus(original: String, current: String): SimilarityStatus =
    (page(original), page(current)) match
      case (Some(originalPage), Some(currentPage)) =>
        if originalPage.finalUrl == currentPage.finalUrl then yes
        else
          compareIdentifiers(urlIdentifiers(original) ++ originalPage.identifiers,
                             urlIdentifiers(current) ++ currentPage.identifiers) match
            case `undefined` => compareContents(originalPage, currentPage)
            case decided => decided
      case _ => undefined

  /** Layer 4: identical bodies or similar visible text. */
  private def compareContents(original: Page, current: Page): SimilarityStatus =
    if original.bodyHash == current.bodyHash then yes
    else if original.shingles.isEmpty || current.shingles.isEmpty then undefined
    else if jaccard(original.shingles, current.shingles) >= MinTextSimilarity then yes
    else no

  /** The first identifier kind present on both sides decides. */
  private def compareIdentifiers(original: Identifiers, current: Identifiers): SimilarityStatus =
    IdentifierKinds.find(kind => original.contains(kind) && current.contains(kind)) match
      case Some(kind) => if original(kind) == current(kind) then yes else no
      case None => undefined

  /**
   * Reads the identifiers written in a URL.
   *
   * An ahead-of-print SciELO pid is ignored, because the article receives a
   * new pid when it is published in an issue.
   */
  private[dd] def urlIdentifiers(url: String): Identifiers =
    Try(URI.create(fetchUrl(url))).toOption.filter(_.getHost != null) match
      case None => Map.empty
      case Some(uri) =>
        val host: String = uri.getHost.toLowerCase
        val decoded: String = UrlsComparator.cleanUrl(url)
        val doi: Option[String] = extractDoi(decoded)
        val scielo: Boolean = host.contains("scielo")
        val pid: Option[String] =
          if scielo then queryParameters(uri).get("pid").map(_.trim.toUpperCase).filter(isRegularPid)
          else None
        val articleId: Option[String] =
          if scielo then ScieloArticleRegex.findFirstMatchIn(Option(uri.getPath).getOrElse("")).map(_.group(1))
          else None

        doi.map("doi" -> _).toMap ++ pid.map("scielo_pid" -> _) ++ articleId.map("scielo_aid" -> _)

  /** Indicates whether a SciELO pid is well formed and not ahead of print. */
  private def isRegularPid(pid: String): Boolean =
    pid match
      case ScieloPidRegex(issue) => issue != AheadOfPrintIssue
      case _ => false

  /**
   * Returns the URL that is downloaded: the cleaned URL without fragment.
   *
   * @param url URL as stored in the document
   * @return URL ready to be requested
   */
  private[dd] def fetchUrl(url: String): String =
    UrlsComparator.cleanUrl(url).takeWhile(_ != '#')

  /**
   * Normalizes an address for comparison: no scheme, lower-case host without
   * {@code www.}, no trailing slash and sorted query parameters, except the
   * language and tracking ones.
   */
  private[dd] def normalizeUrl(uri: URI): String =
    val host: String = Option(uri.getHost).getOrElse("").toLowerCase.stripPrefix("www.")
    val port: String = if uri.getPort < 0 || uri.getPort == 80 || uri.getPort == 443 then "" else s":${uri.getPort}"
    val path: String = Option(uri.getPath).getOrElse("").stripSuffix("/")
    val query: String = queryParameters(uri).toSeq
      .filterNot((key, _) => IgnoredParameters.contains(key) || key.startsWith("utm_"))
      .sorted
      .map((key, value) => if value.isEmpty then key else s"$key=$value")
      .mkString("&")

    s"$host$port$path${if query.isEmpty then "" else s"?$query"}"

  /** Query parameters of a URI, with lower-case names. */
  private def queryParameters(uri: URI): Map[String, String] =
    Option(uri.getQuery).toSeq
      .flatMap(_.split("&"))
      .filter(_.nonEmpty)
      .map(_.split("=", 2))
      .map(parts => parts(0).trim.toLowerCase -> parts.lift(1).getOrElse("").trim)
      .toMap

  /** Extracts a lower-case DOI from a text, without trailing punctuation. */
  private[dd] def extractDoi(text: String): Option[String] =
    DoiRegex.findFirstMatchIn(text)
      .map(_.group(1).reverse.dropWhile(".,;:)]}".contains(_)).reverse.toLowerCase)

  /**
   * Returns the address a URL ends at after following its redirects.
   *
   * @param url URL as stored in the document
   * @return final address, or {@code None} when the URL is invalid,
   *         unreachable, does not answer with a 2xx status or ends at the
   *         root of a site
   */
  def finalUri(url: String): Option[URI] =
    page(url).map(_.finalUri)

  /** Downloads a URL once and keeps the result in the cache. */
  private[dd] def page(url: String): Option[Page] =
    val key: String = fetchUrl(url)
    if cache.size > MaxCacheSize then cache.clear()
    cache.computeIfAbsent(key, download(_, 1))

  /**
   * Downloads a URL, retrying 429 and 5xx answers.
   *
   * @return the page, or {@code None} when the URL is invalid, unreachable,
   *         does not answer with a 2xx status or ends at the root of a site
   */
  private def download(url: String, attempt: Int): Option[Page] =
    val response: Option[HttpResponse[InputStream]] =
      Try(URI.create(url)).toOption.filter(_.getHost != null).flatMap: uri =>
        throttle(uri.getHost.toLowerCase)
        Try:
          val request: HttpRequest = HttpRequest.newBuilder(uri)
            .timeout(RequestTimeout)
            .header("User-Agent", UserAgent)
            .header("Accept", "text/html,application/xhtml+xml,application/pdf;q=0.9,*/*;q=0.8")
            .GET()
            .build()
          httpClient.send(request, HttpResponse.BodyHandlers.ofInputStream())
        .toOption

    response match
      case Some(resp) if resp.statusCode() / 100 == 2 =>
        val body: Option[Array[Byte]] = Using(resp.body())(_.readNBytes(MaxBodyBytes)).toOption
        if isSiteRoot(resp.uri()) then None
        else body.map(bytes => toPage(resp.uri(), resp.headers().firstValue("Content-Type").orElse(""), bytes))
      case Some(resp) =>
        Try(resp.body().close())
        if (resp.statusCode() == 429 || resp.statusCode() / 100 == 5) && attempt < MaxAttempts then
          Thread.sleep(RetryDelayMillis * attempt)
          download(url, attempt + 1)
        else None
      case None => None

  /** Waits so that requests to the same host are at least {@code HostIntervalMillis} apart. */
  private def throttle(host: String): Unit =
    val waitMillis: Long = nextRequest.synchronized:
      val now: Long = System.currentTimeMillis()
      val slot: Long = math.max(now, nextRequest.getOrElse(host, now))
      nextRequest(host) = slot + HostIntervalMillis
      slot - now
    if waitMillis > 0 then Thread.sleep(waitMillis)

  /** Indicates whether a URI points to the root (home page) of a site. */
  private def isSiteRoot(uri: URI): Boolean =
    val path: String = Option(uri.getPath).getOrElse("")
    (path.isEmpty || path == "/") && Option(uri.getQuery).forall(_.isEmpty)

  /** Extracts the final address, identifiers and visible text of a downloaded body. */
  private def toPage(finalUri: URI, contentType: String, body: Array[Byte]): Page =
    val bodyHash: String = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(body))
    val finalIdentifiers: Identifiers = urlIdentifiers(finalUri.toString)
    val isHtml: Boolean = contentType.toLowerCase.contains("html") ||
      (contentType.isEmpty && new String(body.take(512), StandardCharsets.ISO_8859_1).toLowerCase.contains("<html"))

    if !isHtml then Page(finalUri, normalizeUrl(finalUri), finalIdentifiers, Set.empty, bodyHash)
    else
      val html: String = new String(body, charset(contentType, body))
      Page(finalUri, normalizeUrl(finalUri), finalIdentifiers ++ metadataIdentifiers(html, finalUri),
           shingles(visibleText(html)), bodyHash)

  /** Charset from the Content-Type header or the page meta tags; UTF-8 by default. */
  private def charset(contentType: String, body: Array[Byte]): Charset =
    val head: String = new String(body.take(4096), StandardCharsets.ISO_8859_1)
    CharsetRegex.findFirstMatchIn(contentType).orElse(CharsetRegex.findFirstMatchIn(head))
      .flatMap(found => Try(Charset.forName(found.group(1))).toOption)
      .getOrElse(StandardCharsets.UTF_8)

  /**
   * Layer 3: reads the DOI and the canonical address published in the page.
   * A canonical address pointing to the root of a site is ignored.
   */
  private[dd] def metadataIdentifiers(html: String, baseUri: URI): Identifiers =
    val metas: Seq[Map[String, String]] = MetaTagRegex.findAllIn(html).map(attributes).toSeq
    val links: Seq[Map[String, String]] = LinkTagRegex.findAllIn(html).map(attributes).toSeq

    val doi: Option[String] = metas.iterator
      .filter(meta => meta.get("name").orElse(meta.get("property")).exists(name => DoiMetaNames.contains(name.toLowerCase)))
      .flatMap(meta => meta.get("content").flatMap(extractDoi))
      .nextOption()
    val canonicalLink: Option[String] = links
      .find(_.get("rel").exists(_.toLowerCase.split("\\s+").contains("canonical")))
      .flatMap(_.get("href"))
    val ogUrl: Option[String] = metas
      .find(_.get("property").exists(_.equalsIgnoreCase("og:url")))
      .flatMap(_.get("content"))
    val canonical: Option[URI] = canonicalLink.orElse(ogUrl)
      .flatMap(href => Try(baseUri.resolve(UrlsComparator.cleanUrl(href))).toOption)
      .filter(uri => uri.getHost != null && !isSiteRoot(uri))

    canonical.map(uri => urlIdentifiers(uri.toString)).getOrElse(Map.empty) ++
      canonical.map(uri => "canonical" -> normalizeUrl(uri)) ++
      doi.map("doi" -> _)

  /** Attributes of a tag, with lower-case names and unescaped values. */
  private def attributes(tag: String): Map[String, String] =
    AttributeRegex.findAllMatchIn(tag)
      .map(found => found.group(1).toLowerCase ->
        unescape(Option(found.group(2)).orElse(Option(found.group(3))).getOrElse(found.group(4))))
      .toMap

  /** Layer 4: visible text of a page, without scripts, styles, comments and tags. */
  private[dd] def visibleText(html: String): String =
    unescape(TagRegex.replaceAllIn(InvisibleRegex.replaceAllIn(html, " "), " "))

  /** Hashes of the word 3-shingles of a text, ignoring case, accents and punctuation. */
  private[dd] def shingles(text: String): Set[Int] =
    val words: Seq[String] = Normalizer.normalize(text.toLowerCase, Normalizer.Form.NFD)
      .replaceAll("\\p{M}", "")
      .split("[^\\p{L}\\p{N}]+")
      .toSeq
      .filter(_.nonEmpty)

    if words.isEmpty then Set.empty
    else words.sliding(3).map(_.mkString(" ").hashCode).toSet

  private def jaccard(left: Set[Int], right: Set[Int]): Double =
    left.intersect(right).size.toDouble / left.union(right).size

  /** Unescapes numeric and the most common named HTML entities. */
  private def unescape(text: String): String =
    EntityRegex.replaceAllIn(text, found =>
      val entity: String = found.group(1)
      val replacement: String =
        if entity.startsWith("#x") || entity.startsWith("#X") then
          Try(new String(Character.toChars(Integer.parseInt(entity.drop(2), 16)))).getOrElse(found.matched)
        else if entity.startsWith("#") then
          Try(new String(Character.toChars(entity.drop(1).toInt))).getOrElse(found.matched)
        else
          entity match
            case "amp" => "&"
            case "lt" => "<"
            case "gt" => ">"
            case "quot" => "\""
            case "apos" => "'"
            case "nbsp" => " "
            case _ => found.matched
      Regex.quoteReplacement(replacement))
