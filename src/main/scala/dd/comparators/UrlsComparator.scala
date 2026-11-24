package dd.comparators

import dd.heuristics.UrlIdentity
import dd.interfaces.{CompResult, Comparator, Document, SimilarityStatus}

import java.net.{URI, URLDecoder}
import java.nio.charset.StandardCharsets
import scala.util.Try
import scala.util.matching.Regex

/**
 * One parsed URL.
 *
 * @param rawUrl URL as stored in the document (a single occurrence)
 * @param decodedUrl cleaned URL (see {@link UrlsComparator.cleanUrl})
 * @param host host name
 * @param port port, or -1 when the URL has none
 * @param path path
 * @param params query parameters
 */
case class UrlElement(rawUrl: String,
                      decodedUrl: String,
                      host: String,
                      port: Int,
                      path: String,
                      params: Map[String,String])

/**
 * Comparator of URL fields.
 *
 * The field may hold several URLs separated by {@code occSeparator}, and the
 * field itself may be repeated. Two URLs identify the same document when
 * {@link UrlsComparator.sameUrl} accepts them: same host and port and the same
 * {@code id}/{@code pid} parameter or, without one, the same path.
 *
 *  1. Every original URL is compared with every current URL as stored,
 *     without network access.
 *  1. Only when no pair matches, and {@code followRedirects} is set, the
 *     redirects of each pair are followed and the same rule is applied to
 *     the final URLs (see {@link UrlsComparator.hasSameFinalUrl}).
 *
 * The result is {@code yes} when a pair matches, {@code undefined} when one
 * of the documents has no valid URL and {@code no} otherwise.
 *
 * @param fieldName name of the URL field
 * @param occSeparator separator of the URLs inside a field value
 * @param followRedirects whether the final URLs are checked when the stored
 *                        URLs do not match (accesses the network)
 */
class UrlsComparator(fieldName: String,
                     occSeparator: String = UrlsComparator.DefaultOccSeparator,
                     followRedirects: Boolean = true) extends Comparator:
  /**
   * Compares the input documents and returns the comparison result.
   *
   * @param originalDoc source document used in the comparison
   * @param currentDoc candidate document being evaluated
   * @return comparison result describing the evaluated documents
   */
  override def compare(originalDoc: Document,
                       currentDoc: Document): CompResult =
    val oriUrls: Seq[UrlElement] = getUrls(originalDoc)
    val curUrls: Seq[UrlElement] = getUrls(currentDoc)

    def pairs: Iterator[(UrlElement, UrlElement)] =
      for oriUrl <- oriUrls.iterator; curUrl <- curUrls.iterator yield (oriUrl, curUrl)

    val similarPair: Option[(UrlElement, UrlElement)] =
      pairs.find((oriUrl, curUrl) => UrlsComparator.sameUrl(oriUrl, curUrl))
        .orElse:
          if followRedirects then pairs.find((oriUrl, curUrl) => UrlsComparator.hasSameFinalUrl(oriUrl, curUrl))
          else None

    similarPair match
      case Some((oriUrl, curUrl)) =>
        CompResult(name = "UrlsComparator",
                   fieldName = fieldName,
                   originalField = oriUrl.rawUrl,
                   currentField = curUrl.rawUrl,
                   originalFieldOther = Some(oriUrl.decodedUrl),
                   currentFieldOther = Some(curUrl.decodedUrl),
                   similarity = 1.0,
                   isSimilar = SimilarityStatus.yes)
      case None =>
        val status: SimilarityStatus =
          if oriUrls.isEmpty || curUrls.isEmpty then SimilarityStatus.undefined
          else SimilarityStatus.no
        CompResult(name = "UrlsComparator",
                   fieldName = fieldName,
                   originalField = oriUrls.map(_.rawUrl).mkString(occSeparator),
                   currentField = curUrls.map(_.rawUrl).mkString(occSeparator),
                   originalFieldOther = Some(oriUrls.map(_.decodedUrl).mkString(occSeparator)),
                   currentFieldOther = Some(curUrls.map(_.decodedUrl).mkString(occSeparator)),
                   similarity = 0.0,
                   isSimilar = status)

  /**
   * Reads the valid URLs of every occurrence of the field.
   *
   * @param doc document to read
   * @return parsed URLs; invalid ones are ignored
   */
  private def getUrls(doc: Document): Seq[UrlElement] =
    doc.fields
      .collect { case (`fieldName`, value) => value }
      .flatMap(UrlsComparator.urlElements(_, occSeparator))

object UrlsComparator:
  /** Default separator of the URLs inside a field value. */
  val DefaultOccSeparator: String = "//@//"

  /** HTML entities found in URLs exported from fi-admin. */
  private val HtmlEntities: Seq[(String, String)] =
    Seq("&quot;" -> "\"", "&#34;" -> "\"", "&#39;" -> "'", "&lt;" -> "<", "&gt;" -> ">")

  /** Characters not allowed by java.net.URI that may remain in a stored URL. */
  private val IllegalChars: Map[Char, String] =
    Map('"' -> "%22", ' ' -> "%20", '<' -> "%3C", '>' -> "%3E", '{' -> "%7B", '}' -> "%7D",
        '|' -> "%7C", '\\' -> "%5C", '^' -> "%5E", '`' -> "%60")

  /** Quote characters removed from the ends of a URL. */
  private val Quotes: String = "\"'"

  /** Ports that are the default of the http or https scheme. */
  private val DefaultPorts: Set[Int] = Set(-1, 80, 443)

  /**
   * Checks whether two URLs identify the same document, without network
   * access: same host (ignoring case), same port (a default port, explicit
   * or not, matches any other default port, so http and https URLs of the
   * same site match) and the same {@code id} (or {@code pid}) parameter or,
   * when neither URL has one, the same path. Other parameters and the
   * fragment are not compared.
   *
   * @param originalUrl URL of the original document
   * @param currentUrl URL of the current document
   * @return true when both URLs identify the same document
   */
  def sameUrl(originalUrl: UrlElement,
              currentUrl: UrlElement): Boolean =
    sameSite(originalUrl, currentUrl) && sameIdOrPath(originalUrl, currentUrl)

  /**
   * Checks whether two URLs, after following their redirects, end at the
   * same document according to {@link sameUrl}. Downloads are shared with
   * {@link dd.heuristics.UrlIdentity} (same cache, retries and request
   * spacing).
   *
   * @param originalUrl URL of the original document
   * @param currentUrl URL of the current document
   * @return {@code true} when both final URLs are known and identify the same
   *         document; {@code false} when a URL is unreachable, does not
   *         answer with a 2xx status, ends at the root of a site, or the
   *         final URLs differ
   */
  def hasSameFinalUrl(originalUrl: UrlElement,
                      currentUrl: UrlElement): Boolean =
    //println("==> hasSameFinalUrl")
    finalUrl(originalUrl).exists(originalFinal => finalUrl(currentUrl).exists(sameUrl(originalFinal, _)))

  /**
   * Follows the redirects of a URL.
   *
   * @param url URL to follow
   * @return the final URL, or None when it cannot be reached
   */
  private def finalUrl(url: UrlElement): Option[UrlElement] =
    UrlIdentity.finalUri(url.rawUrl).flatMap(uri => fromUri(uri.toString, uri))

  /** Same host (ignoring case) and same port (all default ports are equal). */
  private def sameSite(originalUrl: UrlElement,
                       currentUrl: UrlElement): Boolean =
    def port(url: UrlElement): Int = if DefaultPorts.contains(url.port) then -1 else url.port
    Option(originalUrl.host).exists(_.equalsIgnoreCase(currentUrl.host)) && port(originalUrl) == port(currentUrl)

  /**
   * Checks the document identification of two URLs of the same site: both
   * have the same {@code id} (or {@code pid}) parameter or, when neither has
   * one, both have the same path.
   *
   * @param originalUrl URL of the original document
   * @param currentUrl URL of the current document
   * @return true when both URLs identify the same document of the site
   */
  private[dd] def sameIdOrPath(originalUrl: UrlElement,
                               currentUrl: UrlElement): Boolean =
    def id(url: UrlElement): String = url.params.get("id").orElse(url.params.get("pid")).getOrElse("")
    val originalId: String = id(originalUrl)
    val currentId: String = id(currentUrl)

    if originalId.isEmpty && currentId.isEmpty then originalUrl.path == currentUrl.path
    else originalId == currentId

  /**
   * Splits a field value into its URL occurrences.
   *
   * @param value field value
   * @param occSeparator separator of the occurrences (a literal text)
   * @return the occurrences that are valid URLs
   */
  private[dd] def urlElements(value: String,
                              occSeparator: String = DefaultOccSeparator): Seq[UrlElement] =
    Option(value).toSeq
      .flatMap(_.split(Regex.quote(occSeparator)))
      .map(_.trim)
      .filter(_.nonEmpty)
      .flatMap(toUrlElement)

  /**
   * Parses one stored URL, after cleaning it (see {@link cleanUrl}).
   *
   * @param value URL as stored in the document
   * @return the parsed URL, or None when it is not a valid absolute URL
   */
  private[dd] def toUrlElement(value: String): Option[UrlElement] =
    Try(URI.create(cleanUrl(value))).toOption.flatMap(fromUri(value, _))

  /**
   * Builds the URL element of a parsed URI.
   *
   * @param rawUrl URL as stored, kept in the element
   * @param uri parsed URI
   * @return the URL element, or None when the URI is not an absolute URL
   */
  private def fromUri(rawUrl: String,
                      uri: URI): Option[UrlElement] =
    Try:
      val params: Map[String, String] =
        Option(uri.getQuery)
          .iterator
          .flatMap(_.split("&"))
          .map(_.split("=", 2))
          .collect:
            case Array(key, value) => key -> value
            case Array(key) => key -> ""
          .toMap

      UrlElement(
        rawUrl = rawUrl,
        decodedUrl = uri.toURL.toString,
        host = uri.getHost,
        port = uri.getPort,
        path = uri.getPath,
        params = params
      )
    .toOption

  /**
   * Cleans a stored URL so that java.net.URI can parse it.
   *
   * HTML entities are unescaped ({@code &amp;} becomes {@code &}, also when
   * escaped more than once), the value is percent-decoded when possible,
   * surrounding quotes and spaces are removed, and the remaining characters
   * forbidden in a URI are percent-encoded.
   *
   * @param value URL as stored in the document
   * @return URL ready to be parsed
   */
  private[dd] def cleanUrl(value: String): String =
    val unescaped: String = HtmlEntities.foldLeft(unescapeAmp(value)):
      case (acc, (entity, char)) => acc.replace(entity, char)
    val decoded: String = Try(URLDecoder.decode(unescaped, StandardCharsets.UTF_8)).getOrElse(unescaped)
    val trimmed: String = decoded.trim.dropWhile(Quotes.contains(_)).reverse.dropWhile(Quotes.contains(_)).reverse.trim

    trimmed.flatMap(char => IllegalChars.getOrElse(char, char.toString))

  /** Replaces {@code &amp;} until none is left, covering double escaping. */
  @annotation.tailrec
  private def unescapeAmp(value: String): String =
    val replaced: String = value.replace("&amp;", "&")
    if replaced == value then value else unescapeAmp(replaced)
