package dd.heuristics

import dd.interfaces.{CompResult, SimilarityStatus}

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.time.Duration
import java.util.Arrays
import scala.util.Try

/** Shared comparison helpers used by document heuristics. */
object Util:
  /** Tolerance used to absorb floating-point rounding errors in similarity scores. */
  private val Epsilon: Double = 1e-9

  /** Separator between occurrences of a multi-valued field. */
  private val OccSeparator: String = "//@//"

  /** Query parameters kept when building the URL whose content is downloaded. */
  private val IdParameters: Set[String] = Set("id", "pid")

  /** Maximum time spent downloading one URL. */
  private val UrlTimeout: Duration = Duration.ofSeconds(20)

  /** Shared HTTP client used to download URL contents. */
  private lazy val httpClient: HttpClient =
    HttpClient.newBuilder()
      .connectTimeout(Duration.ofSeconds(10))
      .followRedirects(HttpClient.Redirect.NORMAL)
      .build()

  extension (value: Double)
    /**
     * Compares two floating-point values allowing for rounding errors.
     *
     * @param other value to compare with
     * @return {@code true} when both values differ by less than {@code Epsilon}
     */
    def ~=(other: Double): Boolean = math.abs(value - other) < Epsilon

  /**
   * Returns the similarity score for a comparator field.
   *
   * @param results comparator results to search
   * @param fieldName comparator field name
   * @return the similarity score, or {@code 0.0} when no result exists
   */
  def similarity(results: Seq[CompResult], fieldName: String): Double =
    results.find(_.fieldName == fieldName).map(_.similarity).getOrElse(0.0)

  /**
   * Returns both values for a comparator field.
   *
   * @param results comparator results to search
   * @param fieldName comparator field name
   * @return original and current values, or two empty values when absent
   */
  def values(results: Seq[CompResult], fieldName: String): (String, String) =
    results.find(_.fieldName == fieldName)
      .map(result => (result.originalField, result.currentField))
      .getOrElse(("", ""))

  /**
   * Returns whether the comparator marked the specified field as similar.
   *
   * @param results comparator results to search
   * @param fieldName comparator field name
   * @return {@code true} only when the field result is {@code yes}
   */
  def isSimilar(results: Seq[CompResult], fieldName: String): Boolean =
    results.find(_.fieldName == fieldName)
      .exists(_.isSimilar == SimilarityStatus.yes)

  /**
   * Returns whether the comparator marked the specified field as undefined.
   *
   * @param results comparator results to search
   * @param fieldName comparator field name
   * @return {@code true} only when the field result is {@code undefined}
   */
  def isUndefined(results: Seq[CompResult], fieldName: String): Boolean =
    results.find(_.fieldName == fieldName)
      .exists(_.isSimilar == SimilarityStatus.undefined)

  /**
   * Returns whether the comparator marked the specified field as similar or undefined.
   *
   * @param results comparator results to search
   * @param fieldName comparator field name
   * @return {@code true} when the field status is {@code yes} or {@code undefined}
   */
  def isSimilarOrUndefined(results: Seq[CompResult], fieldName: String): Boolean =
    results.find(_.fieldName == fieldName)
      .exists(result => result.isSimilar == SimilarityStatus.yes || result.isSimilar == SimilarityStatus.undefined)

  /**
   * Returns whether the URLs of a comparator field point to the same content.
   *
   * Each URL is reduced to scheme, host, port and path, keeping only the
   * {@code id} and {@code pid} query parameters, and then downloaded. When a
   * field holds several URLs separated by {@code //@//}, the pair is similar if
   * any original URL has the same content as any current URL.
   *
   * @param results comparator results to search
   * @param fieldName comparator field name
   * @return {@code true} when both contents were downloaded and are equal;
   *         {@code false} when a value is empty, no content could be
   *         downloaded or the contents differ
   */
  def isUrlContentSimilar(results: Seq[CompResult], fieldName: String): Boolean =
    results.find(_.fieldName == fieldName).exists: result =>
      val originalUrls: Seq[String] = splitUrls(result.originalField)
      val currentUrls: Seq[String] = splitUrls(result.currentField)

      originalUrls.nonEmpty && currentUrls.nonEmpty && {
        val originalContents: Seq[Array[Byte]] = originalUrls.flatMap(url => urlContent(contentUrl(url)))
        lazy val currentContents: Seq[Array[Byte]] = currentUrls.flatMap(url => urlContent(contentUrl(url)))

        originalContents.nonEmpty &&
          currentContents.exists(current => originalContents.exists(original => Arrays.equals(original, current)))
      }

  /** Splits a field value into its non-empty URL occurrences. */
  private def splitUrls(value: String): Seq[String] =
    Option(value).toSeq
      .flatMap(_.split(OccSeparator))
      .map(_.trim)
      .filter(_.nonEmpty)

  /**
   * Removes the query parameters of a URL, except {@code id} and {@code pid}.
   * The fragment is dropped and HTML-escaped separators ({@code &amp;}) are
   * accepted.
   *
   * @param url URL to reduce
   * @return URL without the query parameters irrelevant to the content
   */
  private[heuristics] def contentUrl(url: String): String =
    val withoutFragment: String = url.trim.takeWhile(_ != '#')
    val queryStart: Int = withoutFragment.indexOf('?')

    if queryStart < 0 then withoutFragment
    else
      val base: String = withoutFragment.substring(0, queryStart)
      val kept: Seq[String] = withoutFragment.substring(queryStart + 1)
        .replace("&amp;", "&")
        .split("&")
        .toSeq
        .filter(param => IdParameters.contains(param.takeWhile(_ != '=').trim.toLowerCase))

      if kept.isEmpty then base else kept.mkString(s"$base?", "&", "")

  /**
   * Downloads the content of a URL.
   *
   * @param url URL to download
   * @return response body, or {@code None} when the URL is invalid,
   *         unreachable or does not answer with a 2xx status
   */
  private def urlContent(url: String): Option[Array[Byte]] =
    Try:
      val request: HttpRequest = HttpRequest.newBuilder(URI.create(url))
        .timeout(UrlTimeout)
        .GET()
        .build()
      httpClient.send(request, HttpResponse.BodyHandlers.ofByteArray())
    .toOption
      .filter(response => response.statusCode() / 100 == 2)
      .map(_.body())

  /**
   * Parses a textual similarity value.
   *
   * @param value textual similarity value
   * @return parsed similarity, or {@code NaN} when the value is invalid
   */
  def parseSimilarity(value: String): Double =
    Try(value.trim.toDouble).getOrElse(Double.NaN)

  /**
   * Compares two values after trimming surrounding whitespace.
   *
   * @param left first value
   * @param right second value
   * @return {@code true} when both normalized values are equal
   */
  def equal(left: String, right: String): Boolean =
    left.trim == right.trim

  /**
   * Determines whether a value is available for comparison.
   *
   * @param value value to inspect
   * @return {@code false} for an empty value or the marker {@code null}
   */
  def present(value: String): Boolean =
    val normalized: String = value.trim
    normalized.nonEmpty && !normalized.equalsIgnoreCase("null")

  /**
   * Checks whether two values are present and equal.
   *
   * @param left first value
   * @param right second value
   * @return {@code true} when neither value is missing and both are equal
   */
  def equalAndPresent(left: String, right: String): Boolean =
    present(left) && present(right) && equal(left, right)

  /**
   * Checks equality when both values are present; absent values do not fail the check.
   *
   * @param left first value
   * @param right second value
   * @return {@code true} when either value is absent or both values are equal
   */
  def equalOrAbsent(left: String, right: String): Boolean =
    if present(left) && present(right) then equal(left, right) else true

  /**
   * Checks whether both values are missing.
   *
   * @param left first value
   * @param right second value
   * @return {@code true} when both values are empty or {@code null}
   */
  def missing(left: String, right: String): Boolean =
    !present(left) && !present(right)
