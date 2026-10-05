package dd.comparators

import dd.interfaces.{CompResult, Comparator, Document, SimilarityStatus}

import java.net.{URI, URLDecoder}
import java.nio.charset.StandardCharsets

case class UrlElement(rawUrl: String,
                      decodedUrl: String,
                      host: String,
                      port: Int,
                      params: Map[String,String])

class UrlsComparator(fieldName: String,
                     occSeparator: String = "//@//") extends Comparator:
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

    val similarPair: Option[(UrlElement, UrlElement)] =
      oriUrls.iterator
        .flatMap(oriUrl => curUrls.iterator.map(curUrl => (oriUrl, curUrl)))
        .find {
          case (oriUrl, curUrl) =>
            isSimilar(oriUrl, curUrl)
        }

    similarPair match {
      case Some(pair) =>
        CompResult(name="UrlsComparator",
                   fieldName=fieldName,
                   originalField=pair._1.rawUrl,
                   currentField=pair._2.rawUrl,
                   originalFieldOther=Some(pair._1.decodedUrl),
                   currentFieldOther=Some(pair._2.decodedUrl),
                   similarity=1,
                   isSimilar=SimilarityStatus.yes)
      case None =>
        val originalUrls = oriUrls.map(_.rawUrl).mkString(occSeparator)
        val currentUrls = curUrls.map(_.rawUrl).mkString(occSeparator)
        val decodedOriginalUrls = oriUrls.map(_.decodedUrl).mkString(occSeparator)
        val decodedCurrentUrls = curUrls.map(_.decodedUrl).mkString(occSeparator)
        val originalEmpty = originalUrls.trim.isEmpty
        val currentEmpty = currentUrls.trim.isEmpty
        val status =
          if originalEmpty && currentEmpty then SimilarityStatus.yes
          else if originalEmpty != currentEmpty then SimilarityStatus.undefined
          else SimilarityStatus.no
        val score = if status == SimilarityStatus.yes then 1 else 0
        CompResult(name="UrlsComparator",
                   fieldName=fieldName,
                   originalField=originalUrls,
                   currentField=currentUrls,
                   originalFieldOther=Some(decodedOriginalUrls),
                   currentFieldOther=Some(decodedCurrentUrls),
                   similarity=score,
                   isSimilar=status)
    }

  private def isSimilar(originalUrl: UrlElement,
                        currentUrl: UrlElement): Boolean = {
    val originalId: String = originalUrl.params.get("id").orElse(originalUrl.params.get("pid")).getOrElse("")
    val currentId: String = currentUrl.params.get("id").orElse(currentUrl.params.get("pid")).getOrElse("")
    val bothEmpty: Boolean = originalId.isEmpty && currentId.isEmpty
    val similar: Boolean = !bothEmpty && originalId.equals(currentId)

    (originalUrl.host == currentUrl.host) &&
    (originalUrl.port == currentUrl.port) &&
    similar
  }


  private def getUrls(doc: Document): Seq[UrlElement] =
    doc.fields
      .collectFirst:
        case (`fieldName`, value) => value
      .toSeq
      .flatMap(_.split(occSeparator))
      .map(toUrlElement)

  private def toUrlElement(value: String): UrlElement =

    val decodedValue = URLDecoder.decode(value, StandardCharsets.UTF_8)
    val uri = URI.create(decodedValue)

    val params =
      Option(uri.getQuery)
        .iterator
        .flatMap(_.split("&"))
        .map(_.split("=", 2))
        .collect:
           case Array(key, value) => key -> value
           case Array(key) => key -> ""
        .toMap

    UrlElement(
      rawUrl = value,
      decodedUrl = uri.toURL.toString,
      host = uri.getHost,
      port = uri.getPort,
      params = params
    )
