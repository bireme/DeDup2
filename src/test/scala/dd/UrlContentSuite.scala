package dd

import com.sun.net.httpserver.HttpServer
import dd.heuristics.Util
import dd.interfaces.{CompResult, SimilarityStatus}

import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.AtomicReference

class UrlContentSuite extends munit.FunSuite:
  private val field: String = "electronic_address"

  /** Queries received by the local server, used to check the stripped URL. */
  private val lastQuery: AtomicReference[String] = AtomicReference("")

  /** Local HTTP server whose page content depends only on the pid parameter. */
  private val server: Fixture[HttpServer] = new Fixture[HttpServer]("server"):
    private var instance: HttpServer = null
    def apply(): HttpServer = instance
    override def beforeAll(): Unit =
      instance = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0)
      instance.createContext("/page", exchange =>
        val query: String = Option(exchange.getRequestURI.getRawQuery).getOrElse("")
        lastQuery.set(query)
        val body: Array[Byte] = s"content of $query".getBytes(StandardCharsets.UTF_8)
        exchange.sendResponseHeaders(200, body.length)
        exchange.getResponseBody.write(body)
        exchange.close()
      )
      instance.createContext("/missing", exchange =>
        exchange.sendResponseHeaders(404, -1)
        exchange.close()
      )
      instance.start()
    override def afterAll(): Unit = instance.stop(0)

  override def munitFixtures: Seq[Fixture[?]] = Seq(server)

  private def base: String = s"http://127.0.0.1:${server().getAddress.getPort}"

  private def results(original: String, current: String): Seq[CompResult] =
    Seq(CompResult("UrlsComparator", field, original, current, None, None, 0.0, SimilarityStatus.no))

  test("contentUrl keeps only id and pid parameters"):
    assertEquals(
      Util.contentUrl("http://old.scielo.br/scielo.php?script=sci_arttext&amp;pid=S1519-698420040003&amp;lng=en#x"),
      "http://old.scielo.br/scielo.php?pid=S1519-698420040003")
    assertEquals(Util.contentUrl("http://h/p?a=1&id=7&b=2"), "http://h/p?id=7")
    assertEquals(Util.contentUrl("http://h/p?a=1"), "http://h/p")
    assertEquals(Util.contentUrl("http://h/p"), "http://h/p")

  test("same content after removing irrelevant parameters is similar"):
    assert(Util.isUrlContentSimilar(
      results(s"$base/page?script=a&pid=1&lng=en", s"$base/page?pid=1&lng=pt"), field))
    assertEquals(lastQuery.get(), "pid=1")

  test("different content is not similar"):
    assert(!Util.isUrlContentSimilar(results(s"$base/page?pid=1", s"$base/page?pid=2"), field))

  test("any matching occurrence is enough"):
    assert(Util.isUrlContentSimilar(
      results(s"$base/page?pid=9//@//$base/page?pid=1", s"$base/page?pid=1"), field))

  test("empty values, missing field and unavailable URLs are not similar"):
    assert(!Util.isUrlContentSimilar(results("", s"$base/page?pid=1"), field))
    assert(!Util.isUrlContentSimilar(results(s"$base/page?pid=1", " "), field))
    assert(!Util.isUrlContentSimilar(results(s"$base/page?pid=1", s"$base/page?pid=1"), "other"))
    assert(!Util.isUrlContentSimilar(results(s"$base/missing", s"$base/missing"), field))
    assert(!Util.isUrlContentSimilar(results("not a url", "not a url"), field))
