package dd

import com.sun.net.httpserver.{HttpExchange, HttpServer}
import dd.heuristics.{UrlIdentity, Util}
import dd.interfaces.{CompResult, SimilarityStatus}
import dd.interfaces.SimilarityStatus.{no, undefined, yes}

import java.net.{InetSocketAddress, URI}
import java.nio.charset.StandardCharsets
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

class UrlContentSuite extends munit.FunSuite:
  private val field: String = "electronic_address"

  private val articleText: String =
    "Efeito do meloxicam na cicatrizacao de feridas cutaneas em ratos. Resumo: o estudo avaliou " +
    "a cicatrizacao em quarenta animais divididos em dois grupos tratados durante catorze dias."
  private val otherText: String =
    "Avaliacao da funcao renal em pacientes idosos internados. Resumo: estudo transversal com " +
    "cento e vinte pacientes acompanhados em hospital universitario durante dois anos."

  /** Requests received by the flaky page, which answers 502 the first time. */
  private val flakyRequests: AtomicInteger = AtomicInteger(0)

  /** Builds an HTML page with a random script, like the Bunny CDN injects in every answer. */
  private def html(text: String, head: String = ""): String =
    s"""<html><head><meta charset="utf-8">$head
       |<script src="/bunnyprint.js?key=${UUID.randomUUID()}"></script></head>
       |<body><div class="menu">Inicio</div><p>$text</p>
       |<script>var token = '${UUID.randomUUID()}';</script></body></html>""".stripMargin

  private def doiMeta(doi: String): String = s"""<meta name="citation_doi" content="$doi">"""
  private def ogMeta(url: String): String = s"""<meta property="og:url" content="$url"/>"""

  private def send(exchange: HttpExchange, status: Int, body: String, contentType: String = "text/html; charset=utf-8"): Unit =
    val bytes: Array[Byte] = body.getBytes(StandardCharsets.UTF_8)
    exchange.getResponseHeaders.add("Content-Type", contentType)
    exchange.sendResponseHeaders(status, if bytes.isEmpty then -1 else bytes.length)
    if bytes.nonEmpty then exchange.getResponseBody.write(bytes)
    exchange.close()

  private def redirect(exchange: HttpExchange, location: String): Unit =
    exchange.getResponseHeaders.add("Location", location)
    exchange.sendResponseHeaders(302, -1)
    exchange.close()

  /** Local HTTP server answering every test page. */
  private val server: Fixture[HttpServer] = new Fixture[HttpServer]("server"):
    private var instance: HttpServer = null
    def apply(): HttpServer = instance
    override def beforeAll(): Unit =
      instance = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0)
      instance.createContext("/", exchange =>
        exchange.getRequestURI.getPath match
          case "/" => send(exchange, 200, html("Pagina inicial da revista"))
          case "/meta/a" => send(exchange, 200, html(articleText, doiMeta("10.5555/ABC.1")))
          case "/meta/b" => send(exchange, 200, html(otherText, doiMeta("https://doi.org/10.5555/abc.1")))
          case "/meta/c" => send(exchange, 200, html(articleText, doiMeta("10.5555/abc.2")))
          case "/og/1" => send(exchange, 200, html(articleText, ogMeta("/canonical/x?lang=pt")))
          case "/og/2" => send(exchange, 200, html(otherText, ogMeta("/canonical/x/?lang=en")))
          case "/og/3" => send(exchange, 200, html(articleText, ogMeta("/canonical/y")))
          case "/text/a" | "/text/b" => send(exchange, 200, html(articleText))
          case "/text/c" => send(exchange, 200, html(otherText))
          case "/old/x" | "/old/y" => redirect(exchange, "/final")
          case "/final" => send(exchange, 200, html(s"${UUID.randomUUID()}"))
          case "/home-redirect/1" | "/home-redirect/2" => redirect(exchange, "/")
          case "/flaky" =>
            if flakyRequests.incrementAndGet() == 1 then send(exchange, 502, "")
            else send(exchange, 200, html(articleText))
          case "/pdf/a" | "/pdf/b" => send(exchange, 200, "%PDF-1.4 same", "application/pdf")
          case "/pdf/c" => send(exchange, 200, "%PDF-1.4 other", "application/pdf")
          case _ => send(exchange, 404, "")
      )
      instance.start()
    override def afterAll(): Unit = instance.stop(0)

  override def munitFixtures: Seq[Fixture[?]] = Seq(server)

  private def base: String = s"http://127.0.0.1:${server().getAddress.getPort}"

  private def results(original: String, current: String): Seq[CompResult] =
    Seq(CompResult("UrlsComparator", field, original, current, None, None, 0.0, SimilarityStatus.no))

  private def status(original: String, current: String): SimilarityStatus =
    Util.urlDocumentStatus(results(original, current), field)

  // Layer 1: identifiers in the URL, no network access.

  test("layer 1: SciELO pids decide without network access"):
    val pid = "http://%s/scielo.php?script=sci_arttext&amp;pid=S0102-865020000006000%s&lng=pt"
    assertEquals(status(pid.format("old.scielo.br", "08"), pid.format("www.scielo.br", "08")), yes)
    assertEquals(status(pid.format("www.scielo.br", "08"), pid.format("www.scielo.br", "39")), no)

  test("layer 1: ahead-of-print SciELO pids are not decisive"):
    val regular = "https://www.scielo.br/scielo.php?script=sci_arttext&pid=S1415-47572008000300022"
    val ahead = "https://www.scielo.br/scielo.php?script=sci_arttext&pid=S1415-47572008005000022"
    assertEquals(UrlIdentity.offlineStatus(regular, ahead), undefined)
    assertEquals(UrlIdentity.urlIdentifiers(ahead), Map.empty[String, String])

  test("layer 1: DOIs and SciELO article ids in the URL decide"):
    assertEquals(status("https://doi.org/10.1590/S0102-86502000000600008",
                        "http://dx.doi.org/10.1590/s0102-86502000000600008"), yes)
    assertEquals(status("https://doi.org/10.1590/S0102-86502000000600008",
                        "https://doi.org/10.1590/S0102-86502000000600039"), no)
    assertEquals(status("https://www.scielo.br/j/acb/a/Np9LTP7VbhzTkHGZqjbmvRr/?lang=pt",
                        "https://scielo.br/j/acb/a/Np9LTP7VbhzTkHGZqjbmvRr/?lang=en"), yes)

  test("extractDoi and normalizeUrl"):
    assertEquals(UrlIdentity.extractDoi("doi: 10.1590/S0102-865.2000)."), Some("10.1590/s0102-865.2000"))
    assertEquals(UrlIdentity.extractDoi("https://h/p?doi=10.1590/X1&lng=en"), Some("10.1590/x1"))
    assertEquals(UrlIdentity.extractDoi("no doi here"), None)
    assertEquals(UrlIdentity.normalizeUrl(URI.create("https://WWW.Site.org/a/b/?lang=pt&utm_source=x&id=2&a=1")),
                 "site.org/a/b?a=1&id=2")

  test("metadataIdentifiers and visibleText"):
    val page = """<html><head><META CONTENT="10.1590/ABC" NAME="citation_doi">
                 |<link rel="canonical" href="/j/acb/a/XYZ/"></head>
                 |<body><script>var a = 1;</script><p>Texto &amp; resumo</p><!-- c --></body></html>""".stripMargin
    assertEquals(UrlIdentity.metadataIdentifiers(page, URI.create("https://www.scielo.br/x")),
      Map("doi" -> "10.1590/abc", "canonical" -> "scielo.br/j/acb/a/XYZ", "scielo_aid" -> "XYZ"))
    assertEquals(UrlIdentity.visibleText(page).split("\\s+").filter(_.nonEmpty).toSeq, Seq("Texto", "&", "resumo"))

  // Layers 2 to 4: network access.

  test("layer 2: different URLs redirected to the same address are the same document"):
    assertEquals(status(s"$base/old/x", s"$base/old/y"), yes)

  test("layer 3: the citation DOI decides despite random scripts and different text"):
    assertEquals(status(s"$base/meta/a", s"$base/meta/b"), yes)
    assertEquals(status(s"$base/meta/a", s"$base/meta/c"), no)

  test("layer 3: the canonical address decides, ignoring language and trailing slash"):
    assertEquals(status(s"$base/og/1", s"$base/og/2"), yes)
    assertEquals(status(s"$base/og/1", s"$base/og/3"), no)

  test("layer 4: the visible text decides when there is no metadata"):
    assertEquals(status(s"$base/text/a", s"$base/text/b"), yes)
    assertEquals(status(s"$base/text/a", s"$base/text/c"), no)

  test("layer 4: identical non-HTML bodies are the same document"):
    assertEquals(status(s"$base/pdf/a", s"$base/pdf/b"), yes)
    assertEquals(status(s"$base/pdf/a", s"$base/pdf/c"), undefined)

  test("5xx answers are retried"):
    assertEquals(status(s"$base/flaky", s"$base/text/a"), yes)
    assertEquals(flakyRequests.get(), 2)

  test("redirects to the home page and unavailable URLs are undefined"):
    // Records 15379 and 1455942: both URLs ended at the SciELO home page.
    assertEquals(status(s"$base/home-redirect/1", s"$base/home-redirect/2"), undefined)
    assertEquals(status(s"$base/", s"$base/"), undefined)
    assertEquals(status(s"$base/missing", s"$base/missing"), undefined)
    assertEquals(status("not a url", "not a url"), undefined)

  test("any matching occurrence is enough"):
    assertEquals(status(s"$base/missing//@//$base/meta/a", s"$base/meta/b"), yes)

  test("empty values and missing field are undefined; isUrlContentSimilar only accepts yes"):
    assertEquals(status("", s"$base/text/a"), undefined)
    assertEquals(status(s"$base/text/a", " "), undefined)
    assertEquals(Util.urlDocumentStatus(results(s"$base/text/a", s"$base/text/b"), "other"), undefined)
    assert(Util.isUrlContentSimilar(results(s"$base/text/a", s"$base/text/b"), field))
    assert(!Util.isUrlContentSimilar(results(s"$base/text/a", s"$base/text/c"), field))
