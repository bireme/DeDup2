package dd.tools

class LoggableParameterSuite extends munit.FunSuite:
  test("loggableParameter hides passwords and keeps the other values"):
    assertEquals(Tools.loggableParameter("mySqlPassword", "secret"), "***")
    assertEquals(Tools.loggableParameter("password", "secret"), "***")
    assertEquals(Tools.loggableParameter("pswd", "secret"), "***")
    assertEquals(Tools.loggableParameter("mySqlUser", "user"), "user")
    assertEquals(Tools.loggableParameter("fieldSeparator", "|"), "|")

  /** Runs `body` with the dedup.credentials property pointing to a temporary file. */
  private def withCredentialsFile[T](content: String)(body: => T): T =
    val file = java.nio.file.Files.createTempFile("credenciais", ".txt")
    java.nio.file.Files.writeString(file, content)
    val previous = Option(System.getProperty("dedup.credentials"))
    System.setProperty("dedup.credentials", file.toString)
    try body
    finally
      previous.fold(System.clearProperty("dedup.credentials"))(System.setProperty("dedup.credentials", _))
      java.nio.file.Files.deleteIfExists(file)

  test("mySqlCredentials reads user and password from the credentials file"):
    withCredentialsFile("fiadmin.user=u1\nfiadmin.pswd=p\\\\1\nother.user=u2\nother.pswd=p2\n"):
      assertEquals(Tools.mySqlCredentials(Map.empty).get, ("u1", "p\\1"))
      assertEquals(Tools.mySqlCredentials(Map("credentials" -> "other")).get, ("u2", "p2"))

  test("mySqlCredentials gives precedence to -mySqlUser/-mySqlPassword"):
    withCredentialsFile("fiadmin.user=u1\nfiadmin.pswd=p1\n"):
      assertEquals(Tools.mySqlCredentials(Map("mySqlUser" -> "x", "mySqlPassword" -> "y")).get, ("x", "y"))
      assertEquals(Tools.mySqlCredentials(Map("mySqlUser" -> "x")).get, ("x", "p1"))

  test("mySqlCredentials fails for a missing profile without showing values"):
    withCredentialsFile("fiadmin.user=u1\nfiadmin.pswd=p1\n"):
      val error = Tools.mySqlCredentials(Map("credentials" -> "nope")).failed.get
      assert(error.getMessage.contains("nope.user"), error.getMessage)
      assert(!error.getMessage.contains("p1"))
