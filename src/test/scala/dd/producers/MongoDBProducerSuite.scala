package dd.producers

import org.bson.{Document as BsonDocument}

import java.util.Arrays

class MongoDBProducerSuite extends munit.FunSuite:
  test("toDocument converts top-level BSON fields to internal document fields"):
    val bson = new BsonDocument()
      .append("database", "LILACS")
      .append("id", 123)
      .append("title", "Sample title")

    val document = MongoDBProducer.toDocument(bson, None).get

    assertEquals(
      document.fields,
      Seq(
        "database" -> "LILACS",
        "id" -> "123",
        "title" -> "Sample title"
      )
    )

  test("toDocument joins arrays with the default field separator and preserves nested documents as JSON"):
    val nested = new BsonDocument().append("city", "Sao Paulo")
    val bson = new BsonDocument()
      .append("author", Arrays.asList("Ana", "Joao"))
      .append("address", nested)

    val document = MongoDBProducer.toDocument(bson, None).get

    assertEquals(document.fields.head, "author" -> "Ana¦Joao")
    assertEquals(BsonDocument.parse(document.fields(1)._2).getString("city"), "Sao Paulo")

  test("toDocument honors mapped fields, nested paths, and emits empty values for missing fields"):
    val bson = new BsonDocument()
      .append("title", "Sample title")
      .append("database", "LILACS")
      .append("metadata", new BsonDocument().append("id", "123"))

    val fields = Seq(
      MongoDBProducer.parseFieldMapping("database=database").get,
      MongoDBProducer.parseFieldMapping("id=metadata.id").get,
      MongoDBProducer.parseFieldMapping("missing=metadata.missing").get,
      MongoDBProducer.parseFieldMapping("title=title").get
    )
    val document = MongoDBProducer.toDocument(bson, Some(fields)).get

    assertEquals(
      document.fields,
      Seq(
        "database" -> "LILACS",
        "id" -> "123",
        "missing" -> "",
        "title" -> "Sample title"
      )
    )

  test("toDocument extracts subfields from arrays of documents and joins them"):
    val bson = new BsonDocument()
      .append("authors", Arrays.asList(
        new BsonDocument().append("name", "Ana"),
        new BsonDocument().append("name", "Joao")
      ))

    val fields = Some(Seq(MongoDBProducer.parseFieldMapping("author.name=authors.name").get))
    val document = MongoDBProducer.toDocument(bson, fields).get

    assertEquals(document.fields, Seq("author.name" -> "Ana¦Joao"))

  test("parseFieldMapping requires output field and MongoDB path"):
    val valid = MongoDBProducer.parseFieldMapping("title=metadata.title").get
    val invalid = MongoDBProducer.parseFieldMapping("metadata.title")

    assertEquals(valid, MongoDBProducer.FieldMapping("title", Seq("metadata", "title")))
    assert(invalid.isFailure)

  test("mongoUri defaults to localhost and accepts host with embedded port"):
    val defaultUri = MongoDBProducer.mongoUri(MongoDBProducerConfig("dedup", "docs"))
    val embeddedPortUri = MongoDBProducer.mongoUri(
      MongoDBProducerConfig("dedup", "docs", host = Some("mongo.example.org:27018"))
    )
    val authenticatedUri = MongoDBProducer.mongoUri(
      MongoDBProducerConfig("dedup", "docs", host = Some("mongo.example.org"), port = Some(27019), user = Some("usr"), password = Some("psw"))
    )

    assertEquals(defaultUri, "mongodb://localhost:27017")
    assertEquals(embeddedPortUri, "mongodb://mongo.example.org:27018")
    assertEquals(authenticatedUri, "mongodb://usr:psw@mongo.example.org:27019")

  test("MongoDBProducerConfig keeps server cursor timeout disabled by default"):
    assertEquals(MongoDBProducerConfig("dedup", "docs").noCursorTimeout, true)
    assertEquals(MongoDBProducerConfig("dedup", "docs", noCursorTimeout = false).noCursorTimeout, false)
