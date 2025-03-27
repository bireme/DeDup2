package dd.tools

import dd.interfaces.{DocsProducer, Document}
import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.document
import org.apache.lucene.document.{Field, StoredField, TextField}
import org.apache.lucene.index.{IndexWriter, IndexWriterConfig}
import org.apache.lucene.store.{Directory, FSDirectory}

import java.io.File
import java.nio.file.Path
import java.text.Normalizer
import java.text.Normalizer.Form
import scala.jdk.CollectionConverters.IteratorHasAsScala
import scala.util.Try

object Tools {
  def normalizeStr(in: String): String = Normalizer.normalize(in.trim().toLowerCase, Form.NFD)
    .replaceAll("[^a-z0-9]", "")

  def createLuceneIndex(producer: DocsProducer,
                        indexPath: String,
                        fieldToIndex: String,
                        analyzer: Analyzer): Try[Unit] = {
    Try {
      val iPath: Path = new File(indexPath).toPath
      val directory: Directory = FSDirectory.open(iPath)
      val config: IndexWriterConfig = new IndexWriterConfig(analyzer).setOpenMode(IndexWriterConfig.OpenMode.CREATE)
      val iwriter: IndexWriter = new IndexWriter(directory, config)
      var tell: Int = 0

      producer.getDocuments.foreach {
        dc =>
          if (tell % 1000 == 0) println(s"+++$tell")
          tell += 1

          val lucDoc: document.Document = dc.fields.foldLeft(new org.apache.lucene.document.Document()) {
            case (ldoc, (fldName, fldValue)) =>
              if (fldName.equals(fieldToIndex)) {
                if (fldValue.trim.isEmpty) Console.err.println("Error indexing document field is empty")
                else ldoc.add(new TextField(fldName, fldValue, Field.Store.YES))
              } else ldoc.add(new StoredField(fldName, fldValue))
              ldoc
          }
          iwriter.addDocument(lucDoc)
      }
      iwriter.forceMerge(1)
      iwriter.close()
      directory.close()
    }
  }

  def doc2doc(ldoc: org.apache.lucene.document.Document): Document = {
    val fields: Seq[(String, String)] = ldoc.iterator.asScala.foldLeft(Seq[(String, String)]()) {
      case (seq, field) => seq :+ (field.name() -> field.stringValue())
    }
    Document(fields)
  }
}
