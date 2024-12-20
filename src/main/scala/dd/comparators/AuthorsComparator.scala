package dd.comparators

import com.github.vickumar1981.stringdistance.StringDistance.DiceCoefficient
import dd.interfaces.{CompResult, Comparator, Document}
import org.bireme.covid.SplitAuthors

import java.text.Normalizer
import java.text.Normalizer.Form
import scala.io.{BufferedSource, Source}

class AuthorsComparator(fieldName: String) extends Comparator {
  private val occSeparator: String = ";"

  override def compare(originalDoc: Document,
                       currentDoc: Document): CompResult = {
    val (rawOriSeq: Seq[String], oriSeq: Seq[String]) = getAuthors(fieldName, originalDoc)
    val (rawCurSeq: Seq[String], curSeq: Seq[String]) = getAuthors(fieldName, currentDoc)

    if (rawOriSeq.isEmpty && rawCurSeq.isEmpty)
      CompResult("AuthorsComparator", fieldName, rawOriSeq.mkString(occSeparator), rawCurSeq.mkString(occSeparator),
      None, None, 1, isSimilar = true)
    else if ((rawOriSeq.isEmpty && rawCurSeq.nonEmpty) || (rawOriSeq.nonEmpty && rawCurSeq.isEmpty))
      CompResult("AuthorsComparator", fieldName, rawOriSeq.mkString(occSeparator), rawCurSeq.mkString(occSeparator),
        None, None, 0, isSimilar = false)
    else {
      val (fromSeq, toSeq) = if (oriSeq.length <= curSeq.length) (oriSeq, curSeq) else (curSeq, oriSeq)

      if (fromSeq.length < toSeq.length * 0.8)
        CompResult("AuthorsComparator", fieldName, rawOriSeq.mkString(occSeparator), rawCurSeq.mkString(occSeparator),
          None, None, 0, isSimilar = false)
      else {
        //val isSimilar: Boolean = fromSeq.forall(existSimilar(_, toSeq))
        val isSimilar: Boolean = fromSeq.forall {
          au =>
            val sim: Boolean = existSimilar(au, toSeq)
            sim
        }
        CompResult("AuthorsComparator", fieldName, rawOriSeq.mkString(occSeparator), rawCurSeq.mkString(occSeparator),
          None, None, if (isSimilar) 1 else 0, isSimilar)
      }
    }
  }

  private def getAuthors(fieldName: String,
                         document: Document,
                         fixOccSeparator: Boolean = true): (Seq[String], Seq[String]) = {
    val originalAuthors: Seq[String] = document.fields.filter(_._1.equals(fieldName)).map(_._2)

    val rawAuthors: Seq[String] =
      if (fixOccSeparator) originalAuthors.flatMap(au => SplitAuthors.getAuthors(au, ",", occSeparator))
      else originalAuthors.flatMap(_.split(" *" + occSeparator + " *"))

    val normalizedAuthors: Seq[String] = rawAuthors.map {
      author =>
        val trimName: String = author.replaceAll(" {2,}", " ").trim
        val invertedName: Array[String] = trimName.split(" *, *", 2)
        val orderedName: String = (if (invertedName.length == 2) invertedName.reverse else invertedName).mkString(" ")
        val normName: String = {
          val s1: String = Normalizer.normalize(orderedName.toLowerCase(), Form.NFD)
          val s2: String = s1.replaceAll("[\\p{InCombiningDiacriticalMarks}]", "")
          val s3: String = s2.replace(".", "")
          s3
        }
        normName
    }
    (rawAuthors, normalizedAuthors)
  }

  private def existSimilar(author: String,
                           others: Seq[String]): Boolean = {//others.exists(isSimilar(_, author))
    others.exists {
      au =>
        val sim = isSimilar(au, author)
        //println(s"au=$au author=$author sim=$sim\n")
        sim
    }
  }

  private def isSimilar(author1: String,
                        author2: String): Boolean = {
    val SYM1: Double = 0.4
    val SYM2: Double = 0.5

    lazy val aut1: Array[String] = author1.split(" ")
    lazy val aut2: Array[String] = author2.split(" ")

    val cond1 = author1.head == author2.head
    val cond2 = (aut1.length == 2) && (aut2.length == 2)
    val s1 = getSimilarity(author1, author2)
    val cond3 = getSimilarity(author1, author2) >= SYM1
    val cond4 = cond2 && cond3
    val cond5 = getSimilarity(author1, author2) >= SYM2
    //val cond6 = cond1 && (cond4 || cond5)
    println(s"sim=$s1 au1=$author1 au2=$author2")

    (author1.isEmpty && author2.isEmpty) ||
      (((author1.nonEmpty && author2.nonEmpty) &&
       (author1.head == author2.head) &&
        ((aut1.length == 2) && (aut2.length == 2) && (getSimilarity(author1, author2) >= SYM1)) ||
      getSimilarity(author1, author2) >= SYM2))

    /*val cond1 = (author1.head == author2.head)
    val cond2 = ((aut1.length == 2) && (aut2.length == 2))
    val s1 = getSimilarity(author1, author2)
    val cond3 = getSimilarity(author1, author2) >= SYM1
    val cond4 = cond2 && cond3
    val cond5 = getSimilarity(author1, author2) >= SYM2
    val cond6 = cond1 && (cond4 || cond5)

    cond6*/

  }

  private def getSimilarity(str1: String,
                            str2: String): Double = {
    if ((str1.isEmpty && str2.nonEmpty) || (str1.nonEmpty && str2.isEmpty)) 0
    else DiceCoefficient.score(str1, str2)
  }
}

object AuthorsComparator extends App {
  private def usage(): Unit = {
    Console.err.println("Identify if two lists of authors are similar. The authors are separated by ';'" +
      "\nusage: AuthorsComparator <authorsFileName>" +
      "\nwhere the each line of the file has the following pattern: <author1>;<author2>;...|<authorx1>;<authorx2>;...")
    System.exit(1)
  }

  if (args.length < 1) usage()

  private val source: BufferedSource = Source.fromFile(args(0))
  private val lines: Iterator[String] = source.getLines()

  lines.foreach {
    line =>
      val lineT = line.trim
      if (lineT.nonEmpty && lineT.head != '#') {
        val split = lineT.split("\\|", 2)

        if (split.length == 2) {
          val originalDoc: Document = Document(Seq(("authors", split(0))))
          val currentDoc: Document = Document(Seq(("authors", split(1))))
          val au = new AuthorsComparator("authors")
          val result: CompResult = au.compare(originalDoc, currentDoc)

          if (!result.isSimilar) {
            /*println(s"Authors1=${result.originalField}")
            println(s"Authors2=${result.currentField}")
            println(s"Authors are${if (result.isSimilar) "" else " NOT"} similar.")
            println()*/
            println(result.originalField)
            println(result.currentField)
            println()
          }
        } else println(s"Invalid line format: $lineT")
      }
  }
  source.close()
}