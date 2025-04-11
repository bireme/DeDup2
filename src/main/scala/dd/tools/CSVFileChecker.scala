package dd.tools

import scala.io.{BufferedSource, Codec, Source}
import scala.util.{Failure, Success, Try}

object CSVFileChecker extends App {
  private def usage(): Unit = {
    System.err.println("usage: CSVFileChecker <options>")
    System.err.println("options:")
    System.err.println("\t-csvFile=<path>          Path to the comma separated value file (csv)")
    System.err.println("\t-numberOfFields=<num>    Number of fields in each line of the csv file")
    System.err.println("\t[-fieldSeparator=<char>] Character indication the field separator. Default value is ','.")
    System.err.println("\t[-encoding=<str>]        The csv character encoding. Default value is 'utf-8'")
    System.err.println("\t[-reportFile=<path>]     Path to an output file having the check results. Default behaviour is to print the results to the standard output")
    System.err.println("\t[-stopAfter=<num>]       Stop checking after <num> errors were found. Defeault behaviour is to check until the end of the input file.")
    System.exit(1)
  }

  private val parameters: Map[String, String] = args.foldLeft[Map[String, String]](Map()) {
    case (map, par) =>
      val split: Array[String] = par.split(" *= *", 2)
      if (split.size == 1) map + ((split(0).substring(2), ""))
      else map + ((split(0).substring(1), split(1)))
  }

  println("Parameters:")
  parameters.foreach(param => println(s"\t${param._1}=${param._2}"))
  println()

  if (!Set("csvFile", "numberOfFields").forall(parameters.contains)) usage()

  processFile(parameters("csvFile"), parameters("numberOfFields").toInt, parameters.getOrElse("fieldSeparator", ",").trim.head,
              parameters.getOrElse("encoding", "utf-8"), parameters.getOrElse("stopAfter", Integer.MAX_VALUE.toString).toInt,
              parameters.get("reportFile")) match {
    case Success(_) =>
      println("\nChecking process finished successfully!")
      System.exit(0)
    case Failure(exception: Exception) =>
      System.err.println(s"Checking error: ${exception.toString}")
      System.exit(1)
  }

  def processFile(csvFile: String,
                  numberOfFields: Int,
                  fieldSeparator: Char,
                  encoding: String,
                  stopAfter: Int,
                  reportFile: Option[String]): Try[Unit] = {
    Try {
      require(numberOfFields > 0, "numberOfFields parameter should be a positive number")
      require(stopAfter > 0, "stopAfter parameter should be a positive number")

      val source: BufferedSource = Source.fromFile(csvFile)(Codec.string2codec(encoding))
      val lines: Iterator[String] = source.getLines()
      val separator: String = fieldSeparator match {
        case '|' => "\\|"
        case x => x.toString
      }
      //val report =
      var curLine: Int = 1

      while (hasNext(lines,curLine)) {
        if (curLine % 10000 == 0) println(s"+++$curLine")

        Try(lines.next()) match {
          case Success(line) =>
            if (line.trim.nonEmpty) {
              val split: Array[String] = line.split(separator)
              if (split.length != numberOfFields)
                println(s"Invalid number of fields. Line[$curLine]=$line Fields:${split.length}")
            }
          case Failure(exception) =>
            println(s"Possible character encoding error. Line[$curLine] Error:${exception.toString}")
        }
        curLine += 1
      }
      source.close()
    }
  }

  private def hasNext(lines: Iterator[String],
                      curLine: Int): Boolean = {
    Try(lines.hasNext) match {
      case Success(_) => true
      case Failure(exception) =>
        println(s"Stopping the checking process! Possible character encoding error. Line[$curLine] Error:${exception.toString}")
        throw exception
    }
  }
}