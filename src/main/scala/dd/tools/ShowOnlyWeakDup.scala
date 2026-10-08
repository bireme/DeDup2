package dd.tools

import scala.collection.mutable
import scala.io.Source
import scala.util.{Failure, Success, Try, Using}

/** Filters weak duplicate rows that are already represented in a strong duplicate file. */
object ShowOnlyWeakDup:
  /**
   * Runs the weak-duplicate filter from the command line.
   *
   * @param args strong input path, weak input path, and output path, in that order
   */
  def main(args: Array[String]): Unit =
    if args.length != 3 then
      System.err.println("Usage: ShowOnlyWeakDup <strongDupPath> <weakDupPath> <outWeakDupPath>")
      System.exit(1)
    else
      show(args(0), args(1), args(2)) match
        case Success(_) => ()
        case Failure(exception) =>
          System.err.println(s"ShowOnlyWeakDup failed: ${exception.getMessage}")
          System.exit(1)

  /**
   * Writes weak duplicate rows whose document pair does not occur in the strong duplicate file.
   *
   * Both input files are pipe-delimited. The pair key is built from zero-based
   * columns 2 and 3, with the values ordered lexicographically so reversed pairs
   * produce the same key. Each input row must contain at least four columns.
   *
   * @param strongDupPath path to the strong duplicates input file
   * @param weakDupPath path to the weak duplicates input file
   * @param outWeakDupPath path to the filtered weak duplicates output file
   * @return successful completion, or a failure if a file cannot be read/written
   *         or an input row has fewer than four columns
   */
  def show(strongDupPath: String,
           weakDupPath: String,
           outWeakDupPath: String): Try[Unit] =
    Try:
      val strongFile: java.nio.file.Path = java.nio.file.Paths.get(strongDupPath)
      val weakFile: java.nio.file.Path = java.nio.file.Paths.get(weakDupPath)
      require(java.nio.file.Files.isRegularFile(strongFile), s"Strong duplicates file not found: $strongDupPath")
      require(java.nio.file.Files.isRegularFile(weakFile), s"Weak duplicates file not found: $weakDupPath")

      val strongPairs: mutable.TreeSet[String] = mutable.TreeSet.empty[String]
      Using.resource(Source.fromFile(strongDupPath)) { source =>
        source.getLines().foreach { line =>
          strongPairs += pairKey(line)
        }
      }

      Using.resource(java.nio.file.Files.newBufferedWriter(java.nio.file.Paths.get(outWeakDupPath))) { writer =>
        Using.resource(Source.fromFile(weakDupPath)) { source =>
          source.getLines().foreach { line =>
            if !strongPairs.contains(pairKey(line)) then
              writer.write(line)
              writer.newLine()
          }
        }
      }

  /**
   * Builds an order-independent key from zero-based columns 2 and 3 of a row.
   *
   * @param line pipe-delimited input row
   * @return the two selected column values in lexicographical order, joined by an underscore
   * @throws IllegalArgumentException if the row contains fewer than four columns
   */
  private def pairKey(line: String): String =
    val columns: Array[String] = line.split("\\|", -1)
    require(columns.length >= 4, s"Expected at least four pipe-delimited fields: $line")
    val first: String = columns(2)
    val second: String = columns(3)
    if first <= second then s"${first}_${second}" else s"${second}_${first}"
