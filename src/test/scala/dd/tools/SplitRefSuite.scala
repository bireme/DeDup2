package dd.tools

import java.nio.charset.StandardCharsets
import java.nio.file.Files

class SplitRefSuite extends munit.FunSuite:
  test("parseRef handles journal followed only by comma and year"):
    assertEquals(
      SplitRef.parseRef("Braz. j. biol, 2017."),
      Right(PeriodicalRef(
        journal = "Braz. j. biol",
        volume = None,
        issue = None,
        startPage = None,
        endPage = None,
        month = None,
        year = Some(2017),
        notes = Nil
      ))
    )

  test("run expands configured CSV fields into PeriodicalRef values"):
    val input = Files.createTempFile("split-ref-input", ".csv")
    val output = Files.createTempFile("split-ref-output", ".csv")
    Files.writeString(
      input,
      "1|Rev Saude Publica; 12(3): 45-50, Jan 2024 ilus tab|ok\n",
      StandardCharsets.UTF_8
    )

    val result = SplitRef.run(Array(
      s"-csvFile=$input",
      "-fieldPositions=1",
      s"-outCsvFile=$output",
      "-fieldSeparator=|"
    ))

    assert(result.isSuccess)
    assertEquals(
      Files.readString(output, StandardCharsets.UTF_8),
      "1|Rev Saude Publica|12|3|45|50|Jan|2024|ilus tab|ok\n"
    )

  test("run uses pipe as default field separator"):
    val input = Files.createTempFile("split-ref-default-separator-input", ".csv")
    val output = Files.createTempFile("split-ref-default-separator-output", ".csv")
    Files.writeString(input, "Rev Saude Publica; 12(3): 45-50, Jan 2024\n", StandardCharsets.UTF_8)

    val result = SplitRef.run(Array(
      s"-csvFile=$input",
      "-fieldPositions=0",
      s"-outCsvFile=$output"
    ))

    assert(result.isSuccess)
    assertEquals(
      Files.readString(output, StandardCharsets.UTF_8),
      "Rev Saude Publica|12|3|45|50|Jan|2024|\n"
    )

  test("run skips invalid records and continues with following records"):
    val input = Files.createTempFile("split-ref-invalid-input", ".csv")
    val output = Files.createTempFile("split-ref-invalid-output", ".csv")
    Files.writeString(
      input,
      "1|Rev Saude Publica; 12(3): 45-50, Jan 2024|ok\n" +
        "2|invalid reference|bad\n" +
        "3|Cad Saude Coletiva; 7: 10-12, Feb 2020|ok\n",
      StandardCharsets.UTF_8
    )

    val result = SplitRef.run(Array(
      s"-csvFile=$input",
      "-fieldPositions=1",
      s"-outCsvFile=$output"
    ))

    assert(result.isSuccess)
    assertEquals(
      Files.readString(output, StandardCharsets.UTF_8),
      "1|Rev Saude Publica|12|3|45|50|Jan|2024||ok\n" +
        "3|Cad Saude Coletiva|7||10|12|Feb|2020||ok\n"
    )

  test("run treats quotes as ordinary characters while parsing fields"):
    val input = Files.createTempFile("split-ref-quotes-input", ".csv")
    val output = Files.createTempFile("split-ref-quotes-output", ".csv")
    Files.writeString(
      input,
      "1|\"Where there's a woman\": title|Rev Saude Publica; 12(3): 45-50, Jan 2024\n",
      StandardCharsets.UTF_8
    )

    val result = SplitRef.run(Array(
      s"-csvFile=$input",
      "-fieldPositions=2",
      s"-outCsvFile=$output"
    ))

    assert(result.isSuccess)
    assertEquals(
      Files.readString(output, StandardCharsets.UTF_8),
      "1|\"Where there's a woman\": title|Rev Saude Publica|12|3|45|50|Jan|2024|\n"
    )
