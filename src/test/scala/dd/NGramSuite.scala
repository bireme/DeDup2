package dd

import _root_.dd.tools.NGram

class NGramSuite extends munit.FunSuite:
  test("score uses bigrams by default"):
    assertEquals(NGram.score("abcd", "abxd"), 1.0 / 3.0)

  test("score returns normalized similarity for equal strings"):
    assertEquals(NGram.score("abcd", "abcd"), 1.0)
