/*
 * Copyright (2025) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.delta.kernel.utils
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers.contain
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import java.util

class CloseableIteratorSuite extends AnyFunSuite {

  test("filter with all elements matching filter") {
    val res = new util.ArrayList[Int]();
    new IntArrayIterator(List(1, 2, 3, 2, 2))
      .filter((input: Int) => input > 0)
      .forEachRemaining((input: Int) => res.add(input))
    res should contain theSameElementsAs List(1, 2, 3, 2, 2)

  }

  test("filter with no elements matching filter") {
    val res = new util.ArrayList[Int]();
    new IntArrayIterator(List(1, 2, 3, 2, 2))
      .filter((input: Int) => input < 0)
      .forEachRemaining((input: Int) => res.add(input))
    assert(res.isEmpty)
  }

  test("filter with some elements matching filter") {
    val res = new util.ArrayList[Int]();
    new IntArrayIterator(List(1, 2, 3, 2, 2))
      .filter((input: Int) => input < 3)
      .forEachRemaining((input: Int) => res.add(input))
    res should contain theSameElementsAs List(1, 2, 2, 2)
  }

  test("takeWhile with all elements matching filter") {
    val res = new util.ArrayList[Int]();
    new IntArrayIterator(List(1, 2, 3, 2, 2))
      .takeWhile((input: Int) => input > 0)
      .forEachRemaining((input: Int) => res.add(input))
    res should contain theSameElementsAs List(1, 2, 3, 2, 2)
  }

  test("takeWhile with no elements matching filter") {
    val res = new util.ArrayList[Int]();
    val iterator = new IntArrayIterator(List(1, 2, 3, 2, 2))
    iterator
      .takeWhile((input: Int) => input < 0)
      .forEachRemaining((input: Int) => res.add(input))
    assert(res.isEmpty)
    iterator.visited() should contain theSameElementsAs List(1)
  }

  test("takeWhile with some elements matching filter") {
    val res = new util.ArrayList[Int]();
    val iterator = new IntArrayIterator(List(1, 2, 3, 2, 2))
    iterator
      .takeWhile((input: Int) => input < 3)
      .forEachRemaining((input: Int) => res.add(input))
    res should contain theSameElementsAs List(1, 2)
    iterator.visited() should contain theSameElementsAs List(1, 2, 3)
  }

  test("custom breakable filter with all elements matching filter") {
    val res = new util.ArrayList[Int]();
    val iterator = new IntArrayIterator(List(1, 2, 3, 2, 2))
    iterator
      .breakableFilter((input: Int) => {
        if (input < 2) {
          BreakableFilterResult.INCLUDE
        } else if (input == 2) {
          BreakableFilterResult.EXCLUDE
        } else {
          BreakableFilterResult.BREAK
        }
      })
      .forEachRemaining((input: Int) => res.add(input))
    res should contain theSameElementsAs List(1)
    iterator.visited() should contain theSameElementsAs List(1, 2, 3)
  }

}

class IntArrayIterator(val intArray: List[Int]) extends CloseableIterator[Int] {
  private val data: List[Int] = intArray
  private var curIdx = 0
  private val visitedArray: util.ArrayList[Int] = new util.ArrayList[Int]()
  override def hasNext: Boolean = {
    data.size > curIdx
  }

  override def next(): Int = {
    val res = data(curIdx)
    visitedArray.add(res)
    curIdx = curIdx + 1
    res
  }
  override def close(): Unit = {}

  def visited(): util.ArrayList[Int] = visitedArray
}
