/**
 * (c) Copyright 2013 WibiData, Inc.
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kiji.chopsticks

import java.{util, lang}

import org.scalatest.FunSuite
import runtime.ScalaRunTime
import org.kiji.schema.avro.{HashSpec, MD5Hash, RowKeyEncoding}
import org.kiji.schema.filter.RegexQualifierColumnFilter
import org.kiji.chopsticks.ColumnRequest.InputOptions
import java.io.InvalidClassException
import org.kiji.schema.layout.{KijiTableLayout, KijiTableLayouts}
import org.kiji.schema.KijiColumnName

class ReturnTypeCheckerSuite extends FunSuite {
  val intHashMap = new util.TreeMap[java.lang.Long, java.lang.Integer](){
    put(new java.lang.Long(10), new Integer(10))
    put(new java.lang.Long(20), new Integer(20))
  }

  val boolHashMap = new util.TreeMap[java.lang.Long, java.lang.Boolean](){
    put(new java.lang.Long(10), new java.lang.Boolean(true))
    put(new java.lang.Long(20), new java.lang.Boolean(false))
  }

  val longHashMap = new util.TreeMap[java.lang.Long, java.lang.Long](){
    put(new java.lang.Long(10), new lang.Long(10))
  }

  val floatHashMap = new util.TreeMap[java.lang.Long, java.lang.Float](){
    put(new java.lang.Long(10), new lang.Float(10))
  }

  val doubleHashMap = new util.TreeMap[java.lang.Long, java.lang.Double](){
    put(new java.lang.Long(10), new lang.Double(10))
    put(new java.lang.Long(20), new lang.Double(20))
    put(new java.lang.Long(30), new lang.Double(25))
  }

  val bytesHashMap = new util.TreeMap[java.lang.Long, java.nio.ByteBuffer](){
    put(new java.lang.Long(10), java.nio.ByteBuffer.wrap(Array(0x11, 0x12)))
  }

  val charSequenceHashMap = new util.TreeMap[java.lang.Long, java.lang.CharSequence](){
    put(new java.lang.Long(10), "test")
    put(new java.lang.Long(20), "string")
  }

  val listHashMap = new util.TreeMap[java.lang.Long, java.util.List[java.lang.Integer]](){
    put(new java.lang.Long(10), new util.ArrayList[lang.Integer](){
      add(1)
      add(2)})
    put(new java.lang.Long(20), new util.ArrayList[lang.Integer](){
      add(3)
      add(4)})
  }

  val mapHashMap = new util.TreeMap[java.lang.Long, java.util.Map[java.lang.Integer, java.lang.String]](){
    put(new java.lang.Long(10), new java.util.HashMap[java.lang.Integer, java.lang.String](){
      put(1, "t1")
      put(2, "t2")
    })
    put(new java.lang.Long(20), new java.util.HashMap[java.lang.Integer, java.lang.String](){
      put(3, "t3")
      put(4, "t4")
    })
  }

  val voidHashMap = new util.TreeMap[java.lang.Long, java.lang.Void](){
    put(new java.lang.Long(10), null)
  }

  val unionHashMap = new util.TreeMap[java.lang.Long, java.lang.Object](){
    put(new java.lang.Long(10), new java.lang.Integer(10))
    put(new java.lang.Long(20), "test")
  }

  val enumHashMap = new util.TreeMap[java.lang.Long, RowKeyEncoding](){
    put(new java.lang.Long(10), RowKeyEncoding.FORMATTED)
    put(new java.lang.Long(20), RowKeyEncoding.HASH_PREFIX)
  }

  val fixedHashMap = new util.TreeMap[java.lang.Long, org.kiji.schema.avro.MD5Hash](){
    put(new java.lang.Long(10), new org.kiji.schema.avro.MD5Hash(Array[Byte](01, 02)))
    put(new java.lang.Long(20), new org.kiji.schema.avro.MD5Hash(Array[Byte](03, 04)))
  }

  val recordHashMap = new util.TreeMap[java.lang.Long, org.kiji.schema.avro.HashSpec](){
    put(new java.lang.Long(10), HashSpec.newBuilder.setHashSize(2).build)
    put(new java.lang.Long(20), HashSpec.newBuilder().build())
  }

  val badRecordHashMap = new util.TreeMap[java.lang.Long, InputOptions](){
    val filter = new RegexQualifierColumnFilter(".*")
    val maxVersions = 2
    val opts = new InputOptions(maxVersions, filter)
    put(new java.lang.Long(10), opts)
  }

    test("Test Return Type Int") {
    val res = KijiScheme.convertKijiValuesToScalaTypes(intHashMap)
    assert(2 == res.size)
    res.foreach(kv => {
      assert(kv._2.isInstanceOf[Int])
    })
    assert(10 == res(10L))
    assert(20 == res(20L))

    // convert back
    val resJava = KijiScheme.convertScalaTypesToKijiValues(res, null, null)
    assert(resJava.isInstanceOf[util.NavigableMap[lang.Long, java.lang.Object]])
    assert(resJava == intHashMap)
  }

  test("Test Return Type Boolean") {
    val res = KijiScheme.convertKijiValuesToScalaTypes(boolHashMap)
    assert(2 == res.size)
    res.foreach(kv => assert(kv._2.isInstanceOf[Boolean]))
    assert(true == res(10L))
    assert(false == res(20L))
    // convert back
    val resJava = KijiScheme.convertScalaTypesToKijiValues(res, null, null)
    assert(resJava.isInstanceOf[util.NavigableMap[lang.Long, java.lang.Object]])
    assert(resJava == boolHashMap)
  }

  test("Test Return Type Long") {
    val res = KijiScheme.convertKijiValuesToScalaTypes(longHashMap)
    assert(1 == res.size)
    res.foreach(kv => assert(kv._2.isInstanceOf[Long]))
    assert(10L == res(10L))
    // convert back
    val resJava = KijiScheme.convertScalaTypesToKijiValues(res, null, null)
    assert(resJava.isInstanceOf[util.NavigableMap[lang.Long, java.lang.Object]])
    assert(resJava == longHashMap)
  }

  test("Test Return Type Float") {
    val res = KijiScheme.convertKijiValuesToScalaTypes(floatHashMap)
    assert(1 == res.size)
    res.foreach(kv => assert(kv._2.isInstanceOf[Float]))
    assert(10F == res(10L))
    // convert back
    val resJava = KijiScheme.convertScalaTypesToKijiValues(res, null, null)
    assert(resJava.isInstanceOf[util.NavigableMap[lang.Long, java.lang.Object]])
    assert(resJava == floatHashMap)
  }

  test("Test Return Type Double") {
    val res = KijiScheme.convertKijiValuesToScalaTypes(doubleHashMap)
    assert(3 == res.size)
    res.foreach(kv => assert(kv._2.isInstanceOf[Double]))
    assert(10D == res(10L))
    assert(20D == res(20L))
    assert(25D == res(30L))
    // convert back
    val resJava = KijiScheme.convertScalaTypesToKijiValues(res, null, null)
    assert(resJava.isInstanceOf[util.NavigableMap[lang.Long, java.lang.Object]])
    assert(resJava == doubleHashMap)
  }

  test("Test Return Type Bytes") {
    val res = KijiScheme.convertKijiValuesToScalaTypes(bytesHashMap)
    assert(1 == res.size)
    res.foreach(kv => assert(kv._2.isInstanceOf[Array[Byte]]))
    val expected: Array[Byte] = Array(17, 18)
    assert(expected.deep == res(10L).asInstanceOf[Array[Byte]].deep)

    // convert back
    val tableLayout = KijiTableLayout.newLayout(KijiTableLayouts.getLayout("avro-types.json"))
    val columnName = new KijiColumnName("family", "column2")
    val resJava = KijiScheme.convertScalaTypesToKijiValues(res, tableLayout, columnName)
    assert(resJava.isInstanceOf[util.NavigableMap[lang.Long, java.lang.Object]])
    assert(resJava == bytesHashMap)

  }

  test("Test Return Type CharSequence") {
    val res = KijiScheme.convertKijiValuesToScalaTypes(charSequenceHashMap)
    assert(2 == res.size)
    res.foreach(kv => assert(kv._2.isInstanceOf[String]))
    assert("test" == res(10L))
    assert("string" == res(20L))

    // convert back
    val resJava = KijiScheme.convertScalaTypesToKijiValues(res, null, null)
    assert(resJava.isInstanceOf[util.NavigableMap[lang.Long, java.lang.Object]])
    assert(resJava == charSequenceHashMap)
  }

  test("Test Return Type List") {
    val res = KijiScheme.convertKijiValuesToScalaTypes(listHashMap)
    assert(2 == res.size)
    res.foreach(kv => assert(kv._2.isInstanceOf[List[Int]]))
    val expect1: List[Int] = List(1, 2)
    val expect2: List[Int] = List(3, 4)
    assert(expect1 == res(10L))
    assert(expect2 == res(20L))
    // convert back
    val resJava = KijiScheme.convertScalaTypesToKijiValues(res, null, null)
    assert(resJava.isInstanceOf[util.NavigableMap[lang.Long, java.lang.Object]])
    assert(resJava == listHashMap)
  }

  test("Test Return Type Map") {
    val res = KijiScheme.convertKijiValuesToScalaTypes(mapHashMap)
    assert(2 == res.size)
    res.foreach(kv => assert(kv._2.isInstanceOf[Map[Int, String]]))
    val expect1: Map[Int, String] = Map(1->"t1", 2-> "t2")
    val expect2: Map[Int, String] = Map(3->"t3", 4-> "t4")
    assert(expect1 == res(10L))
    assert(expect2 == res(20L))
    // convert back
    val resJava = KijiScheme.convertScalaTypesToKijiValues(res, null, null)
    assert(resJava.isInstanceOf[util.NavigableMap[lang.Long, java.lang.Object]])
    assert(resJava == mapHashMap)
  }

  test("Test Return Type Null") {
    val res = KijiScheme.convertKijiValuesToScalaTypes(voidHashMap)
    assert(1 == res.size)
    res.foreach(kv => assert(null == kv._2))
    // convert back
    val resJava = KijiScheme.convertScalaTypesToKijiValues(res, null, null)
    assert(resJava.isInstanceOf[util.NavigableMap[lang.Long, java.lang.Object]])
    assert(resJava == voidHashMap)
  }

  test("Test Return Type Union") {
    val res = KijiScheme.convertKijiValuesToScalaTypes(unionHashMap)
    assert(2 == res.size)
    res.foreach(kv => assert(kv._2.isInstanceOf[java.lang.Object]))
    assert(10 == res(10L))
    assert("test" == res(20L))

    // convert back
    val resJava = KijiScheme.convertScalaTypesToKijiValues(res, null, null)
    assert(resJava.isInstanceOf[util.NavigableMap[lang.Long, java.lang.Object]])
    assert(resJava == unionHashMap)
  }

  test("Test Return Type Enum") {
    val res = KijiScheme.convertKijiValuesToScalaTypes(enumHashMap)
    assert(2 == res.size)
    res.foreach(kv => assert(kv._2.isInstanceOf[RowKeyEncoding]))
    assert(RowKeyEncoding.FORMATTED == res(10L))
    assert(RowKeyEncoding.HASH_PREFIX == res(20L))

    // convert back
    val resJava = KijiScheme.convertScalaTypesToKijiValues(res, null, null)
    assert(resJava.isInstanceOf[util.NavigableMap[lang.Long, java.lang.Object]])
    assert(resJava == enumHashMap)
  }

  test("Test Return Type Fixed") {
    val res = KijiScheme.convertKijiValuesToScalaTypes(fixedHashMap)
    assert(2 == res.size)
    res.foreach(kv => assert(kv._2.isInstanceOf[Array[Byte]]))
    assert(Array[Byte](01, 02).deep == res(10L).asInstanceOf[Array[Byte]].deep)
    assert(Array[Byte](03, 04).deep == res(20L).asInstanceOf[Array[Byte]].deep)

    // convert back
    val tableLayout = KijiTableLayout.newLayout(KijiTableLayouts.getLayout("avro-types.json"))
    val columnName = new KijiColumnName("family", "column1")
    val resJava = KijiScheme.convertScalaTypesToKijiValues(res, tableLayout, columnName)
    assert(resJava.isInstanceOf[util.NavigableMap[lang.Long, java.lang.Object]])
    assert(resJava == fixedHashMap)
  }

  test("Test Return Type Avro Record") {
    val res = KijiScheme.convertKijiValuesToScalaTypes(recordHashMap)
    assert(2 == res.size)
    res.foreach(kv => assert(kv._2.isInstanceOf[HashSpec]))
    val expect1 = HashSpec.newBuilder.setHashSize(2).build
    val expect2 = HashSpec.newBuilder().build()
    assert(expect1 == res(10L).asInstanceOf[HashSpec])
    assert(expect2 == res(20L).asInstanceOf[HashSpec])

    // convert back
    val resJava = KijiScheme.convertScalaTypesToKijiValues(res, null, null)
    assert(resJava.isInstanceOf[util.NavigableMap[lang.Long, java.lang.Object]])
    assert(resJava == recordHashMap)
  }

  test("Test Bad Return Type") {
    intercept[InvalidClassException] {
      KijiScheme.convertKijiValuesToScalaTypes(badRecordHashMap)
    }
  }
}
