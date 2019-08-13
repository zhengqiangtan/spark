/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.serializer

import java.io._
import java.nio.ByteBuffer
import javax.annotation.concurrent.NotThreadSafe

import scala.reflect.ClassTag

import org.apache.spark.SparkEnv
import org.apache.spark.annotation.{DeveloperApi, Private}
import org.apache.spark.util.NextIterator

/**
 * :: DeveloperApi ::
 * A serializer. Because some serialization libraries are not thread safe, this class is used to
 * create [[org.apache.spark.serializer.SerializerInstance]] objects that do the actual
 * serialization and are guaranteed to only be called from one thread at a time.
 *
 * Implementations of this trait should implement:
 *
 * 1. a zero-arg constructor or a constructor that accepts a [[org.apache.spark.SparkConf]]
 * as parameter. If both constructors are defined, the latter takes precedence.
 *
 * 2. Java serialization interface.
 *
 * @note Serializers are not required to be wire-compatible across different versions of Spark.
 * They are intended to be used to serialize/de-serialize data within a single Spark application.
 */
@DeveloperApi
abstract class Serializer {

  /**
   * Default ClassLoader to use in deserialization. Implementations of [[Serializer]] should
   * make sure it is using this when set.
   */
  @volatile protected var defaultClassLoader: Option[ClassLoader] = None

  /**
   * Sets a class loader for the serializer to use in deserialization.
    *
    * 反序列化时使用的类加载器，要保证子类优先使用该类加载器
   *
   * @return this Serializer object
   */
  def setDefaultClassLoader(classLoader: ClassLoader): Serializer = {
    defaultClassLoader = Some(classLoader)
    this
  }

  /** Creates a new [[SerializerInstance]].
    *
    * 创建一个新的序列化器实例
    **/
  def newInstance(): SerializerInstance

  /**
   * :: Private ::
   * Returns true if this serializer supports relocation of its serialized objects and false
   * otherwise. This should return true if and only if reordering the bytes of serialized objects
   * in serialization stream output is equivalent to having re-ordered those elements prior to
   * serializing them. More specifically, the following should hold if a serializer supports
   * relocation:
   *
   * {{{
   * serOut.open()
   * position = 0
   * serOut.write(obj1)
   * serOut.flush()
   * position = # of bytes writen to stream so far
   * obj1Bytes = output[0:position-1]
   * serOut.write(obj2)
   * serOut.flush()
   * position2 = # of bytes written to stream so far
   * obj2Bytes = output[position:position2-1]
   * serIn.open([obj2bytes] concatenate [obj1bytes]) should return (obj2, obj1)
   * }}}
   *
   * In general, this property should hold for serializers that are stateless and that do not
   * write special metadata at the beginning or end of the serialization stream.
   *
   * This API is private to Spark; this method should not be overridden in third-party subclasses
   * or called in user code and is subject to removal in future Spark releases.
   *
   * See SPARK-7311 for more details.
    * 参数如果是true，则表示该序列化器支持重新定位他的序列化对象，否则则不行。
    * 如果支持，这表示在流中输出的被序列化的对象的字节可以进行排序。这相当于对象排序后再进行序列化。
    * 该属性现在被用于判断Shuffle使用哪个ShuffleWriter。
    *
   */
  @Private
  private[spark] def supportsRelocationOfSerializedObjects: Boolean = false
}


/**
 * :: DeveloperApi ::
 * An instance of a serializer, for use by one thread at a time.
 *
 * It is legal to create multiple serialization / deserialization streams from the same
 * SerializerInstance as long as those streams are all used within the same thread.
 */
@DeveloperApi
@NotThreadSafe
abstract class SerializerInstance {
  // 序列化特定类型的对象为ByteBuffer
  def serialize[T: ClassTag](t: T): ByteBuffer

  // 将ByteBuffer反序列化为特定类型的对象，会使用当前线程的类加载器
  def deserialize[T: ClassTag](bytes: ByteBuffer): T

  // 使用指定的类加载器将ByteBuffer反序列化为特定类型的对象
  def deserialize[T: ClassTag](bytes: ByteBuffer, loader: ClassLoader): T

  // 将输出流包装为序列化流
  def serializeStream(s: OutputStream): SerializationStream

  // 将输入流包装为反序列化流
  def deserializeStream(s: InputStream): DeserializationStream
}

/**
 * :: DeveloperApi ::
 * A stream for writing serialized objects.
 */
@DeveloperApi
abstract class SerializationStream {
  /** The most general-purpose method to write an object.
    * 将对象写出到SerializationStream
    **/
  def writeObject[T: ClassTag](t: T): SerializationStream
  /** Writes the object representing the key of a key-value pair.
    * 将key写出到SerializationStream
    **/
  def writeKey[T: ClassTag](key: T): SerializationStream = writeObject(key)
  /** Writes the object representing the value of a key-value pair. 、
    * 将value写出到SerializationStream
    **/
  def writeValue[T: ClassTag](value: T): SerializationStream = writeObject(value)
  // 刷新
  def flush(): Unit
  // 关闭
  def close(): Unit

  // 将迭代器中的对象写出到SerializationStream
  def writeAll[T: ClassTag](iter: Iterator[T]): SerializationStream = {
    // 遍历并调用writeObject()方法写出
    while (iter.hasNext) {
      writeObject(iter.next())
    }
    this
  }
}


/**
 * :: DeveloperApi ::
 * A stream for reading serialized objects.
 */
@DeveloperApi
abstract class DeserializationStream {
  /** The most general-purpose method to read an object.
    * 从反序列化流中读取对象
    **/
  def readObject[T: ClassTag](): T
  /** Reads the object representing the key of a key-value pair.
    * 从反序列化流中读取对象为Key
    **/
  def readKey[T: ClassTag](): T = readObject[T]()
  /** Reads the object representing the value of a key-value pair.
    * 从反序列化流中读取对象为Value
    **/
  def readValue[T: ClassTag](): T = readObject[T]()

  // 关闭流
  def close(): Unit

  /**
   * Read the elements of this stream through an iterator. This can only be called once, as
   * reading each element will consume data from the input source.
    *
    * 从反序列化流中读取对象并构建为迭代器，迭代器元素为单个对象
   */
  def asIterator: Iterator[Any] = new NextIterator[Any] {
    // 获取下一个元素的方法或通过调用readObject[Any]()方法进行读取
    override protected def getNext() = {
      try {
        readObject[Any]()
      } catch {
        case eof: EOFException => // 抛出EOFException说明读取完了
          // 标记finished为true并返回null
          finished = true
          null
      }
    }

    // 关闭操作
    override protected def close() {
      DeserializationStream.this.close()
    }
  }

  /**
   * Read the elements of this stream through an iterator over key-value pairs. This can only be
   * called once, as reading each element will consume data from the input source.
    *
    * 从反序列化流中读取对象并构建为迭代器，迭代器元素是键值对
   */
  def asKeyValueIterator: Iterator[(Any, Any)] = new NextIterator[(Any, Any)] {
    // 获取下一个元素的方法或通过两次调用readObject[Any]()方法进行读取
    override protected def getNext() = {
      try {
        // 同时读取两次，一个为键，一个为值
        (readKey[Any](), readValue[Any]())
      } catch {
        case eof: EOFException => // 抛出EOFException说明读取完了
          // 标记finished为true并返回null
          finished = true
          null
      }
    }

    override protected def close() {
      DeserializationStream.this.close()
    }
  }
}
