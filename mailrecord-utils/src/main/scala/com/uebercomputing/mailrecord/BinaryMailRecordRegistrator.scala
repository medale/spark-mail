package com.uebercomputing.mailrecord

import org.apache.spark.serializer.KryoRegistrator
import com.esotericsoftware.kryo.Kryo
import com.twitter.chill.avro.AvroSerializer

/**
 * Lets Kryo know how to serialize a BinaryMailRecord via Twitter's chill-avro library.
 */
class BinaryMailRecordRegistrar extends KryoRegistrator {

  def registerClasses(kryo: Kryo): Unit = {
    kryo.register(classOf[BinaryMailRecord], AvroSerializer.SpecificRecordBinarySerializer[BinaryMailRecord])
  }
}
