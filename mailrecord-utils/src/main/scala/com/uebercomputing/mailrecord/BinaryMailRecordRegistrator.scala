package com.uebercomputing.mailrecord

import org.apache.spark.serializer.KryoRegistrator
import com.esotericsoftware.kryo.Kryo
import com.twitter.chill.avro.AvroSerializer

class BinaryMailRecordRegistrar extends KryoRegistrator {

  def registerClasses(kryo: Kryo): Unit = {
    kryo.register(classOf[BinaryMailRecord], AvroSerializer.SpecificRecordBinarySerializer[BinaryMailRecord])
  }
}
