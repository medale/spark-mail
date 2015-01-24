package com.uebercomputing.mailrecord

import org.apache.spark.serializer.KryoRegistrator
import com.esotericsoftware.kryo.Kryo
import com.twitter.chill.avro.AvroSerializer

/**
 * Lets Kryo know how to serialize a MailRecord via Twitter's chill-avro library.
 */
class MailRecordRegistrar extends KryoRegistrator {

  def registerClasses(kryo: Kryo): Unit = {
    kryo.register(classOf[MailRecord], AvroSerializer.SpecificRecordBinarySerializer[MailRecord])
  }
}
