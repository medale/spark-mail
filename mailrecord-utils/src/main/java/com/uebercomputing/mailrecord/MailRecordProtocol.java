/**
 * Autogenerated by Avro
 * 
 * DO NOT EDIT DIRECTLY
 */
package com.uebercomputing.mailrecord;

@SuppressWarnings("all")
@org.apache.avro.specific.AvroGenerated
public interface MailRecordProtocol {
  public static final org.apache.avro.Protocol PROTOCOL = org.apache.avro.Protocol.parse("{\"protocol\":\"MailRecordProtocol\",\"namespace\":\"com.uebercomputing.mailrecord\",\"version\":\"1.0.0\",\"types\":[{\"type\":\"record\",\"name\":\"Attachment\",\"fields\":[{\"name\":\"fileName\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"}},{\"name\":\"size\",\"type\":\"int\"},{\"name\":\"mimeType\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"}},{\"name\":\"data\",\"type\":\"bytes\"}]},{\"type\":\"record\",\"name\":\"MailRecord\",\"fields\":[{\"name\":\"uuid\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"}},{\"name\":\"from\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"}},{\"name\":\"to\",\"type\":[\"null\",{\"type\":\"array\",\"items\":{\"type\":\"string\",\"avro.java.string\":\"String\"}}],\"default\":null},{\"name\":\"cc\",\"type\":[\"null\",{\"type\":\"array\",\"items\":{\"type\":\"string\",\"avro.java.string\":\"String\"}}],\"default\":null},{\"name\":\"bcc\",\"type\":[\"null\",{\"type\":\"array\",\"items\":{\"type\":\"string\",\"avro.java.string\":\"String\"}}],\"default\":null},{\"name\":\"dateUtcEpoch\",\"type\":\"long\"},{\"name\":\"subject\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"}},{\"name\":\"mailFields\",\"type\":[\"null\",{\"type\":\"map\",\"values\":{\"type\":\"string\",\"avro.java.string\":\"String\"},\"avro.java.string\":\"String\"}],\"default\":null},{\"name\":\"body\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"}},{\"name\":\"attachments\",\"type\":[\"null\",{\"type\":\"array\",\"items\":\"Attachment\"}],\"default\":null}]}],\"messages\":{}}");

  @SuppressWarnings("all")
  public interface Callback extends MailRecordProtocol {
    public static final org.apache.avro.Protocol PROTOCOL = com.uebercomputing.mailrecord.MailRecordProtocol.PROTOCOL;
  }
}