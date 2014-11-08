package mailrecord;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.io.IOUtils;

// scalastyle:off
class MailRecordWriter extends Closeable {

        private	var mailRecordWriter: DataFileWriter[MailRecord] = null

	def open(out: OutputStream): Unit = {
		val datumWriter = new SpecificDatumWriter[MailRecord](classOf[MailRecord])
		mailRecordWriter = new DataFileWriter[MailRecord](datumWriter)
		mailRecordWriter.setCodec(CodecFactory.snappyCodec())
		mailRecordWriter.create(MailRecord.getClassSchema(), out)
	}

	def append(record: MailRecord): Unit = {
		mailRecordWriter.append(record)
	}

	
	override def close() {
		IOUtils.closeQuietly(mailRecordWriter)
		mailRecordWriter = null
	}

}
