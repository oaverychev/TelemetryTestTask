package storm.kafka;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.io.Serializable;

public class LogMessageSerializer extends com.esotericsoftware.kryo.Serializer<LogMessage> implements Serializable {

    public void write(Kryo kryo, Output output, LogMessage logMessage ) {
        // Write down all the fields of the object
        output.writeLong( logMessage.getUnixTimestamp() );
        output.writeBytes( logMessage.getName().getBytes() );
        output.writeBytes( logMessage.getType().getBytes() );
        output.writeInt(logMessage.getCount());

    }

    public LogMessage read(Kryo kryo, Input input, Class<LogMessage> aClass ) {
        // Read all the fields of the object
        LogMessage lm = new LogMessage( );
        lm.setUnixTimestamp(input.readLong());
        lm.setName(input.readString());
        lm.setType(input.readString());
        lm.setCount(input.readInt());
        return lm;
    }
}
