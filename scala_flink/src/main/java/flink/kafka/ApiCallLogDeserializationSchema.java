package flink.kafka;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;

/**
 * @author 张朋飞
 */
public class ApiCallLogDeserializationSchema implements DeserializationSchema {

    @Override
    public Object deserialize(byte[] message) {
        ByteBuffer buffer = ByteBuffer.wrap(message).order(ByteOrder.LITTLE_ENDIAN);
        String mess = byteBuffertoString(buffer);
        //封装为POJO类
        Gson gson = new Gson();
        ApiCallLog data = null;
        try {
            data = gson.fromJson(mess, ApiCallLog.class);
        } catch (JsonSyntaxException e) {
            e.printStackTrace();
        }
        return data;
    }

    @Override
    public boolean isEndOfStream(Object nextElement) {
        return false;
    }

    @Override
    public TypeInformation getProducedType() {
        return TypeExtractor.getForClass(ApiCallLog.class);
    }

    /**
     * 将ByteBuffer类型转换为String类型
     *
     * @param buffer
     * @return
     */
    public static String byteBuffertoString(ByteBuffer buffer) {
        Charset charset = null;
        CharsetDecoder decoder = null;
        CharBuffer charBuffer = null;
        try {
            charset = Charset.forName("UTF-8");
            decoder = charset.newDecoder();
            // charBuffer = decoder.decode(buffer);//用这个的话，只能输出来一次结果，第二次显示为空
            charBuffer = decoder.decode(buffer.asReadOnlyBuffer());
            return charBuffer.toString();
        } catch (Exception ex) {
            ex.printStackTrace();
            return "";
        }
    }
}
