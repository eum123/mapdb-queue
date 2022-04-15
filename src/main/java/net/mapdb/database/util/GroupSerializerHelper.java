package net.mapdb.database.util;

import net.mapdb.database.exception.UnsupportedClassType;
import org.mapdb.Serializer;
import org.mapdb.serializer.GroupSerializer;

public class GroupSerializerHelper {
    /**
     * Class에 맞는 Serializer를 반환한다.
     * @param clazz
     * @return
     * @throws UnsupportedClassType
     */
    public static GroupSerializer convertClassToGroupSerializer(Class clazz) throws UnsupportedClassType {
        if(clazz == String.class) {
            return Serializer.STRING;
        }

        if(clazz == byte[].class) {
            return Serializer.BYTE_ARRAY;
        }

        throw new UnsupportedClassType("unsupported class type :" + clazz);
    }
}
