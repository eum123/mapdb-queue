package net.mapdb.database;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mapdb.Serializer;
import org.mapdb.serializer.GroupSerializer;
import org.mapdb.serializer.SerializerString;
import org.mapdb.serializer.SerializerUtils;

public class SerializerTest {
    @Test
    public void test() {
        System.out.println(SerializerUtils.serializerForClass(String.class).getClass());
        System.out.println(SerializerUtils.serializerForClass(String.class));
        System.out.println(GroupSerializer.STRING);

        Assertions.assertTrue((SerializerUtils.serializerForClass(String.class) instanceof  SerializerString));

        //Assertions.assertInstanceOf(SerializerUtils.serializerForClass(String.class).getClass(), SerializerString.STRING.getClass());

        Assertions.assertEquals(SerializerUtils.serializerForClass(String.class).getClass(), Serializer.STRING.getClass());
    }
}
