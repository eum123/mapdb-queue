package net.mapdb.database.util;

import net.mapdb.database.util.sequence.DatePrefixIntSequenceGenerator;
import net.mapdb.database.util.sequence.Sequence;
import org.junit.jupiter.api.Test;

public class SequenceTest {
    @Test
    public void sequenceTest() {
        Sequence sequence = new DatePrefixIntSequenceGenerator(5, 5);
        System.out.println(sequence.nextValue());
    }
}
