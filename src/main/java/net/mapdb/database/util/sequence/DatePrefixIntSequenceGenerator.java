package net.mapdb.database.util.sequence;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class DatePrefixIntSequenceGenerator extends Sequence {
	private int seq = (int) System.currentTimeMillis();
	private DateTimeFormatter formatter;

	public DatePrefixIntSequenceGenerator(int minLength,
										  int maxLength) {

		this(DateTimeFormatter.ofPattern("yyyyMMddhhmmss"), minLength, maxLength);

	}

	public DatePrefixIntSequenceGenerator(DateTimeFormatter formatter, int minLength,
                                          int maxLength) {
		super(minLength, maxLength);
		this.formatter = formatter;

		if (seq < 0) {
			seq += Integer.MAX_VALUE;
		}
	}

	protected String generatePrefix() {
		return LocalDateTime.now().format(formatter);
	}

	protected String generate() {

		String v = Integer.toString(seq++);

		if (seq < 0) {
			seq = 0;
		}

		return v;
	}
}
