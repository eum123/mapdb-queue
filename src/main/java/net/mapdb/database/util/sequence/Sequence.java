package net.mapdb.database.util.sequence;

/**
 */
public abstract class Sequence { 
    private int minLen;

    private int maxLen;
    
    protected Sequence(int minLen, int maxLen) {
    	if(minLen <= 0) {
    		throw new IllegalArgumentException("첫번째 값은 0보다 커야 합니다.");
    	}
    	if(maxLen <= 0) {
    		throw new IllegalArgumentException("두번째 값은 0보다 커야 합니다.");
    	}
    	if(minLen > maxLen) {
    		throw new IllegalArgumentException("두번째 값은 첫번째 값보다 커야 합니다.");
    	}
    	this.minLen = minLen;
        this.maxLen = maxLen;
    }

    public synchronized final String nextValue() {
        return generatePrefix() + resize(generate());
    }

    private String resize(String v) {
        int vLen = v.length();

        if (vLen < minLen) {
            StringBuffer buf = new StringBuffer(minLen);

            for (int i = minLen - vLen; i > 0; i--) {
                buf.append('0');
            }

            buf.append(v);
            return buf.toString();
        }

        if (vLen > maxLen) {
            return v.substring(vLen - maxLen);
        }

        return v;
    }

    protected abstract String generatePrefix();

    protected abstract String generate();
}