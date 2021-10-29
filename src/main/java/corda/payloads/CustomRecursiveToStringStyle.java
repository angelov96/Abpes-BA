package corda.payloads;

import corda.configuration.Configuration;
import org.apache.commons.lang.SystemUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.log4j.Logger;

import java.util.Collection;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CustomRecursiveToStringStyle extends ToStringStyle {

    private static final Logger LOG = Logger.getLogger(CustomRecursiveToStringStyle.class);

    private static final CustomRecursiveToStringStyle INSTANCE =
            new CustomRecursiveToStringStyle(Configuration.STRING_STYLE_MAX_RECURSIVE_DEPTH);
    private final int maxDepth;
    private final String tabs;
    // http://stackoverflow.com/a/16934373/603516
    private final ThreadLocal<MutableInteger> depth = ThreadLocal.withInitial(() -> new MutableInteger(0));

    public CustomRecursiveToStringStyle(int maxDepth) {
        this.maxDepth = maxDepth;
        tabs = StringUtils.repeat("\t", maxDepth);

        setUseShortClassName(true);
        setUseIdentityHashCode(false);
        setContentStart(" {");
        setFieldSeparator(SystemUtils.LINE_SEPARATOR);
        setFieldSeparatorAtStart(true);
        setFieldNameValueSeparator(" = ");
        setContentEnd("}");
    }

    public static ToStringStyle getInstance() {
        return INSTANCE;
    }

    public static String toString(Object value) {
        StringBuffer sb = new StringBuffer();
        INSTANCE.appendDetail(sb, null, value);
        return sb.toString();
    }

    @Override
    protected void appendDetail(StringBuffer buffer, String fieldName, Object value) {
        if (getDepth() >= maxDepth || noReflectionNeeded(value)) {
            appendTabified(buffer, String.valueOf(value));
        } else {
            new ReflectionToStringBuilder(value, this, buffer, null, false, false).toString();
        }
    }

    private int getDepth() {
        return depth.get().get();
    }

    private StringBuffer appendTabified(StringBuffer buffer, String value) {
        Matcher matcher = Pattern.compile("\n").matcher(value);
        String replacement = "\n" + tabs.substring(0, getDepth());
        while (matcher.find()) {
            matcher.appendReplacement(buffer, replacement);
        }
        matcher.appendTail(buffer);
        return buffer;
    }

    private boolean noReflectionNeeded(Object value) {
        try {
            return value != null &&
                    (value.getClass().getName().startsWith("java.lang.")
                            || value.getClass().getMethod("toString").getDeclaringClass() != Object.class);
        } catch (NoSuchMethodException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    protected void appendFieldSeparator(StringBuffer buffer) {
        buffer.append(getFieldSeparator());
        padDepth(buffer);
    }

    private void padDepth(StringBuffer buffer) {
        buffer.append(tabs, 0, getDepth());
    }

    @Override
    public void appendStart(StringBuffer buffer, Object object) {
        depth.get().increment();
        super.appendStart(buffer, object);
    }

    @Override
    public void appendEnd(StringBuffer buffer, Object object) {
        super.appendEnd(buffer, object);
        buffer.setLength(buffer.length() - getContentEnd().length());
        buffer.append(SystemUtils.LINE_SEPARATOR);
        depth.get().decrement();
        padDepth(buffer);
        appendContentEnd(buffer);
    }

    @Override
    public void append(final StringBuffer buffer, final String fieldName, final Object value,
                       final Boolean fullDetail) {
        Object valueTmp;
        appendFieldStart(buffer, fieldName);

        if (value == null) {
            appendNullText(buffer, fieldName);

        } else {

            if (value instanceof byte[]) {
                valueTmp = new String((byte[]) value) + " <- byte array as string | ";
                appendInternal(buffer, fieldName, valueTmp, isFullDetail(fullDetail));
            }
            appendInternal(buffer, fieldName, value, isFullDetail(fullDetail));
        }

        appendFieldEnd(buffer, fieldName);
    }

    @Override
    protected void removeLastFieldSeparator(StringBuffer buffer) {
        int len = buffer.length();
        int sepLen = getFieldSeparator().length() + getDepth();
        if (sepLen > 0 && len >= sepLen) {
            buffer.setLength(len - sepLen);
        }
    }

    @Override
    protected void appendDetail(StringBuffer buffer, String fieldName, Collection<?> coll) {
        buffer.append(ReflectionToStringBuilder.toString(coll.toArray(), this, true, true));
    }

    static class MutableInteger {
        private int value;

        MutableInteger(int value) {
            this.value = value;
        }

        public final int get() {
            return value;
        }

        public final void increment() {
            ++value;
        }

        public final void decrement() {
            --value;
        }
    }
}
