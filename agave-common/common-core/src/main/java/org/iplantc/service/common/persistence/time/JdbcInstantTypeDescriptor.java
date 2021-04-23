package org.iplantc.service.common.persistence.time;

import org.hibernate.cfg.Environment;
import org.hibernate.type.descriptor.WrapperOptions;
import org.hibernate.type.descriptor.java.AbstractTypeDescriptor;
import org.hibernate.type.descriptor.java.MutableMutabilityPlan;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

public class JdbcInstantTypeDescriptor extends AbstractTypeDescriptor<Instant> {
    public static final JdbcInstantTypeDescriptor INSTANCE = new JdbcInstantTypeDescriptor();
    public static final String TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss";

    public static class TimestampMutabilityPlan extends MutableMutabilityPlan<Instant> {
        public static final JdbcInstantTypeDescriptor.TimestampMutabilityPlan INSTANCE = new JdbcInstantTypeDescriptor.TimestampMutabilityPlan();

        public Instant deepCopyNotNull(Instant value) {
            return Instant.ofEpochMilli(value.toEpochMilli());
        }
    }

    public JdbcInstantTypeDescriptor() {
        super( Instant.class, JdbcInstantTypeDescriptor.TimestampMutabilityPlan.INSTANCE );
    }

    public String toString(Instant value) {
        return DateTimeFormatter.ofPattern(TIMESTAMP_FORMAT).format(value);
    }

    public Instant fromString(String string) {
        return DateTimeFormatter.ofPattern(TIMESTAMP_FORMAT).parse(string, Instant::from);
    }

    @Override
    public boolean areEqual(Instant one, Instant another) {
        if ( one == another ) {
            return true;
        }
        if ( one == null || another == null) {
            return false;
        }

        long t1 = one.toEpochMilli();
        long t2 = another.toEpochMilli();

        boolean oneIsInstant = one instanceof Instant;
        boolean anotherIsInstant = another instanceof Instant;

        long n1 = oneIsInstant ? one.toEpochMilli() : 0;
        long n2 = anotherIsInstant ? another.getEpochSecond() : 0;

        if ( !Environment.jvmHasJDK14Timestamp() ) {
            t1 += n1 / 1000000;
            t2 += n2 / 1000000;
        }

        if ( t1 != t2 ) {
            return false;
        }

        if ( oneIsInstant && anotherIsInstant ) {
            // both are Timestamps
            long nn1 = n1 % 1000000;
            long nn2 = n2 % 1000000;
            return nn1 == nn2;
        }
        else {
            // at least one is a plain old Date
            return true;
        }
    }

    @Override
    public int extractHashCode(Instant value) {
        return Long.valueOf( value.toEpochMilli()/ 1000 ).hashCode();
    }

    @SuppressWarnings({ "unchecked" })
    public <X> X unwrap(Instant value, Class<X> type, WrapperOptions options) {
        if ( value == null ) {
            return null;
        }
        if (Instant.class.isAssignableFrom(type)) {
            return (X) value;
        }
        if ( Timestamp.class.isAssignableFrom( type ) ) {
            return (X) new Timestamp(value.toEpochMilli());
        }
        if ( java.sql.Date.class.isAssignableFrom( type ) ) {
            return (X) new java.sql.Date( value.toEpochMilli() );
        }
        if ( java.sql.Time.class.isAssignableFrom( type ) ) {
            return (X) new java.sql.Time( value.toEpochMilli() );
        }
        if ( Date.class.isAssignableFrom( type ) ) {
            return (X) Date.from(value);
        }
        if ( Calendar.class.isAssignableFrom( type ) ) {
            final GregorianCalendar cal = new GregorianCalendar();
            cal.setTimeInMillis( value.toEpochMilli() );
            return (X) cal;
        }
        if ( Long.class.isAssignableFrom( type ) ) {
            return (X) Long.valueOf( value.toEpochMilli() );
        }
        throw unknownUnwrap( type );
    }

    @SuppressWarnings({ "UnnecessaryUnboxing" })
    public <X> Instant wrap(X value, WrapperOptions options) {
        if ( value == null ) {
            return null;
        }
        if (value instanceof Timestamp) {
            return ((Timestamp) value).toInstant();
        }

        if (value instanceof Long) {
            return Instant.ofEpochMilli( ( (Long) value ).longValue() );
        }

        if (value instanceof Calendar) {
            return Instant.ofEpochMilli( ( (Calendar) value ).getTimeInMillis() );
        }

        if (value instanceof Date) {
            return ((Date)value).toInstant();
        }

        if (value instanceof Instant) {
            return (Instant) value;
        }

        throw unknownWrap( value.getClass() );
    }
}
