package org.iplantc.service.common.persistence.time;

import org.hibernate.HibernateException;
import org.hibernate.dialect.Dialect;
import org.hibernate.engine.SessionImplementor;
import org.hibernate.type.*;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.Comparator;

public class InstantType
        extends AbstractSingleColumnStandardBasicType<Instant>
        implements VersionType<Instant>, LiteralType<Instant> {
    public static final InstantType INSTANCE = new InstantType();

    public InstantType() {
        super( InstantTypeDescriptor.INSTANCE, JdbcInstantTypeDescriptor.INSTANCE );
    }

    public String getName() {
        return TimestampType.INSTANCE.getName();
    }

    @Override
    public String[] getRegistrationKeys() {
        return TimestampType.INSTANCE.getRegistrationKeys();
    }

    public Instant next(Instant current, SessionImplementor session) {
        return seed(session);
    }

    public Instant seed(SessionImplementor session) {
        return Instant.now();
    }

    public Comparator<Instant> getComparator() {
        return getJavaTypeDescriptor().getComparator();
    }

    public String objectToSQLString(Instant value, Dialect dialect) throws Exception {
        Timestamp ts = new Timestamp( value.toEpochMilli() );
        return StringType.INSTANCE.objectToSQLString( ts.toString(), dialect );
    }

    public Instant fromStringValue(String value) throws HibernateException {
        return fromString(value);
    }
}