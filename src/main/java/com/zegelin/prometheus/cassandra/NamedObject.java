package com.zegelin.prometheus.cassandra;

import com.google.common.base.MoreObjects;

import javax.management.ObjectName;
import java.util.function.BiFunction;

/**
 * An arbitrary object (typically an MBean) and its JMX name.
 *
 * @param <T> Object type
 */
public class NamedObject<T> {
    public final ObjectName name;
    public final T object;

    public NamedObject(final ObjectName name, final T object) {
        this.object = object;
        this.name = name;
    }

    public <U> NamedObject<U> map(final BiFunction<ObjectName, ? super T, ? extends U> mapper) {
        final U mappedObject = mapper.apply(name, object);

        return mappedObject == null ? null : new NamedObject<>(name, mappedObject);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("name", name)
                .add("object", object)
                .toString();
    }
}
