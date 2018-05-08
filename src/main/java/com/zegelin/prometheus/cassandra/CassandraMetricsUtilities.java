package com.zegelin.prometheus.cassandra;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import com.google.common.reflect.TypeToken;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

@SuppressWarnings("Duplicates")
public class CassandraMetricsUtilities {
    private static final Map<Class<? extends CassandraMetricsRegistry.MetricMBean>, Field> MBEAN_METRIC_FIELDS = new HashMap<>();

//    /**
//     * Given a Cassandra MetricMBean return the internal Codahale/Dropwizard Metric object
//     */
//    public static <T extends Metric> NamedObject<T> metricForMBean(final NamedObject<?> mBean, final Class<T> metricClass) {
//        return mBean.map((name, object) -> {
//            if (!(object instanceof CassandraMetricsRegistry.MetricMBean)) {
//                throw new IllegalStateException(String.format("MBean %s isn't an instance of CassandraMetricsRegistry.MetricMBean", name));
//            }
//
//            final CassandraMetricsRegistry.MetricMBean metricMBean = (CassandraMetricsRegistry.MetricMBean) object;
//
//            try {
//                final Field metricField = MBEAN_METRIC_FIELDS.computeIfAbsent(metricMBean.getClass(), clazz -> {
//                    try {
//                        @SuppressWarnings("JavaReflectionMemberAccess")
//                        final Field field = clazz.getDeclaredField("metric");
//                        field.setAccessible(true);
//
//                        return field;
//
//                    } catch (final NoSuchFieldException e) {
//                        throw new IllegalArgumentException(e);
//                    }
//                });
//
//                final Metric rawMetric = (Metric) metricField.get(object);
//
//                if (!metricClass.isInstance(rawMetric)) {
//                    throw new IllegalStateException(String.format("%s's internal metric object isn't an instance of %s", name, metricClass));
//                }
//
//                return metricClass.cast(rawMetric);
//
//
//            } catch (final IllegalAccessException e) {
//                throw new IllegalArgumentException(e);
//            }
//        });
//    }


    public static <T> NamedObject<T> metricForMBean(final NamedObject<?> mBean) {

        return mBean.map((name, object) -> {
            if (!(object instanceof CassandraMetricsRegistry.MetricMBean)) {
                throw new IllegalStateException(String.format("MBean %s isn't an instance of CassandraMetricsRegistry.MetricMBean", name));
            }

            final CassandraMetricsRegistry.MetricMBean metricMBean = (CassandraMetricsRegistry.MetricMBean) object;

            try {
                final Field metricField = MBEAN_METRIC_FIELDS.computeIfAbsent(metricMBean.getClass(), clazz -> {
                    try {
                        @SuppressWarnings("JavaReflectionMemberAccess")
                        final Field field = clazz.getDeclaredField("metric");
                        field.setAccessible(true);

                        return field;

                    } catch (final NoSuchFieldException e) {
                        throw new IllegalArgumentException(e);
                    }
                });

                final Metric rawMetric = (Metric) metricField.get(object);

                return (T) rawMetric;


            } catch (final IllegalAccessException e) {
                throw new IllegalArgumentException(e);
            }
        });
    }

    public static Class gaugeType(final Gauge<?> gauge) {
        try {
            return gauge.getClass().getMethod("getValue").getReturnType();

        } catch (final NoSuchMethodException e) {
            throw new IllegalStateException("Gauge object is missing its getValue() method.", e);
        }
    }

}
