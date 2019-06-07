package com.zegelin.cassandra.exporter.collector;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.zegelin.cassandra.exporter.MBeanGroupMetricFamilyCollector;
import com.zegelin.prometheus.domain.MetricFamily;

import javax.management.ObjectName;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CachingCollector extends MBeanGroupMetricFamilyCollector {

    public static Factory cache(final Factory delegateFactory, final long duration, final TimeUnit unit) {
        return (mBean) -> {
            final MBeanGroupMetricFamilyCollector collector = delegateFactory.createCollector(mBean);

            if (collector == null) {
                return null;
            }

            return new CachingCollector(collector, duration, unit);
        };
    }


    private final MBeanGroupMetricFamilyCollector delegate;
    private final long duration;
    private final TimeUnit unit;

    private final Supplier<List<MetricFamily>> cachedCollect;


    private CachingCollector(final MBeanGroupMetricFamilyCollector delegate, final long duration, final TimeUnit unit) {
        this.delegate = delegate;
        this.duration = duration;
        this.unit = unit;

        this.cachedCollect = Suppliers.memoizeWithExpiration(() -> {
            return delegate.collect().map(MetricFamily::cache).collect(Collectors.toList());
        }, duration, unit);
    }

    @Override
    public String name() {
        return delegate.name();
    }

    @Override
    public MBeanGroupMetricFamilyCollector merge(final MBeanGroupMetricFamilyCollector rawOther) {
        if (!(rawOther instanceof CachingCollector)) {
            throw new IllegalStateException();
        }

        final MBeanGroupMetricFamilyCollector otherDelegate = ((CachingCollector) rawOther).delegate;

        final MBeanGroupMetricFamilyCollector newDelegate = delegate.merge(otherDelegate);

        return new CachingCollector(newDelegate, duration, unit);
    }

    @Override
    public MBeanGroupMetricFamilyCollector removeMBean(final ObjectName mBeanName) {
        final MBeanGroupMetricFamilyCollector newDelegate = delegate.removeMBean(mBeanName);

        if (newDelegate == null) {
            return null;
        }

        return new CachingCollector(newDelegate, duration, unit);
    }

    @Override
    public Stream<MetricFamily> collect() {
        return cachedCollect.get().stream();
    }
}
