package com.zegelin.prometheus.cassandra;

import com.zegelin.jmx.NamedObject;
import com.zegelin.prometheus.domain.Labels;
import com.zegelin.prometheus.domain.MetricFamily;

import javax.management.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

public interface MBeanGroupMetricFamilyCollector {
    /**
     * @return the name of the collector. Collectors with the same name will be merged together {@see merge}.
     */
    String name();

    /**
     * Merge two {@link MBeanGroupMetricFamilyCollector}s together.
     *
     * @param other The other {@link MBeanGroupMetricFamilyCollector} to merge with.
     * @return a new {@link MBeanGroupMetricFamilyCollector} that is the combinator of this {@link MBeanGroupMetricFamilyCollector} and {@param other}
     */
    default MBeanGroupMetricFamilyCollector merge(final MBeanGroupMetricFamilyCollector other) {
        throw new IllegalStateException(String.format("Merging of %s and %s not implemented.", this, other));
    }

    /**
     * @return a new MBeanGroupMetricFamilyCollector with the named MBean removed, or null if the collector is empty.
     */
    default MBeanGroupMetricFamilyCollector removeMBean(final ObjectName mBeanName) {
        return null;
    }

    /**
     * @return a {@link Stream} of {@link MetricFamily}s that contain the metrics collected by this collector.
     */
    Stream<? extends MetricFamily<?>> collect();


    interface Factory {
        /**
         * Create a {@link MBeanGroupMetricFamilyCollector} for the given MBean, or null if this factory
         * doesn't support the given MBean.
         *
         * @return the MBeanGroupMetricFamilyCollector for the given MBean, or null
         */
        MBeanGroupMetricFamilyCollector createCollector(final NamedObject<?> mBean);


        /**
         * A builder of {@see MBeanGroupMetricFamilyCollector.Factory}s
         */
        class Builder {
            private final CollectorConstructor collectorConstructor;
            private final QueryExp objectNameQuery;
            private final String metricFamilyName;

            private String help;

            interface LabelMaker extends Function<Map<String, String>, Map<String, String>> {
                @Override
                Map<String, String> apply(Map<String, String> keyPropertyList);
            }

            private final List<LabelMaker> labelMakers = new ArrayList<>();


            Builder(final CollectorConstructor collectorConstructor, final QueryExp objectNameQuery, final String metricFamilyName) {
                this.collectorConstructor = collectorConstructor;
                this.objectNameQuery = objectNameQuery;
                this.metricFamilyName = metricFamilyName;
            }


            Builder withLabelMaker(final LabelMaker labelMaker) {
                labelMakers.add(labelMaker);

                return this;
            }

            Builder withHelp(final String help) {
                this.help = help;

                return this;
            }


            Factory build() {
                return new Factory() {
                    @Override
                    public MBeanGroupMetricFamilyCollector createCollector(final NamedObject<?> mBean) {
                        try {
                            if (!objectNameQuery.apply(mBean.name))
                                return null;
                        } catch (final BadStringOperationException | BadBinaryOpValueExpException | BadAttributeValueExpException | InvalidApplicationException e) {
                            throw new IllegalStateException("Failed to apply object name query to object name.", e);
                        }

                        final Map<String, String> keyPropertyList = mBean.name.getKeyPropertyList();

                        final Map<String, String> rawLabels = new HashMap<>();
                        {
                            for (final LabelMaker labelMaker : labelMakers) {
                                rawLabels.putAll(labelMaker.apply(keyPropertyList));
                            }
                        }

                        final String name = String.format("cassandra_%s", metricFamilyName);

                        return collectorConstructor.groupCollectorForMBean(name, help, new Labels(rawLabels), mBean);
                    }
                };
            }

            @FunctionalInterface
            public interface CollectorConstructor {
                MBeanGroupMetricFamilyCollector groupCollectorForMBean(final String name, final String help, final Labels labels, final NamedObject<?> mBean);
            }
        }
    }
}
