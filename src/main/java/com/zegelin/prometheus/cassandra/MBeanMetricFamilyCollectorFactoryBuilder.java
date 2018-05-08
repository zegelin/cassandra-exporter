package com.zegelin.prometheus.cassandra;

import com.zegelin.prometheus.cassandra.collector.FooBar;

import javax.management.*;
import java.util.*;
import java.util.function.Function;


/**
 * A builder of MBeanMetricFamilyCollectorFactories
 */
class MBeanMetricFamilyCollectorFactoryBuilder {
    private final FooBar fooBar;
    private final QueryExp objectNameQuery;
    private final String metricFamilyName;

    private String help;

    interface LabelMaker extends Function<Map<String, String>, Map<String, String>> {
        @Override
        Map<String, String> apply(Map<String, String> keyPropertyList);
    }

    private final List<LabelMaker> labelMakers = new ArrayList<>();


    MBeanMetricFamilyCollectorFactoryBuilder(final FooBar fooBar, final QueryExp objectNameQuery, final String metricFamilyName) {
        this.fooBar = fooBar;
        this.objectNameQuery = objectNameQuery;
        this.metricFamilyName = metricFamilyName;
    }


    MBeanMetricFamilyCollectorFactoryBuilder withLabelMaker(final LabelMaker labelMaker) {
        labelMakers.add(labelMaker);

        return this;
    }

    MBeanMetricFamilyCollectorFactoryBuilder withHelp(final String help) {
        this.help = help;

        return this;
    }


    MBeanMetricFamilyCollectorFactory build() {
        return new MBeanMetricFamilyCollectorFactory() {
            @Override
            public MBeanMetricFamilyCollector createCollector(final NamedObject<?> mBean) {
                try {
                    if (!objectNameQuery.apply(mBean.name))
                        return null;
                } catch (final BadStringOperationException | BadBinaryOpValueExpException | BadAttributeValueExpException | InvalidApplicationException e) {
                    throw new IllegalStateException("Failed to apply object name query to object name.", e);
                }

                final Map<String, String> keyPropertyList = mBean.name.getKeyPropertyList();

                final Map<String, String> labels = new HashMap<>();
                {
                    for (final LabelMaker labelMaker : labelMakers) {
                        labels.putAll(labelMaker.apply(keyPropertyList));
                    }
                }

                final String name = String.format("cassandra_%s", metricFamilyName);

                return fooBar.forMBean(name, help, labels, mBean);
            }
        };
    }
}
