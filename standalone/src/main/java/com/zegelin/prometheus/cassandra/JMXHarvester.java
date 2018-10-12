package com.zegelin.prometheus.cassandra;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.zegelin.prometheus.cassandra.cli.HarvesterOptions;
import com.zegelin.prometheus.cassandra.collector.RemoteGossiperMBeanMetricFamilyCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.*;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


public class JMXHarvester extends Harvester {
    private static final Logger logger = LoggerFactory.getLogger(JMXHarvester.class);

    private final MBeanServerConnection mBeanServerConnection;

    @SuppressWarnings("FieldCanBeLocal")
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

    JMXHarvester(final MBeanServerConnection mBeanServerConnection, final MetadataFactory metadataFactory, final HarvesterOptions options) {
        super(metadataFactory, options);

        this.mBeanServerConnection = mBeanServerConnection;

        // periodically scan for new/destroyed MBeans
        scheduledExecutorService.scheduleWithFixedDelay(this::reconcileMBeans, 0, 30, TimeUnit.SECONDS);

        addCollectorFactory(RemoteGossiperMBeanMetricFamilyCollector.factory(metadataFactory));
    }

    private Set<ObjectInstance> currentMBeans = ImmutableSet.of();

    void reconcileMBeans() {
        try {
            final Set<ObjectInstance> mBeans = mBeanServerConnection.queryMBeans(null, null);

            // unregister
            {
                final Set<ObjectInstance> removedMBeans = Sets.difference(currentMBeans, mBeans);

                logger.debug("Removing {} old MBeans.", removedMBeans.size());

                for (final ObjectInstance instance : removedMBeans) {
                    unregisterMBean(instance.getObjectName());
                }
            }

            // register
            {
                final Set<ObjectInstance> addedMBeans = Sets.difference(mBeans, currentMBeans);

                logger.debug("Found {} new MBeans.", addedMBeans.size());

                for (final ObjectInstance instance : addedMBeans) {
                    final MBeanInfo mBeanInfo = mBeanServerConnection.getMBeanInfo(instance.getObjectName());
                    final Descriptor mBeanDescriptor = mBeanInfo.getDescriptor();

                    final String interfaceClassName = (String) mBeanDescriptor.getFieldValue(JMX.INTERFACE_CLASS_NAME_FIELD);
                    if (interfaceClassName == null) {
                        logger.debug("Cannot register MBean {}. MBean interface class name not defined.", instance);

                        continue;
                    }

                    final Class<?> interfaceClass;
                    try {
                        interfaceClass = Class.forName(interfaceClassName);

                    } catch (final ClassNotFoundException e) {
                        logger.debug("Cannot register MBean {}. Unrecognised class.", instance);

                        continue;
                    }

                    logger.debug("Registering MBean/MXBean {}.", instance);

                    final boolean isMXBean = Boolean.valueOf((String) mBeanDescriptor.getFieldValue(JMX.MXBEAN_FIELD));

                    final ObjectName objectName = instance.getObjectName();
                    final Object mBeanProxy;
                    if (isMXBean) {
                        mBeanProxy = JMX.newMXBeanProxy(mBeanServerConnection, objectName, interfaceClass);
                    } else {
                        mBeanProxy = JMX.newMBeanProxy(mBeanServerConnection, objectName, interfaceClass);
                    }

                    registerMBean(mBeanProxy, objectName);
                }
            }

            currentMBeans = mBeans;

        } catch (final Throwable e) {
            logger.error("Failed to reconcile MBeans.", e);
        }
    }
}
