package com.zegelin.cassandra.exporter;

import com.sun.jmx.mbeanserver.JmxMBeanServer;
import com.zegelin.cassandra.exporter.collector.InternalGossiperMBeanMetricFamilyCollector;
import com.zegelin.jmx.DelegatingMBeanServerInterceptor;
import com.zegelin.cassandra.exporter.cli.HarvesterOptions;

import javax.management.*;
import java.lang.management.ManagementFactory;

class MBeanServerInterceptorHarvester extends Harvester {
    class MBeanServerInterceptor extends DelegatingMBeanServerInterceptor {
        MBeanServerInterceptor(final MBeanServer delegate) {
            super(delegate);
        }

        @Override
        public ObjectInstance registerMBean(final Object object, ObjectName name) throws InstanceAlreadyExistsException, MBeanRegistrationException, NotCompliantMBeanException {
            // delegate first so that any exceptions (such as InstanceAlreadyExistsException) will throw first before additional processing occurs.
            final ObjectInstance objectInstance = super.registerMBean(object, name);

            // a MBean can provide its own name upon registration
            name = objectInstance.getObjectName();

            MBeanServerInterceptorHarvester.this.registerMBean(object, name);

            return objectInstance;
        }

        @Override
        public void unregisterMBean(final ObjectName mBeanName) throws InstanceNotFoundException, MBeanRegistrationException {
            try {
                MBeanServerInterceptorHarvester.this.unregisterMBean(mBeanName);

            } finally {
                super.unregisterMBean(mBeanName);
            }
        }
    }

    MBeanServerInterceptorHarvester(final HarvesterOptions options) {
        this(new InternalMetadataFactory(), options);
    }

    private MBeanServerInterceptorHarvester(final MetadataFactory metadataFactory, final HarvesterOptions options) {
        super(metadataFactory, options);

        registerPlatformMXBeans();

        installMBeanServerInterceptor();

        addCollectorFactory(InternalGossiperMBeanMetricFamilyCollector.factory(metadataFactory));
    }


    private void registerPlatformMXBeans() {
        // the platform MXBeans get registered right at JVM startup, before the agent gets a chance to
        // install the interceptor.
        // instead, directly register the MXBeans here...
        ManagementFactory.getPlatformManagementInterfaces().stream()
                .flatMap(i -> ManagementFactory.getPlatformMXBeans(i).stream())
                .distinct()
                .forEach(mxBean -> registerMBean(mxBean, mxBean.getObjectName()));
    }

    private void installMBeanServerInterceptor() {
        final JmxMBeanServer mBeanServer = (JmxMBeanServer) ManagementFactory.getPlatformMBeanServer();

        final MBeanServerInterceptor interceptor = new MBeanServerInterceptor(mBeanServer.getMBeanServerInterceptor());

        mBeanServer.setMBeanServerInterceptor(interceptor);
    }
}
