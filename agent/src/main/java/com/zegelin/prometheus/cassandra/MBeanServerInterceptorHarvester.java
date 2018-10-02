package com.zegelin.prometheus.cassandra;

import com.google.common.collect.ImmutableList;
import com.sun.jmx.mbeanserver.JmxMBeanServer;
import com.zegelin.jmx.DelegatingMBeanServerInterceptor;
import com.zegelin.prometheus.cassandra.cli.HarvesterOptions;

import javax.management.*;
import java.lang.management.ManagementFactory;
import java.lang.management.PlatformManagedObject;
import java.util.List;
import java.util.stream.Collectors;

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
        super(new InternalMetadataFactory(), options);

        registerPlatformMXBeans();
        registerMBeanServerInterceptor();

    }

    private void registerPlatformMXBeans() {
        ManagementFactory.getPlatformManagementInterfaces().stream()
                .flatMap(i -> ManagementFactory.getPlatformMXBeans(i).stream())
                .distinct()
                .forEach(mxBean -> registerMBean(mxBean, mxBean.getObjectName()));
    }

    private void registerMBeanServerInterceptor() {
        final JmxMBeanServer mBeanServer = (JmxMBeanServer) ManagementFactory.getPlatformMBeanServer();

        final MBeanServerInterceptor interceptor = new MBeanServerInterceptor(mBeanServer.getMBeanServerInterceptor());

        mBeanServer.setMBeanServerInterceptor(interceptor);
    }
}
