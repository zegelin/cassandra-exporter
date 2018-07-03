package com.zegelin.prometheus.cassandra;

import com.sun.jmx.mbeanserver.JmxMBeanServer;
import com.zegelin.jmx.DelegatingMBeanServerInterceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.*;
import java.lang.management.ManagementFactory;

/**
 * A {@link Harvester} that
 */

public class MBeanServerInterceptorHarvester extends BaseHarvester {
    private static final Logger logger = LoggerFactory.getLogger(MBeanServerInterceptorHarvester.class);

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

    public MBeanServerInterceptorHarvester() {
        super(new FactoriesProvider(new InternalMetadataFactory()));

        registerMBeanServerInterceptor();
    }

    private void registerMBeanServerInterceptor() {
        final JmxMBeanServer mBeanServer = (JmxMBeanServer) ManagementFactory.getPlatformMBeanServer();

        final MBeanServerInterceptor interceptor = new MBeanServerInterceptor(mBeanServer.getMBeanServerInterceptor());

        mBeanServer.setMBeanServerInterceptor(interceptor);
    }


}
