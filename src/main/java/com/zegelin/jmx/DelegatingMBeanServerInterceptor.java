package com.zegelin.jmx;

import com.sun.jmx.interceptor.MBeanServerInterceptor;

import javax.management.*;
import javax.management.loading.ClassLoaderRepository;
import java.io.ObjectInputStream;
import java.util.Set;

public class DelegatingMBeanServerInterceptor implements MBeanServerInterceptor {
    private final MBeanServer delegate;

    public DelegatingMBeanServerInterceptor(final MBeanServer delegate) {
        this.delegate = delegate;
    }


    @Override
    public ObjectInstance createMBean(final String className, final ObjectName name) throws ReflectionException, InstanceAlreadyExistsException, MBeanRegistrationException, MBeanException, NotCompliantMBeanException {
        return delegate.createMBean(className, name);
    }

    @Override
    public ObjectInstance createMBean(final String className, final ObjectName name, final ObjectName loaderName) throws ReflectionException, InstanceAlreadyExistsException, MBeanRegistrationException, MBeanException, NotCompliantMBeanException, InstanceNotFoundException {
        return delegate.createMBean(className, name, loaderName);
    }

    @Override
    public ObjectInstance createMBean(final String className, final ObjectName name, final Object[] params, final String[] signature) throws ReflectionException, InstanceAlreadyExistsException, MBeanRegistrationException, MBeanException, NotCompliantMBeanException {
        return delegate.createMBean(className, name, params, signature);
    }

    @Override
    public ObjectInstance createMBean(final String className, final ObjectName name, final ObjectName loaderName, final Object[] params, final String[] signature) throws ReflectionException, InstanceAlreadyExistsException, MBeanRegistrationException, MBeanException, NotCompliantMBeanException, InstanceNotFoundException {
        return delegate.createMBean(className, name, loaderName, params, signature);
    }

    @Override
    public ObjectInstance registerMBean(final Object object, final ObjectName name) throws InstanceAlreadyExistsException, MBeanRegistrationException, NotCompliantMBeanException {
        return delegate.registerMBean(object, name);
    }

    @Override
    public void unregisterMBean(final ObjectName name) throws InstanceNotFoundException, MBeanRegistrationException {
        delegate.unregisterMBean(name);
    }

    @Override
    public ObjectInstance getObjectInstance(final ObjectName name) throws InstanceNotFoundException {
        return delegate.getObjectInstance(name);
    }

    @Override
    public Set<ObjectInstance> queryMBeans(final ObjectName name, final QueryExp query) {
        return delegate.queryMBeans(name, query);
    }

    @Override
    public Set<ObjectName> queryNames(final ObjectName name, final QueryExp query) {
        return delegate.queryNames(name, query);
    }

    @Override
    public boolean isRegistered(final ObjectName name) {
        return delegate.isRegistered(name);
    }

    @Override
    public Integer getMBeanCount() {
        return delegate.getMBeanCount();
    }

    @Override
    public Object getAttribute(final ObjectName name, final String attribute) throws MBeanException, AttributeNotFoundException, InstanceNotFoundException, ReflectionException {
        return delegate.getAttribute(name, attribute);
    }

    @Override
    public AttributeList getAttributes(final ObjectName name, final String[] attributes) throws InstanceNotFoundException, ReflectionException {
        return delegate.getAttributes(name, attributes);
    }

    @Override
    public void setAttribute(final ObjectName name, final Attribute attribute) throws InstanceNotFoundException, AttributeNotFoundException, InvalidAttributeValueException, MBeanException, ReflectionException {
        delegate.setAttribute(name, attribute);
    }

    @Override
    public AttributeList setAttributes(final ObjectName name, final AttributeList attributes) throws InstanceNotFoundException, ReflectionException {
        return delegate.setAttributes(name, attributes);
    }

    @Override
    public Object invoke(final ObjectName name, final String operationName, final Object[] params, final String[] signature) throws InstanceNotFoundException, MBeanException, ReflectionException {
        return delegate.invoke(name, operationName, params, signature);
    }

    @Override
    public String getDefaultDomain() {
        return delegate.getDefaultDomain();
    }

    @Override
    public String[] getDomains() {
        return delegate.getDomains();
    }

    @Override
    public void addNotificationListener(final ObjectName name, final NotificationListener listener, final NotificationFilter filter, final Object handback) throws InstanceNotFoundException {
        delegate.addNotificationListener(name, listener, filter, handback);
    }

    @Override
    public void addNotificationListener(final ObjectName name, final ObjectName listener, final NotificationFilter filter, final Object handback) throws InstanceNotFoundException {
        delegate.addNotificationListener(name, listener, filter, handback);
    }

    @Override
    public void removeNotificationListener(final ObjectName name, final ObjectName listener) throws InstanceNotFoundException, ListenerNotFoundException {
        delegate.removeNotificationListener(name, listener);
    }

    @Override
    public void removeNotificationListener(final ObjectName name, final ObjectName listener, final NotificationFilter filter, final Object handback) throws InstanceNotFoundException, ListenerNotFoundException {
        delegate.removeNotificationListener(name, listener, filter, handback);
    }

    @Override
    public void removeNotificationListener(final ObjectName name, final NotificationListener listener) throws InstanceNotFoundException, ListenerNotFoundException {
        delegate.removeNotificationListener(name, listener);
    }

    @Override
    public void removeNotificationListener(final ObjectName name, final NotificationListener listener, final NotificationFilter filter, final Object handback) throws InstanceNotFoundException, ListenerNotFoundException {
        delegate.removeNotificationListener(name, listener, filter, handback);
    }

    @Override
    public MBeanInfo getMBeanInfo(final ObjectName name) throws InstanceNotFoundException, IntrospectionException, ReflectionException {
        return delegate.getMBeanInfo(name);
    }

    @Override
    public boolean isInstanceOf(final ObjectName name, final String className) throws InstanceNotFoundException {
        return delegate.isInstanceOf(name, className);
    }

    @Override
    public Object instantiate(final String className) throws ReflectionException, MBeanException {
        return delegate.instantiate(className);
    }

    @Override
    public Object instantiate(final String className, final ObjectName loaderName) throws ReflectionException, MBeanException, InstanceNotFoundException {
        return delegate.instantiate(className, loaderName);
    }

    @Override
    public Object instantiate(final String className, final Object[] params, final String[] signature) throws ReflectionException, MBeanException {
        return delegate.instantiate(className, params, signature);
    }

    @Override
    public Object instantiate(final String className, final ObjectName loaderName, final Object[] params, final String[] signature) throws ReflectionException, MBeanException, InstanceNotFoundException {
        return delegate.instantiate(className, loaderName, params, signature);
    }

    @Override
    @Deprecated
    public ObjectInputStream deserialize(final ObjectName name, final byte[] data) throws InstanceNotFoundException, OperationsException {
        return delegate.deserialize(name, data);
    }

    @Override
    @Deprecated
    public ObjectInputStream deserialize(final String className, final byte[] data) throws OperationsException, ReflectionException {
        return delegate.deserialize(className, data);
    }

    @Override
    @Deprecated
    public ObjectInputStream deserialize(final String className, final ObjectName loaderName, final byte[] data) throws InstanceNotFoundException, OperationsException, ReflectionException {
        return delegate.deserialize(className, loaderName, data);
    }

    @Override
    public ClassLoader getClassLoaderFor(final ObjectName mbeanName) throws InstanceNotFoundException {
        return delegate.getClassLoaderFor(mbeanName);
    }

    @Override
    public ClassLoader getClassLoader(final ObjectName loaderName) throws InstanceNotFoundException {
        return delegate.getClassLoader(loaderName);
    }

    @Override
    public ClassLoaderRepository getClassLoaderRepository() {
        return delegate.getClassLoaderRepository();
    }
}
