package com.zegelin;

import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.Set;
import java.util.TreeSet;

public class JmxNames {

    public static void main(String[] args) throws IOException, MalformedObjectNameException {

        final JMXServiceURL serviceURL = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://localhost:7199/jmxrmi");

        final JMXConnector connector = JMXConnectorFactory.connect(serviceURL, null);

        final MBeanServerConnection serverConnection = connector.getMBeanServerConnection();

        final Set<ObjectName> objectNames = serverConnection.queryNames(new ObjectName("org.apache.cassandra.metrics:*"), null);

        final TreeSet<ObjectName> sortedObjectNames = new TreeSet<>(objectNames);

        sortedObjectNames.forEach(n -> System.out.printf("objectName(\"%s\")%n", n));
    }
}
