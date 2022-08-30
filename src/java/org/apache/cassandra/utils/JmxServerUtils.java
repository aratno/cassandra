/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.utils;

import com.sun.jmx.remote.security.MBeanServerAccessController;
import org.apache.cassandra.config.CassandraJmxSecurityProfile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanException;
import javax.management.ObjectName;
import javax.management.ReflectionException;

public class JmxServerUtils
{
    private static Logger logger = LoggerFactory.getLogger(JmxServerUtils.class);

    private static final String CASSANDRA_JMX_SECURITY_PROFILE = "cassandra.jmx.security.profile";

    public static CassandraJmxSecurityProfile getSecurityProfile()
    {
        try
        {
            return CassandraJmxSecurityProfile.convert(System.getProperty(CASSANDRA_JMX_SECURITY_PROFILE));
        } catch (IllegalArgumentException iae)
        {
            logger.warn("Invalid value for {} provided, using default", CASSANDRA_JMX_SECURITY_PROFILE, iae);
            return CassandraJmxSecurityProfile.getDefault();
        }
    }

    public static class CassandraMBeanServerAccessController extends MBeanServerAccessController
    {
        @Override
        protected void checkRead()
        {}

        @Override
        protected void checkWrite()
        {}

        // This is taken from MBeanServerAccessController, with additional security checks
        // For a longer list of JMX-based vulnerabilities, see:
        // https://github.com/qtc-de/beanshooter/blob/2ec4f7a4b44a29f52315973fe944eb34bc772063/beanshooter/src/de/qtc/beanshooter/mbean/diagnostic/Dispatcher.java#L48
        // Some vulnerabilities depend on mechanisms that are not present in Java 8, like CompilerDirectiveAdd and JvmtiLoad
        @Override
        public Object invoke(ObjectName name, String operationName, Object params[], String signature[]) throws InstanceNotFoundException, MBeanException, ReflectionException
        {
            checkWrite();

            if (JmxServerUtils.getSecurityProfile() == CassandraJmxSecurityProfile.RESTRICTIVE)
            {
                // Loading arbitrary (JVM and native) libraries from remotes
                checkMLetMethods(name, operationName);
            }

            return getMBeanServer().invoke(name, operationName, params, signature);
        }

        private void checkMLetMethods(ObjectName name, String operation)
                throws InstanceNotFoundException
        {
            // Inspired by MBeanServerAccessController, but that class ignores check if a SecurityManager is installed,
            // which we don't want

            if (!operation.equals("addURL") && !operation.equals("getMBeansFromURL"))
                return;

            if (!getMBeanServer().isInstanceOf(name, "javax.management.loading.MLet"))
                return;

            if (operation.equals("addURL") || operation.equals("getMBeansFromURL"))
                throw new SecurityException("Access is denied!");
        }
    }
}
