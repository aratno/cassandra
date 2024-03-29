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

package org.apache.cassandra.distributed.util;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.CopyOption;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFileAttributes;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.jacoco.core.internal.data.CRC64;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.distributed.shared.Versions;

/**
 * TODO: Document this.
 */
public class JacocoClassIdFiltering
{
    private static final Logger logger = LoggerFactory.getLogger(JacocoClassIdFiltering.class);

    private static final Path JACOCO_CLASSDUMP = Paths.get("build/jacoco/classdump");
    private static final Path JACOCO_EXCLUSION_CLASSDUMP = Paths.get("build/jacoco/exclusionclassdump");
    static {
        new File(JACOCO_EXCLUSION_CLASSDUMP.toUri()).mkdir();
    }

    public static void main(String[] args) throws IOException, URISyntaxException
    {
        final URL[] classpaths = Versions.getClassPath();

        logger.info("Starting with classpath: {}", List.of(classpaths));

        Set<Path> keepClassesWithIds = getLocalClassesWithJacocoIds(classpaths);

        removedDumpedClassesExcept(classpaths, keepClassesWithIds);
    }

    private static void removedDumpedClassesExcept(URL[] classpaths, Set<Path> keepClassesWithIds) throws IOException
    {
        Files.walkFileTree(JACOCO_CLASSDUMP, new FileVisitor<>()
        {
            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException
            {
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException
            {
                if (!file.toFile().getName().endsWith(".class"))
                    return FileVisitResult.CONTINUE;

                // org/apache/cassandra/Klass.0123456789.class
                Path classdumpWithId = JACOCO_CLASSDUMP.relativize(file);

                if (keepClassesWithIds.contains(classdumpWithId))
                    logger.info("Keeping classdump for {}", classdumpWithId);
                else
                {
                    logger.info("Removing classdump for {}: {}", classdumpWithId, file.toAbsolutePath());
                    Files.createDirectories(JACOCO_EXCLUSION_CLASSDUMP.resolve(classdumpWithId).getParent());
                    Files.move(file, JACOCO_EXCLUSION_CLASSDUMP.resolve(classdumpWithId));
                }

                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException
            {
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException
            {
                return FileVisitResult.CONTINUE;
            }
        });
    }

    private static Set<Path> getLocalClassesWithJacocoIds(URL[] classpaths) throws IOException, URISyntaxException
    {
        final URL[] localclasses = Arrays.stream(classpaths).filter(url -> {
            try
            {
                return Paths.get(url.toURI()).toFile().isDirectory();
            }
            catch (URISyntaxException e)
            {
                return false;
            }
        }).toArray(URL[]::new);
        logger.debug("Local classes: {}", List.of(localclasses));

        Set<Path> paths = new HashSet<>();
        try (URLClassLoader classloader = new URLClassLoader(classpaths))
        {
            for (URL localclassdir : List.of(localclasses))
            {
                logger.debug("Checking local classes: {}", localclassdir);

                Files.walkFileTree(Paths.get(localclassdir.toURI()), new FileVisitor<>()
                {
                    @Override
                    public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException
                    {
                        return FileVisitResult.CONTINUE;
                    }

                    @Override
                    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException
                    {
                        if (!file.toFile().getName().endsWith(".class"))
                            return FileVisitResult.CONTINUE;

                        // org/apache/cassandra/Klass.class
                        String classDiskName = file.toAbsolutePath().toString().substring(localclassdir.getFile().length());

                        // org.apache.cassandra.Klass
                        String className = classDiskName
                                           .replace('/', '.')
                                           .replace(".class", "");

                        logger.debug("Got local class: {} {}", file, className);

                        byte[] classContent;
                        try (InputStream classContentStream = classloader.getResourceAsStream(classDiskName))
                        {
                            assert classContentStream != null;
                            classContent = classContentStream.readAllBytes();
                        }

                        // org/apache/cassandra/Klass.0123456789.class
                        final String classWithPathAndId = String.format("%s.%016x.class", className.replace('.', '/'), CRC64.classId(classContent));
                        logger.debug("Got classId {} for class {}", classWithPathAndId, className);
                        paths.add(Paths.get(classWithPathAndId));
                        return FileVisitResult.CONTINUE;
                    }

                    @Override
                    public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException
                    {
                        return FileVisitResult.CONTINUE;
                    }

                    @Override
                    public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException
                    {
                        return FileVisitResult.CONTINUE;
                    }
                });
            }
        }

        return paths;
    }
}
