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

package org.apache.cassandra.anttasks;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.tools.ant.Project;
import org.apache.tools.ant.Task;
import org.jacoco.core.internal.data.CRC64;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>Jacoco produces two types of data at runtime:
 * <ol>
 *      <li>Execution data, tracking which methods and instructions were executed during runtime, configured with jacoco.partialexecfile
 *      <li>Classdump data, a dump of each class loaded while the Jacoco agent is running, configured with jacoco.classdump.dir
 * </ol>
 *
 * <p>At analysis time, Jacoco merges the execution data and classdump data to produce reports.
 *
 * <p>Classdumps are necessary for Cassandra because our test run-time environment may differ from our build environment,
 * especially with respect to bytecode interception used in our tests. Configuring a classdump is the best way to get
 * accurate data from Jacoco, but we currently do not configure a classdump for our python-dtest suite. For the
 * python-dtest suite, see the no-jacoco-prune system property in build.xml.
 *
 * <p> This class implements an Ant task to prune the classdump to only include the data we need. Specifically, it
 * removes all classes that were loaded but don't need coverage analysis (including dependency libraries, test classes,
 * and alternate jvm-dtest versions for upgrade tests).
 *
 * <p>Classdump data is stored in the package/Klass.id.class format, much like a build but with <a href="https://www.eclemma.org/jacoco/trunk/doc/classids.html">class IDs</a>.
 */
public class JacocoPruning extends Task
{
    private static final Logger logger = LoggerFactory.getLogger(JacocoPruning.class);

    @Override
    public void execute()
    {
        try
        {
            Project project = getProject();

            Path classdump = Paths.get(project.getProperty("jacoco.classdump.dir"));
            logger.info("Pruning Jacoco classdump at: {}", classdump);

            URL[] referenceClasspaths = Arrays.stream(project.getProperty("build.classes.main").split(","))
                    .map(JacocoPruning::convert).collect(Collectors.toList())
                    .toArray(new URL[]{});

            logger.info("Using reference classes from local build classpath: {}", List.of(referenceClasspaths));

            Set<Path> keepClassesWithIds = getLocalClassesWithJacocoIds(referenceClasspaths);

            logger.trace("Keeping classes: {}", keepClassesWithIds);
            if (keepClassesWithIds.isEmpty())
                throw new RuntimeException("Could not find any classes to keep. Have you run a build?");

            pruneClassdump(classdump, keepClassesWithIds);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    private static URL convert(String path)
    {
        try
        {
            return Paths.get(path).toUri().toURL();
        }
        catch (MalformedURLException e)
        {
            throw new RuntimeException(e);
        }
    }

    private void pruneClassdump(Path classdump, Set<Path> keepClassesWithIds) throws IOException
    {
        // Make a separate directory for excluded classes, rather than deleting them directly
        Path exclusionClassdump = classdump.getParent().resolve("exclclassdump");
        assert new File(exclusionClassdump.toUri()).mkdir();

        logger.info("Putting pruned classdump contents into {}", exclusionClassdump);

        class CountingPruner implements FileVisitor<Path>
        {
            public long kept = 0;
            public long pruned = 0;

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
                Path classdumpWithId = classdump.relativize(file);

                if (keepClassesWithIds.contains(classdumpWithId))
                {
                    logger.trace("Keeping classdump for {}", classdumpWithId);
                    kept++;
                }
                else
                {
                    logger.trace("Removing classdump for {}: {}", classdumpWithId, file.toAbsolutePath());
                    Files.createDirectories(exclusionClassdump.resolve(classdumpWithId).getParent());
                    Files.move(file, exclusionClassdump.resolve(classdumpWithId));
                    pruned++;
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
        };
        CountingPruner visitor = new CountingPruner();
        Files.walkFileTree(classdump, visitor);

        logger.info("Pruned {} files from classdump", visitor.pruned);
        logger.info("Kept {} files from classdump", visitor.kept);
    }

    private Set<Path> getLocalClassesWithJacocoIds(URL[] classpaths) throws IOException, URISyntaxException
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
        logger.trace("Local classes: {}", List.of(localclasses));

        Set<Path> paths = new HashSet<>();
        try (URLClassLoader classloader = new URLClassLoader(classpaths))
        {
            for (URL localclassdir : List.of(localclasses))
            {
                logger.trace("Checking local classes: {}", localclassdir);

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

                        logger.trace("Got local class: {} {}", file, className);

                        byte[] classContent;
                        try (InputStream classContentStream = classloader.getResourceAsStream(classDiskName))
                        {
                            assert classContentStream != null;
                            classContent = classContentStream.readAllBytes();
                        }

                        // org/apache/cassandra/Klass.0123456789.class
                        final String classWithPathAndId = String.format("%s.%016x.class", className.replace('.', '/'), CRC64.classId(classContent));
                        logger.trace("Got classId {} for class {}", classWithPathAndId, className);
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
