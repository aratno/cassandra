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

package org.apache.cassandra.cql3.statements;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.ClientState;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
public class SelectStatementTest
{

    private static final String KEYSPACE = "ks";

    @BeforeClass
    public static void setupClass()
    {
        DatabaseDescriptor.daemonInitialization();
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE, KeyspaceParams.simple(1));
    }

    private static SelectStatement parseSelect(String query)
    {
        CQLStatement stmt = QueryProcessor.parseStatement(query).prepare(ClientState.forInternalCalls());
        assert stmt instanceof SelectStatement;
        return (SelectStatement) stmt;
    }

    @Test
    public void testNonsensicalBounds()
    {
        QueryProcessor.executeOnceInternal("CREATE TABLE ks.tbl (k int, c int, v int, primary key (k, c))");
        QueryProcessor.executeOnceInternal("INSERT INTO ks.tbl (k, c, v) VALUES (100, 10, 0)");
        Assert.assertEquals(Slices.NONE, parseSelect("SELECT * FROM ks.tbl WHERE k=100 AND c > 10 AND c <= 10").makeSlices(QueryOptions.DEFAULT));
        Assert.assertEquals(Slices.NONE, parseSelect("SELECT * FROM ks.tbl WHERE k=100 AND c < 10 AND c >= 10").makeSlices(QueryOptions.DEFAULT));
        Assert.assertEquals(Slices.NONE, parseSelect("SELECT * FROM ks.tbl WHERE k=100 AND c < 10 AND c > 10").makeSlices(QueryOptions.DEFAULT));
    }

    @Test(expected = UnauthorizedException.class)
    public void testNonSuperUserUnauthorized()
    {
        DatabaseDescriptor.setAllowNonSuperUserSelectSaltedHash(false);

        ClientState mockClientState = mock(ClientState.class);
        AuthenticatedUser mockUser = mock(AuthenticatedUser.class);
        when(mockUser.isSuper()).thenReturn(false);
        when(mockClientState.getUser()).thenReturn(mockUser);

        String query = "SELECT * FROM system_auth.roles;";
        SelectStatement selectStatement = parseSelect(query);
        selectStatement.authorize(mockClientState);
    }

    @Test
    public void testNonSuperUserAuthorized()
    {
        // Default: non-superusers can access column
        DatabaseDescriptor.setAllowNonSuperUserSelectSaltedHash(true);

        ClientState mockClientState = mock(ClientState.class);
        AuthenticatedUser mockUser = mock(AuthenticatedUser.class);
        when(mockUser.isSuper()).thenReturn(false);
        when(mockUser.getName()).thenReturn("test_user");
        when(mockClientState.getUser()).thenReturn(mockUser);

        String query = "SELECT * FROM system_auth.roles;";
        SelectStatement selectStatement = parseSelect(query);
        selectStatement.authorize(mockClientState);
    }

    @Test
    public void testReferencingSaltedHash()
    {
        String query1 = "SELECT role, can_login, is_superuser, member_of FROM system_auth.roles;";
        SelectStatement selectStatement1 = parseSelect(query1);
        Assert.assertFalse(selectStatement1.isReferencingSaltedHash());

        String query2 = "SELECT * FROM system_auth.roles;";
        SelectStatement selectStatement2 = parseSelect(query2);
        Assert.assertTrue(selectStatement2.isReferencingSaltedHash());

        String query3 = "SELECT salted_hash FROM system_auth.roles;";
        SelectStatement selectStatement3 = parseSelect(query3);
        Assert.assertTrue(selectStatement3.isReferencingSaltedHash());

        String query4 = "SELECT JSON * FROM system_auth.roles;";
        SelectStatement selectStatement4 = parseSelect(query4);
        Assert.assertTrue(selectStatement4.isReferencingSaltedHash());
    }
}
