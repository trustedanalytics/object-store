/**
 * Copyright (c) 2015 Intel Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.trustedanalytics.store.hdfs;

import org.trustedanalytics.store.hdfs.fs.FsPermissionHelper;

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatcher;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.endsWith;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class OrgSpecificHdfsObjectStoreTest {

    private static final String CF_USER = "testCF";
    private static final String HIVE_USER = "hiveUSER";
    private static final String VCAP_USER = "vcapUSER";
    private static final ImmutableList<String> TECHNICAL_USERS = ImmutableList.of(CF_USER, HIVE_USER, VCAP_USER);
    private static final List<AclEntry> CF_VCAP_HIVE_EXECUTE_ACLS =
            FsPermissionHelper.getAclsForTechnicalUsers(TECHNICAL_USERS, FsAction.EXECUTE);
    private static final String URL = "hdfs://nameservice1/some_dir/";
    private static final Path PATH = new Path(URL);
    private static final String UUID_PATTERN = "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$";

    private FileSystem fs = mock(FileSystem.class, RETURNS_DEEP_STUBS);

    @Test
    public void new_directoryExists_nothingSpecial() throws IOException {
        when(fs.exists(PATH)).thenReturn(true);

        new OrgSpecificHdfsObjectStore(TECHNICAL_USERS, fs, URL);

        verify(fs, atLeastOnce()).exists(PATH);
    }

    @Test
    public void new_directoryNotExist_dirMadePermissionSetAclSet() throws IOException {
        when(fs.exists(PATH)).thenReturn(false);
        when(fs.getFileStatus(PATH).getPermission()).thenReturn(FsPermissionHelper.permission770);
        when(fs.getAclStatus(PATH).getEntries()).thenReturn(CF_VCAP_HIVE_EXECUTE_ACLS);

        new OrgSpecificHdfsObjectStore(TECHNICAL_USERS, fs, URL);

        verify(fs, atLeastOnce()).mkdirs(PATH);
        verify(fs, atLeastOnce()).setPermission(PATH, FsPermissionHelper.permission770);
        verify(fs, atLeastOnce()).modifyAclEntries(PATH, CF_VCAP_HIVE_EXECUTE_ACLS);
    }

    @Test(expected = IOException.class)
    public void new_directoryNotExistAndErrorDuringMkdirs_exceptionRethrown() throws IOException {
        when(fs.exists(PATH)).thenReturn(false);
        when(fs.mkdirs(PATH)).thenThrow(new IOException());

        new OrgSpecificHdfsObjectStore(TECHNICAL_USERS, fs, URL);
    }

    @Test(expected = IOException.class)
    public void new_directoryNotExistSetPermissionError_exceptionRethrown() throws IOException {
        when(fs.exists(PATH)).thenReturn(false);
        doThrow(new IOException()).when(fs).setPermission(PATH, FsPermissionHelper.permission770);

        new OrgSpecificHdfsObjectStore(TECHNICAL_USERS, fs, URL);
    }

    @Test(expected = IOException.class)
    public void new_directoryNotExistSetPermissionFail_exceptionThrown() throws IOException {
        when(fs.exists(PATH)).thenReturn(false);
        when(fs.getFileStatus(PATH).getPermission()).thenReturn(FsPermission.getDefault());

        new OrgSpecificHdfsObjectStore(TECHNICAL_USERS, fs, URL);
    }

    @Test(expected = IOException.class)
    public void new_directoryNotExistSetAclsError_exceptionRethrown() throws IOException {
        when(fs.exists(PATH)).thenReturn(false);
        when(fs.getFileStatus(PATH).getPermission()).thenReturn(FsPermissionHelper.permission770);
        doThrow(new IOException()).when(fs).modifyAclEntries(PATH, CF_VCAP_HIVE_EXECUTE_ACLS);

        new OrgSpecificHdfsObjectStore(TECHNICAL_USERS, fs, URL);
    }

    @Test
    public void save_inputBytesGiven_directoryCreatedWithProperACLs() throws IOException {
        when(fs.exists(PATH)).thenReturn(true);
        when(fs.getAclStatus(startsWith(PATH)).getEntries()).thenReturn(CF_VCAP_HIVE_EXECUTE_ACLS);

        OrgSpecificHdfsObjectStore objectStore = new OrgSpecificHdfsObjectStore(TECHNICAL_USERS, fs, URL);

        String objectId = objectStore.save(new byte[0]);
        assertThat(objectId, endsWith("000000_1"));

        verify(fs).modifyAclEntries(pathValidAndStartsWith(URL),
                eq(FsPermissionHelper.getAclsForTechnicalUsers(TECHNICAL_USERS, FsAction.READ_EXECUTE)));
    }

    private Path startsWith(Path expectedPrefix) {
        return argThat(new ArgumentMatcher<Path>() {
            @Override public boolean matches(Object actual) {
                Path actualPath = (Path) actual;
                String path = actualPath.toString();
                return path.startsWith(expectedPrefix.toString());
            }
        });
    }

    private Path pathValidAndStartsWith(String text) {
        return argThat(new ArgumentMatcher<Path>() {
            @Override public boolean matches(Object actual) {
                Path actualPath = (Path) actual;
                String path = actualPath.toString();
                return path.startsWith(text) && path.matches(".*/" + UUID_PATTERN);
            }
        });
    }
}
