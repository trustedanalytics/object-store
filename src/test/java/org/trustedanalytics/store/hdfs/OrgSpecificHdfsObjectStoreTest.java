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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatcher;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.util.Arrays;

import static org.hamcrest.Matchers.endsWith;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class OrgSpecificHdfsObjectStoreTest {

    private static final String CF_USER = "testCF";
    private static final String HIVE_USER = "hiveUSER";
    private static final String URL = "hdfs://nameservice1/some_dir/";
    private static final Path PATH = new Path(URL);

    private FileSystem fs = mock(FileSystem.class, RETURNS_DEEP_STUBS);

    @Test
    public void createOrgSpecificHdfsObjectStore_directoryExists_nothingSpecial()
        throws IOException {

        when(fs.exists(PATH)).thenReturn(true);

        new OrgSpecificHdfsObjectStore(CF_USER, HIVE_USER, fs, URL);

        verify(fs).exists(PATH);
    }

    @Test
    public void createOrgSpecificHdfsObjectStore_directoryNotExist_directoryCreated()
        throws IOException {

        when(fs.exists(PATH)).thenReturn(false);
        when(fs.getFileStatus(PATH).getPermission()).thenReturn(null);

        new OrgSpecificHdfsObjectStore(CF_USER, HIVE_USER, fs, URL);

        verify(fs).exists(PATH);
        verify(fs).mkdirs(PATH, FsPermissionHelper.getPermission770());
        verify(fs).setPermission(PATH, FsPermissionHelper.getPermission770());
        verify(fs).modifyAclEntries(PATH,
            FsPermissionHelper.getAclsForTechnicalUsers(Arrays.asList(CF_USER, HIVE_USER), FsAction.EXECUTE));
    }

    @Test(expected = IOException.class)
    public void createOrgSpecificHdfsObjectStore_fileSystemThrowsException_exceptionRethrown()
        throws IOException {

        when(fs.exists(PATH)).thenThrow(IOException.class);
        new OrgSpecificHdfsObjectStore(CF_USER, HIVE_USER, fs, URL);
    }

    @Test
    public void save_inputBytesGiven_directoryCreatedWithProperACLs() throws IOException {

        OrgSpecificHdfsObjectStore objectStore =
            new OrgSpecificHdfsObjectStore(CF_USER, HIVE_USER, fs, URL);

        String objectId = objectStore.save(new byte[0]);
        assertThat(objectId, endsWith("000000_1"));

        verify(fs).modifyAclEntries(PATH,
            FsPermissionHelper.getAclsForTechnicalUsers(Arrays.asList(CF_USER, HIVE_USER), FsAction.EXECUTE));
        verify(fs).modifyAclEntries(pathValidAndStartsWith(URL),
            eq(FsPermissionHelper.getAclsForTechnicalUsers(Arrays.asList(CF_USER, HIVE_USER), FsAction.READ_EXECUTE)));
    }

    private Path pathValidAndStartsWith(String text) {
        return argThat(new ArgumentMatcher<Path>() {
            @Override public boolean matches(Object actual) {
                Path actualPath = (Path) actual;
                String path = actualPath.toString();
                return path.startsWith(text) && path.matches(".*/" + uuidPattern());
            }

            private String uuidPattern() {
                return "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$";
            }
        });
    }
}
