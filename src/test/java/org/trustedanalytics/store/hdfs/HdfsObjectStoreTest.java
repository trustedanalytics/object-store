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

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import static org.hamcrest.Matchers.endsWith;

import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.FsAction;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.trustedanalytics.store.hdfs.fs.FsPermissionHelper;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;

import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;

public class HdfsObjectStoreTest {

    private static final String CF_USER = "testCF";
    private static final String HIVE_USER = "hiveUSER";
    private static final String VCAP_USER = "vcapUSER";
    private static final ImmutableList<String> TECHNICAL_USERS = ImmutableList.of(CF_USER, HIVE_USER, VCAP_USER);
    private static final List<AclEntry> CF_VCAP_HIVE_EXECUTE_ACLS =
            FsPermissionHelper.getAclsForTechnicalUsers(TECHNICAL_USERS, FsAction.EXECUTE);

    private FileSystem fs = mock(FileSystem.class, RETURNS_DEEP_STUBS);
    private HdfsObjectStore ob_store;

    private static final String URL = "hdfs://nameservice1/some_dir/";
    private static final Path PATH = new Path(URL);

    @Before public void setUpMocks() throws IOException {
        when(fs.getAclStatus(any(Path.class)).getEntries()).thenReturn(CF_VCAP_HIVE_EXECUTE_ACLS);
        ob_store = new HdfsObjectStore(TECHNICAL_USERS, fs, new Path("homeFolder"));
    }

    @Test(expected = NoSuchElementException.class)
    public void remove_noExistingElements_throwsException() throws IOException {
        ob_store.remove("NO_SUCH_ELEMENT");
    }

    @Test public void remove_existingElements_removed() throws IOException {
        when(fs.exists(any())).thenReturn(Boolean.TRUE);

        ob_store.remove("EXISTING_ELEMENTS");
        verify(fs, times(1)).delete(any(Path.class), any(Boolean.class));
    }

    @Test public void save_element_fullpath() throws IOException {
        String id = ob_store.save(new byte[0]);
        Assert.assertThat(id, endsWith("000000_1"));
    }

    @Test public void saveObject_element_fullpath() throws IOException {
        ObjectId id = ob_store.saveObject(new ByteArrayInputStream(new byte[0]));
        Assert.assertThat(id.getDirectoryName(), notNullValue());
        Assert.assertThat(id.getFileName(), endsWith("000000_1"));
    }

    @Test public void remove_oldObjectId_parentFolderIsSafe() throws IOException {
        when(fs.exists(any())).thenReturn(Boolean.TRUE);

        ob_store.remove("generatedId/000000_1");
        ob_store.remove("generatedId");
        verify(fs, times(2)).delete(eq(new Path("homeFolder/generatedId")), any(Boolean.class));
    }

    @Test (expected=IllegalArgumentException.class)
    public void remove_smartHacker_parentFolderIsSafe() throws IOException {
        when(fs.exists(any())).thenReturn(Boolean.TRUE);

        ob_store.remove("/000000_1");
    }
}
