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

import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class HdfsObjectStoreTest {

    private static final String ID_PATTERN = "[0-9a-zA-Z_-]+-[0-9]{6}-[0-9]{6}$";
    private static final String ID_WITH_RANDOM_PATTERN = "[0-9a-zA-Z_-]+-[0-9]{6}-[0-9]{6}-[0-9a-zA-Z]{2}$";
    private static final String DEFAULT_FILE_NAME = "000000_1";
    private static final String CF_USER = "testCF";
    private static final String HIVE_USER = "hiveUSER";
    private static final String VCAP_USER = "vcapUSER";
    private static final ImmutableList<String> TECHNICAL_USERS = ImmutableList.of(CF_USER, HIVE_USER, VCAP_USER);
    private static final List<AclEntry> CF_VCAP_HIVE_EXECUTE_ACLS =
            FsPermissionHelper.getAclsForTechnicalUsers(TECHNICAL_USERS, FsAction.EXECUTE);

    private FileSystem fs = mock(FileSystem.class, RETURNS_DEEP_STUBS);
    private HdfsObjectStore obStore;

    private static final String URL = "hdfs://nameservice1/some_dir/";
    private static final Path PATH = new Path(URL);

    @Before public void setUpMocks() throws IOException {
        when(fs.getAclStatus(any(Path.class)).getEntries()).thenReturn(CF_VCAP_HIVE_EXECUTE_ACLS);
        obStore = new HdfsObjectStore(TECHNICAL_USERS, fs, new Path("homeFolder"));
    }

    @Test(expected = NoSuchElementException.class)
    public void remove_noExistingElements_throwsException() throws IOException {
        obStore.remove("NO_SUCH_ELEMENT");
    }

    @Test public void remove_existingElements_removed() throws IOException {
        when(fs.exists(any())).thenReturn(Boolean.TRUE);

        obStore.remove("EXISTING_ELEMENTS");
        verify(fs, times(1)).delete(any(Path.class), any(Boolean.class));
    }

    @Test public void save_element_fullpath() throws IOException {
        String dataSetName = "dataSetName";
        String id = obStore.save(new byte[0], dataSetName);
        Assert.assertThat(id, endsWith("000000_1"));
    }

    @Test public void saveObject_element_fullpath() throws IOException {
        String dataSetName = "dataSetName";
        ObjectId id = obStore.saveObject(new ByteArrayInputStream(new byte[0]), dataSetName);
        Assert.assertThat(id.getDirectoryName(), notNullValue());
        Assert.assertThat(id.getFileName(), endsWith("000000_1"));
    }

    @Test public void remove_oldObjectId_parentFolderIsSafe() throws IOException {
        when(fs.exists(any())).thenReturn(Boolean.TRUE);

        obStore.remove("generatedId/000000_1");
        obStore.remove("generatedId");
        verify(fs, times(2)).delete(eq(new Path("homeFolder/generatedId")), any(Boolean.class));
    }

    @Test (expected=IllegalArgumentException.class)
    public void remove_smartHacker_parentFolderIsSafe() throws IOException {
        when(fs.exists(any())).thenReturn(Boolean.TRUE);

        obStore.remove("/000000_1");
    }

    @Test
    public void getUniqueId_notUniquePath_generatePostfix() throws IOException {
        //Arrange
        final String expectedFileName = "/" + DEFAULT_FILE_NAME;
        //File exists at first attempt, so method will generate random postfix
        when(fs.exists(any())).thenReturn(Boolean.TRUE).thenReturn(Boolean.FALSE);
        //Act
        final ObjectId objectId = obStore.getUniqueId("dataSetName");
        //Assert
        assertTrue(objectId.getDirectoryName().matches(ID_WITH_RANDOM_PATTERN));
        assertEquals(expectedFileName, objectId.getFileName());
    }

    @Test
    public void getUniqueId_uniquePath_doNotGeneratePostfix() throws IOException {
        //Arrange
        final String expectedFileName = "/" + DEFAULT_FILE_NAME;
        when(fs.exists(any())).thenReturn(Boolean.FALSE);
        //Act
        final ObjectId objectId = obStore.getUniqueId("dataSetName");
        //Assert
        assertTrue(objectId.getDirectoryName().matches(ID_PATTERN));
        assertEquals(expectedFileName, objectId.getFileName());
    }
}
