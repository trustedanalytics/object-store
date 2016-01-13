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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;

import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class OrgSpecificHdfsObjectStoreTest {

    private static final String URL = "hdfs://nameservice1/some_dir/";
    private static final Path PATH = new Path(URL);

    private FileSystem fs = mock(FileSystem.class, RETURNS_DEEP_STUBS);

    @Test
    public void createOrgSpecificHdfsObjectStore_directoryExists_nothingSpecial()
        throws IOException {

        when(fs.exists(PATH)).thenReturn(true);

        new OrgSpecificHdfsObjectStore(fs, URL);

        verify(fs).exists(PATH);
    }

    @Test
    public void createOrgSpecificHdfsObjectStore_directoryNotExist_directoryCreated()
        throws IOException {

        when(fs.exists(PATH)).thenReturn(false);
        when(fs.getFileStatus(PATH).getPermission()).thenReturn(null);

        new OrgSpecificHdfsObjectStore(fs, URL);

        verify(fs).exists(PATH);
        verify(fs).mkdirs(PATH, FsPermissionHelper.getPermission770());
        verify(fs).setPermission(PATH, FsPermissionHelper.getPermission770());
    }

    @Test(expected = IOException.class)
    public void createOrgSpecificHdfsObjectStore_fileSystemThrowsException_exceptionRethrown()
        throws IOException {

        when(fs.exists(PATH)).thenThrow(IOException.class);
        new OrgSpecificHdfsObjectStore(fs, URL);
    }
}
