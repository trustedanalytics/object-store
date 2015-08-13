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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.NoSuchElementException;
import java.util.UUID;

import org.trustedanalytics.store.ObjectStore;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.apache.commons.lang.StringUtils;

public class HdfsObjectStore implements ObjectStore {

    public static final int BUF_SIZE = 4096;

    private static final String SAVED_DATASET_FILENAME = "/000000_1";

    private FileSystem hdfs;

    private Path chrootPath;

    public HdfsObjectStore(FileSystem hdfs, Path chrootPath) {
        this.hdfs = hdfs;
        this.chrootPath = chrootPath;
    }

    private String getRandomId() {
        return UUID.randomUUID().toString() + SAVED_DATASET_FILENAME;
    }

    @Override
    public String save(InputStream input) throws IOException {
        String objectId = getRandomId();
        Path path = idToPath(objectId);
        removePathIfExists(path);
        try (OutputStream os = getOutputStream(path)) {
            IOUtils.copyBytes(input, os, BUF_SIZE);
        }
        return objectId;
    }

    @Override
    public void remove(String objectId) throws IOException {
        Path path = idToDirectoryPath(objectId);
        if (path.equals(chrootPath)) {
            throw new IllegalArgumentException("objectId");
        }
        if (hdfs.exists(path)) {
            hdfs.delete(path, true);
        } else {
            throw new NoSuchElementException();
        }

    }

    private void removePathIfExists(Path path) throws IOException {
        if (hdfs.exists(path)) {
            hdfs.delete(path, true);
        }
    }

    private OutputStream getOutputStream(Path path) throws IOException {
        return hdfs.create(path, new Progressable() {
            @Override
            public void progress() {
                // TODO: callback
            }
        });
    }

    @Override
    public InputStream getContent(String objectId) throws IOException {
        return hdfs.open(idToPath(objectId));
    }

    @Override
    public String getId() {
        return chrootPath.toString();
    }

    private Path idToPath(String id) {
        return new Path(chrootPath + "/" + id);
    }

    private Path idToDirectoryPath(String id) {
        String directoryPath = StringUtils.removeEnd(id, SAVED_DATASET_FILENAME);
        return new Path(chrootPath + "/" + directoryPath);
    }
}
