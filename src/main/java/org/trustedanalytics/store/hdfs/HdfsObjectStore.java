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

import static org.apache.commons.lang.RandomStringUtils.randomAlphanumeric;

import com.google.common.collect.ImmutableList;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.trustedanalytics.store.ObjectStore;
import org.trustedanalytics.store.hdfs.fs.FsPermissionHelper;
import org.trustedanalytics.id.IdWithTimestamp;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

public class HdfsObjectStore implements ObjectStore {

    private static final Logger LOGGER = LoggerFactory.getLogger(HdfsObjectStore.class);
    private final static int RANDOM_ELEMENTS_COUNT = 2;

    public static final int BUF_SIZE = 4096;

    static final String SAVED_DATASET_FILENAME = "/000000_1";

    private FileSystem hdfs;

    private Path chrootPath;

    private final ImmutableList<String> technicalUsers;

    public HdfsObjectStore(ImmutableList<String> technicalUsers, FileSystem hdfs, Path chrootPath) {
        this.hdfs = hdfs;
        this.chrootPath = chrootPath;
        this.technicalUsers = technicalUsers;
    }

    public HdfsObjectStore(FileSystem hdfs, Path chrootPath) {
        this.hdfs = hdfs;
        this.chrootPath = chrootPath;
        this.technicalUsers = ImmutableList.of();
    }

    @Override
    public String save(InputStream input, String dataSetName) throws IOException {
        return saveObject(input, dataSetName).toString();
    }

    private ObjectId getIdWithTimestamp(String dataSetName) {
        return new ObjectId(IdWithTimestamp.generate(dataSetName).getId(), SAVED_DATASET_FILENAME);
    }

    ObjectId saveObject(InputStream input, String dataSetName) throws IOException {
        ObjectId objectId = createNewObjectDir(dataSetName);
        setAClsForTechnicalUsers(objectId);

        Path path = idToPath(objectId.toString());
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

    ObjectId getUniqueId(String dataSetName) throws IOException {
        ObjectId id = getIdWithTimestamp(dataSetName);
        Path path = new Path(idToPath(id.toString()).toString());
        String postfix = "";
        while(hdfs.exists(path)) {
            postfix += "-" + randomAlphanumeric(RANDOM_ELEMENTS_COUNT);
            path = new Path(path.toString() + postfix);
        }
        return new ObjectId(id.getDirectoryName() + postfix, id.getFileName());
    }

    private OutputStream getOutputStream(Path path) throws IOException {
        return hdfs.create(path, new Progressable() {
            @Override
            public void progress() {
                //intentionally left empty
            }
        });
    }

    public InputStream getContent(String objectId) throws IOException {
        return hdfs.open(idToPath(objectId));
    }

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

    ObjectId createNewObjectDir(String dataSetName) throws IOException {
        ObjectId uniqueId = getUniqueId(dataSetName);
        hdfs.mkdirs(idToDirectoryPath(uniqueId.toString()));
        return uniqueId;
    }

    private void setAClsForTechnicalUsers(ObjectId objectId) throws IOException {
        LOGGER.debug("setAClsForTechnicalUsers objectId = [" + objectId + "]");
        Path path = getPath(chrootPath, objectId.getDirectoryName());

        hdfs.modifyAclEntries(path,
                FsPermissionHelper.getAclsForTechnicalUsers(technicalUsers, FsAction.READ_EXECUTE));
        hdfs.modifyAclEntries(path,
                FsPermissionHelper.getDefaultAclsForTechnicalUsers(technicalUsers, FsAction.READ_EXECUTE));

        List<AclEntry> actualAcls = hdfs.getAclStatus(path).getEntries();
        LOGGER.info("ACLs for '" + objectId + "': " + aclsToString(actualAcls));
    }

    private Path getPath(Path basicPath, String objectDirName) {
        return new Path(basicPath + "/" + objectDirName);
    }

    private String aclsToString(List<AclEntry> requiredAcls) {
        return requiredAcls.stream().map(AclEntry::toString).collect(Collectors.joining(", "));
    }
}
