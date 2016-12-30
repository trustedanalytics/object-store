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
package org.trustedanalytics.store;

import org.trustedanalytics.id.IdWithTimestamp;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;

public class InFolderObjectStore implements ObjectStore {

    private File folder;

    public InFolderObjectStore(String folder) {
        this.folder = new File(folder);
    }

    @Override
    public String save(InputStream input, String dataSetName) throws IOException {
        File file = new File(folder, IdWithTimestamp.generate(dataSetName).getId());
        Files.copy(input, file.toPath());
        return file.getName();
    }

    @Override
    public InputStream getContent(String objectId) throws IOException {
        return new FileInputStream(new File(folder, objectId));
    }

    @Override
    public String getId() {
        return "file://" + folder.getAbsolutePath() + "/";
    }
}
