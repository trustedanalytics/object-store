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
package org.trustedanalytics.store.s3;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.InterruptedByTimeoutException;

import org.trustedanalytics.store.ObjectStore;
import org.springframework.beans.factory.annotation.Autowired;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.transfer.TransferManager;
import org.trustedanalytics.id.JobIdSupplier;

// TOODs:
// * use multipart uploads to obey 5 GB limit and loading all into memory
// * provide timeouts (there is risk that it will hung somewhere)
// * provide progress updates
public class S3ObjectStore implements ObjectStore {

    private static final String S3_FOLDER = "downloader/";

    private final AmazonS3 amazonS3;
    private final String bucket;
    private final TransferManager transferManager;
    private final JobIdSupplier jobIdSupplier;

    @Autowired
    public S3ObjectStore(AmazonS3 amazonS3, String bucket, JobIdSupplier jobIdSupplier) {
        this.amazonS3 = amazonS3;
        this.bucket = bucket;
        this.transferManager = new TransferManager(amazonS3);
        this.jobIdSupplier = jobIdSupplier;
    }

    @Override
    public String save(InputStream input, String dataSetName) throws IOException {
        String name = S3_FOLDER + jobIdSupplier.get(dataSetName);
        PutObjectRequest request = new PutObjectRequest(bucket, name, input, new ObjectMetadata());
        try {
            transferManager.upload(request).waitForUploadResult();
            return name;
        } catch (InterruptedException e) {
            throw new InterruptedByTimeoutException();
        }
    }

    @Override
    public InputStream getContent(String objectId) throws IOException {
        S3Object s3Object = amazonS3.getObject(bucket, objectId);
        return s3Object.getObjectContent();
    }

    @Override
    public void remove(String objectId) throws IOException {
        amazonS3.deleteObject(bucket, objectId);
    }

    @Override
    public String getId() {
        return "s3://" + bucket;
    }
}
