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

import org.springframework.cloud.service.BaseServiceInfo;

public class S3ServiceInfo extends BaseServiceInfo {

    private final String accessKey;
    private final String secretKey;
    private final String bucket;

    public S3ServiceInfo(String id, String accessKey, String secretKey, String bucket) {
        super(id);
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.bucket = bucket;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public String getSecretKey() {
        return secretKey;
    }

    public String getBucket() {
        return bucket;
    }

}
