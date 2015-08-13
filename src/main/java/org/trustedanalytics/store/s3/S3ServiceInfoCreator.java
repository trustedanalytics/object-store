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

import org.springframework.cloud.cloudfoundry.CloudFoundryServiceInfoCreator;
import org.springframework.cloud.cloudfoundry.Tags;

import java.util.Map;

public class S3ServiceInfoCreator extends CloudFoundryServiceInfoCreator<S3ServiceInfo>{

    public S3ServiceInfoCreator() {
        super(new Tags("S3"));
    }

    @Override
    public S3ServiceInfo createServiceInfo(Map<String, Object> serviceData) {
        String id = (String) serviceData.get("name");
        Map<String, String> credentials = (Map<String, String>) serviceData.get("credentials");
        if (credentials == null) {
            throw new IllegalStateException("Could not get service credentials.");
        }
        String accessKey = credentials.get("access_key_id");
        String secretKey = credentials.get("secret_access_key");
        String bucket = credentials.get("bucket");
        return new S3ServiceInfo(id, accessKey, secretKey, bucket);
    }
}
