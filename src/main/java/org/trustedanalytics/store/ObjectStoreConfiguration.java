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

import org.trustedanalytics.store.hdfs.HdfsObjectStore;
import org.trustedanalytics.store.s3.S3ObjectStore;
import org.trustedanalytics.store.s3.S3ServiceInfo;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import org.trustedanalytics.utils.hdfs.EnableHadoop;
import org.trustedanalytics.utils.hdfs.HdfsConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.Cloud;
import org.springframework.cloud.CloudFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import java.io.IOException;
import java.net.URISyntaxException;

@EnableHadoop
@Configuration
public class ObjectStoreConfiguration {

    @Bean
    @Profile("default")
    public ObjectStore objectStore() {
        return new InFolderObjectStore(System.getenv("DOWNLOADS_DIR"));
    }

    @Bean
    @Profile("s3")
    public ObjectStore s3ObjectStore() {
        CloudFactory cloudFactory = new CloudFactory();
        Cloud cloud = cloudFactory.getCloud();
        S3ServiceInfo s3ServiceInfo = (S3ServiceInfo) cloud.getServiceInfo("S3-serv-instance");
        AWSCredentials awsCredentials =
                new BasicAWSCredentials(s3ServiceInfo.getAccessKey(), s3ServiceInfo.getSecretKey());
        AmazonS3 amazonS3 = new AmazonS3Client(awsCredentials);
        return new S3ObjectStore(amazonS3, s3ServiceInfo.getBucket());
    }

    @Bean
    @Profile({"hdfs", "insecure", "secure"})
    public ObjectStore hdfsObjectStore(HdfsConfig hdfsConf) throws IOException, InterruptedException,
            URISyntaxException {
        return new HdfsObjectStore(hdfsConf.getFileSystem(), hdfsConf.getPath());
    }
}
