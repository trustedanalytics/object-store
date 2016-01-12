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

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.cloud.Cloud;
import org.springframework.cloud.CloudFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;
import org.trustedanalytics.hadoop.config.client.Configurations;
import org.trustedanalytics.store.hdfs.FileSystemFactoryImpl;
import org.trustedanalytics.store.hdfs.HdfsObjectStore;
import org.trustedanalytics.store.hdfs.OrgSpecificHdfsObjectStoreFactory;
import org.trustedanalytics.store.s3.S3ObjectStore;
import org.trustedanalytics.store.s3.S3ServiceInfo;

import javax.security.auth.login.LoginException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.Function;

@org.springframework.context.annotation.Configuration
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
    @Profile("hdfs")
    public ObjectStore hdfsObjectStore() throws IOException, InterruptedException,
        URISyntaxException, LoginException {
        FileSystemFactoryImpl fsFactory =
            new FileSystemFactoryImpl(Configurations.newInstanceFromEnv());
        FileSystem fs = fsFactory.getFileSystem();
        Path path = fsFactory.getChrootedPath();
        return new HdfsObjectStore(fs, path);
    }

    //autowire this, if you want token to be automatically acquired from web context
    @Bean
    @Profile("multitenant-hdfs")
    public Function<UUID, ObjectStore> multitenantHdfsObjectStoreSupplier() throws IOException {
        OrgSpecificHdfsObjectStoreFactory factory = new OrgSpecificHdfsObjectStoreFactory();
        return factory::create;
    }

    //autowire this, if you want to provide token with your own logic
    @Bean
    @Profile("multitenant-hdfs")
    public BiFunction<UUID, String, ObjectStore> multitenantHdfsObjectStoreFactory()
        throws IOException {
        OrgSpecificHdfsObjectStoreFactory factory = new OrgSpecificHdfsObjectStoreFactory();
        return factory::create;
    }
}
