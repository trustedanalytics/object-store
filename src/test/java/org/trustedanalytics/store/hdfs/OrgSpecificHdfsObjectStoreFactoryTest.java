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

import org.apache.commons.io.IOUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;
import org.trustedanalytics.hadoop.config.client.AppConfiguration;
import org.trustedanalytics.hadoop.config.client.Configurations;

import java.io.IOException;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

@RunWith(MockitoJUnitRunner.class)
public class OrgSpecificHdfsObjectStoreFactoryTest {

    private OrgSpecificHdfsObjectStoreFactory setup(String testEnvFileName) throws IOException {
        String jsonSerializedEnv =
            IOUtils.toString(getClass().getResourceAsStream(testEnvFileName));
        AppConfiguration appConfiguration = Configurations.newInstanceFromJson(jsonSerializedEnv);
        return new OrgSpecificHdfsObjectStoreFactory(appConfiguration);
    }

    @Test
    public void getHdfsUri_uriFromBrokerHasTemplate_resolveOrganization() throws IOException {
        OrgSpecificHdfsObjectStoreFactory factory = setup("/env_with_template.json");

        String hdfsUri =
            factory.getHdfsUri(UUID.fromString("50a99746-e1ce-47a3-a293-2d16070319c2"));

        assertThat(hdfsUri, equalTo(
            "hdfs://localhost/org/50a99746-e1ce-47a3-a293-2d16070319c2/instances/1cfe7b45-1e07-4751-a853-78ef47a313cc/"));
    }

    @Test
    public void getHdfsUri_uriFromBrokerHaveNoTemplate_returnUriAsItIs() throws IOException {
        OrgSpecificHdfsObjectStoreFactory factory = setup("/env_without_template.json");

        String hdfsUri =
            factory.getHdfsUri(UUID.fromString("50a99746-e1ce-47a3-a293-2d16070319c2"));

        assertThat(hdfsUri, equalTo(
            "hdfs://localhost/org/single-org/instances/1cfe7b45-1e07-4751-a853-78ef47a313cc/"));
    }
}
