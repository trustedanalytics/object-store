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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import static org.mockito.Mockito.*;

import org.mockito.runners.MockitoJUnitRunner;
import org.trustedanalytics.hadoop.config.ConfigurationHelper;
import org.trustedanalytics.hadoop.config.ConfigurationLocator;
import org.trustedanalytics.hadoop.config.PropertyLocator;
import org.trustedanalytics.hadoop.kerberos.KrbLoginManager;
import org.trustedanalytics.hadoop.kerberos.KrbLoginManagerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@RunWith(MockitoJUnitRunner.class)
public class FileSystemFactoryImplTest {

  FileSystemFactory fileSystemFactory;

  @Mock
  ConfigurationHelper configurationHelper;

  @Mock
  KrbLoginManagerFactory loginManagerFactory;

  @Mock
  KrbLoginManager loginManager;

  @Before
  public void setUp() throws Exception {
    fileSystemFactory = new FileSystemFactoryImpl(configurationHelper, loginManagerFactory);
    when(configurationHelper.getPropertyFromEnv(PropertyLocator.HDFS_URI))
        .thenReturn(Optional.of("/tmp/some_dir/"));
    when(configurationHelper.getPropertyFromEnv(PropertyLocator.KRB_KDC))
        .thenReturn(Optional.of("kdc.host.addr"));
    when(configurationHelper.getPropertyFromEnv(PropertyLocator.KRB_REALM))
        .thenReturn(Optional.of("SOME_REALM"));
    when(configurationHelper.getPropertyFromEnv(PropertyLocator.USER))
        .thenReturn(Optional.of("user_name"));
    when(configurationHelper.getPropertyFromEnv(PropertyLocator.PASSWORD))
        .thenReturn(Optional.of("password"));

    when(loginManagerFactory.getKrbLoginManagerInstance("kdc.host.addr", "SOME_REALM"))
        .thenReturn(loginManager);

  }

  @Test
  public void testGetFileSystem_setAuthMethodKerbers_authenticate() throws Exception {
    //given
    Map<String, String> hadoopConfigProps = new HashMap<>();
    hadoopConfigProps.put("hadoop.security.authentication", "kerberos");

    when(configurationHelper.getConfigurationFromEnv(ConfigurationLocator.HADOOP))
        .thenReturn(hadoopConfigProps);

    //when
    FileSystem actual = fileSystemFactory.getFileSystem();

    //then
    verify(loginManager).loginInHadoop(anyObject(), anyObject());
    Assert.assertTrue(actual.getScheme().equals("file"));
  }

  @Test
  public void testGetFileSystem_setAuthMethodSimple_noAuthentication() throws Exception {
    //given
    Map<String, String> hadoopConfigProps = new HashMap<>();
    hadoopConfigProps.put("hadoop.security.authentication", "simple");

    when(configurationHelper.getConfigurationFromEnv(ConfigurationLocator.HADOOP))
        .thenReturn(hadoopConfigProps);

    //when
    FileSystem actual = fileSystemFactory.getFileSystem();

    //then
    Assert.assertTrue(actual.getScheme().equals("file"));

  }

  @Test
  public void testGetPath_setHdfsUri_returnPath() throws Exception {
    //when
    Path actual = fileSystemFactory.getChrootedPath();

    //then
    Assert.assertTrue(actual.toString().equals("/tmp/some_dir"));
  }

}