[![Build Status](https://travis-ci.org/trustedanalytics/object-store.svg?branch=master)](https://travis-ci.org/trustedanalytics/object-store)
[![Dependency Status](https://www.versioneye.com/user/projects/57236dc0ba37ce0031fc1e80/badge.svg?style=flat)](https://www.versioneye.com/user/projects/57236dc0ba37ce0031fc1e80)

object-store
=========

It is library holding interface and implementations to manage downloadable objects.
Implementations include:
* Support for HDFS
* Support for multitenant HDFS (working as specific user, not as a technical one)
* In-memory store 
* In-folder store

If you want to use ObjectStore, you need to set one of below spring profiles in your app:
* default
* hdfs
* multitenant-hdfs
