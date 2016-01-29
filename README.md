object-store
=========

It is library holding interface and implementations to manage downloadable objects.
Implementations include:
* Support for HDFS
* Support for multitenant HDFS (working as specific user, not as a technical one)
* Support for S3
* In-memory store 
* In-folder store

If you want to use ObjectStore, you need to set one of below spring profiles in your app:
* default
* s3
* hdfs
* multitenant-hdfs
