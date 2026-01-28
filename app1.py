[cdprct@cdp-gateway-01-rct ~]$ hdfs dfs -ls hdfs://nameservice1/raw/ebk_web_device_history/16-Jan-2026/ebk_web_device_history_20250501.txt
-rw-r--r--   1 cloudera-scm supergroup    3530244 2026-01-16 16:08 hdfs://nameservice1/raw/ebk_web_device_history/16-Jan-2026/ebk_web_device_history_20250501.txt
[cdprct@cdp-gateway-01-rct ~]$ hdfs dfs -cp hdfs://nameservice1/raw/ebk_web_device_history/16-Jan-2026/ebk_web_device_history_20250501.txt hdfs://nameservice1/raw/ebk_web_device_history/16-Jan-2026/ebk_web_device_history_20250502.txt
[cdprct@cdp-gateway-01-rct ~]$ cd /opt/workspace/Script/CEKO/hcomp/lib/bin
[cdprct@cdp-gateway-01-rct bin]$ ./hcompressor

usage: hcompressor [-h] [--skip_extension SKIP_EXTENSION]
                   [--only_extension ONLY_EXTENSION] [--trust {0,1,2}]
                   [--logs_file LOGS_FILE] [--silent [SILENT]]
                   [--compression_type {bz2,zstd,gz}] [-r [RECURSIVE]]
                   [--mode {compression,decompression}]
                   [--delete_input [DELETE_INPUT]]
                   input_path output_path
hcompressor: error: the following arguments are required: input_path, output_path
[cdprct@cdp-gateway-01-rct bin]$ ./hcompressor ^C
[cdprct@cdp-gateway-01-rct bin]$ ./hcompressor hdfs://nameservice1/raw/ebk_web_device_history/16-Jan-2026/ebk_web_device_history_20250502.txt ^C
[cdprct@cdp-gateway-01-rct bin]$ ./hcompressor hdfs://nameservice1/raw/ebk_web_device_history/16-Jan-2026/ebk_web_device_history_20250502.txt  hdfs://nameservice1/raw/ebk_web_device_history/16-Jan-2026

3314085 | 2026-01-28 13:35:02,066 | hcompressor | INFO | Arguments parsed successfully
3314085 | 2026-01-28 13:35:13,936 | hcompressor | INFO | Files to compress : 1
3314085 | 2026-01-28 13:35:13,937 | hcompressor | INFO | Compressing hdfs://nameservice1/raw/ebk_web_device_history/16-Jan-2026/ebk_web_device_history_20250502.txt to hdfs://nameservice1/raw/ebk_web_device_history/16-Jan-2026/ebk_web_device_history_20250502.txt.zstd
-put: java.net.URISyntaxException: Expected scheme-specific part at index 5: hdfs:
Usage: hadoop fs [generic options]
        [-appendToFile <localsrc> ... <dst>]
        [-cat [-ignoreCrc] <src> ...]
        [-checksum <src> ...]
        [-chgrp [-R] GROUP PATH...]
        [-chmod [-R] <MODE[,MODE]... | OCTALMODE> PATH...]
        [-chown [-R] [OWNER][:[GROUP]] PATH...]
        [-copyFromLocal [-f] [-p] [-l] [-d] [-t <thread count>] <localsrc> ... <dst>]
        [-copyToLocal [-f] [-p] [-ignoreCrc] [-crc] <src> ... <localdst>]
        [-count [-q] [-h] [-v] [-t [<storage type>]] [-u] [-x] [-e] [-s] <path> ...]
        [-cp [-f] [-p | -p[topax]] [-d] <src> ... <dst>]
        [-createSnapshot <snapshotDir> [<snapshotName>]]
        [-deleteSnapshot <snapshotDir> <snapshotName>]
        [-df [-h] [<path> ...]]
        [-du [-s] [-h] [-v] [-x] <path> ...]
        [-expunge [-immediate]]
        [-find <path> ... <expression> ...]
        [-get [-f] [-p] [-ignoreCrc] [-crc] <src> ... <localdst>]
        [-getfacl [-R] <path>]
-cat: java.net.URISyntaxException: Expected scheme-specific part at index 5: hdfs:
Usage: hadoop fs [generic options]
        [-appendToFile <localsrc> ... <dst>]
        [-cat [-ignoreCrc] <src> ...]
        [-checksum <src> ...]
        [-getfattr [-R] {-n name | -d} [-e en] <path>]
        [-chgrp [-R] GROUP PATH...]
        [-getmerge [-nl] [-skip-empty-file] <src> <localdst>]   [-chmod [-R] <MODE[,MODE]... | OCTALMODE> PATH...]
        [-chown [-R] [OWNER][:[GROUP]] PATH...]

        [-copyFromLocal [-f] [-p] [-l] [-d] [-t <thread count>] <localsrc> ... <dst>]
        [-head <file>]  [-copyToLocal [-f] [-p] [-ignoreCrc] [-crc] <src> ... <localdst>]

        [-help [cmd ...]]
        [-count [-q] [-h] [-v] [-t [<storage type>]] [-u] [-x] [-e] [-s] <path> ...]
        [-cp [-f] [-p | -p[topax]] [-d] <src> ... <dst>]
        [-createSnapshot <snapshotDir> [<snapshotName>]]
        [-ls [-C] [-d] [-h] [-q] [-R] [-t] [-S] [-r] [-u] [-e] [<path> ...]]
        [-deleteSnapshot <snapshotDir> <snapshotName>]
        [-df [-h] [<path> ...]]
        [-mkdir [-p] <path> ...]        [-du [-s] [-h] [-v] [-x] <path> ...]

        [-expunge [-immediate]] [-moveFromLocal [-f] [-p] [-l] [-d] <localsrc> ... <dst>]

        [-moveToLocal <src> <localdst>]
        [-mv <src> ... <dst>]
        [-put [-f] [-p] [-l] [-d] [-t <thread count>] <localsrc> ... <dst>]
        [-renameSnapshot <snapshotDir> <oldName> <newName>]
        [-find <path> ... <expression> ...]
        [-rm [-f] [-r|-R] [-skipTrash] [-safely] <src> ...]
        [-get [-f] [-p] [-ignoreCrc] [-crc] <src> ... <localdst>]
        [-rmdir [--ignore-fail-on-non-empty] <dir> ...]
        [-getfacl [-R] <path>]
        [-setfacl [-R] [{-b|-k} {-m|-x <acl_spec>} <path>]|[--set <acl_spec> <path>]]
        [-setfattr {-n name [-v value] | -x name} <path>]
        [-setrep [-R] [-w] <rep> <path> ...]
        [-stat [format] <path> ...]
        [-tail [-f] [-s <sleep interval>] <file>]
        [-test -[defsz] <path>]
        [-text [-ignoreCrc] <src> ...]
        [-touch [-a] [-m] [-t TIMESTAMP (yyyyMMdd:HHmmss) ] [-c] <path> ...]
        [-touchz <path> ...]
        [-truncate [-w] <length> <path> ...]
        [-usage [cmd ...]]

Generic options supported are:
-conf <configuration file>        specify an application configuration file
-D <property=value>               define a value for a given property
-fs <file:///|hdfs://namenode:port> specify default filesystem URL to use, overrides 'fs.defaultFS' property from configurations.
-jt <local|resourcemanager:port>  specify a ResourceManager     [-getfattr [-R] {-n name | -d} [-e en] <path>]
-files <file1,...>                specify a comma-separated list of files to be copied to the map reduce cluster

-libjars <jar1,...>               specify a comma-separated list of jar files to be included in the classpath
-archives <archive1,...>          specify a comma-separated list of archives to be unarchived on the compute machines

The general command line syntax is:
command [genericOptions] [commandOptions]

Usage: hadoop fs [generic options] -put [-f] [-p] [-l] [-d] [-t <thread count>] <localsrc> ... <dst>
        [-getmerge [-nl] [-skip-empty-file] <src> <localdst>]
        [-head <file>]
        [-help [cmd ...]]
        [-ls [-C] [-d] [-h] [-q] [-R] [-t] [-S] [-r] [-u] [-e] [<path> ...]]
        [-mkdir [-p] <path> ...]
        [-moveFromLocal [-f] [-p] [-l] [-d] <localsrc> ... <dst>]
        [-moveToLocal <src> <localdst>]
        [-mv <src> ... <dst>]
        [-put [-f] [-p] [-l] [-d] [-t <thread count>] <localsrc> ... <dst>]
        [-renameSnapshot <snapshotDir> <oldName> <newName>]
        [-rm [-f] [-r|-R] [-skipTrash] [-safely] <src> ...]
        [-rmdir [--ignore-fail-on-non-empty] <dir> ...]
        [-setfacl [-R] [{-b|-k} {-m|-x <acl_spec>} <path>]|[--set <acl_spec> <path>]]
        [-setfattr {-n name [-v value] | -x name} <path>]
        [-setrep [-R] [-w] <rep> <path> ...]
        [-stat [format] <path> ...]
        [-tail [-f] [-s <sleep interval>] <file>]
        [-test -[defsz] <path>]
        [-text [-ignoreCrc] <src> ...]
        [-touch [-a] [-m] [-t TIMESTAMP (yyyyMMdd:HHmmss) ] [-c] <path> ...]
        [-touchz <path> ...]
        [-truncate [-w] <length> <path> ...]
        [-usage [cmd ...]]

Generic options supported are:
-conf <configuration file>        specify an application configuration file
-D <property=value>               define a value for a given property
-fs <file:///|hdfs://namenode:port> specify default filesystem URL to use, overrides 'fs.defaultFS' property from configurations.
-jt <local|resourcemanager:port>  specify a ResourceManager
-files <file1,...>                specify a comma-separated list of files to be copied to the map reduce cluster
-libjars <jar1,...>               specify a comma-separated list of jar files to be included in the classpath
-archives <archive1,...>          specify a comma-separated list of archives to be unarchived on the compute machines

The general command line syntax is:
command [genericOptions] [commandOptions]

Usage: hadoop fs [generic options] -cat [-ignoreCrc] <src> ...
3314085 | 2026-01-28 13:35:22,014 | hcompressor | ERROR | Error while compression files: [Errno 32] Broken pipe
Traceback (most recent call last):
  File "./hcompressor", line 22, in <module>
    hcompressor.hcompressor.main()
  File "/opt/workspace/Script/CEKO/hcomp/lib/hcompressor/hcompressor.py", line 91, in main
    raise e
  File "/opt/workspace/Script/CEKO/hcomp/lib/hcompressor/hcompressor.py", line 84, in main
    compressor_writer.compress_write()
  File "/opt/workspace/Script/CEKO/hcomp/lib/hcompressor/compressors.py", line 121, in compress_write
    self.run_file_compression(chunk_size, input_file_path)
  File "/opt/workspace/Script/CEKO/hcomp/lib/hcompressor/compressors.py", line 134, in run_file_compression
    chunk_size=chunk_size,
  File "/opt/workspace/Script/CEKO/hcomp/lib/hcompressor/compressors.py", line 353, in compress_file_write
    file_out.write(chunk)
  File "/opt/workspace/Script/CEKO/hcomp/env/lib/python3.7/site-packages/smart_open/hdfs.py", line 139, in close
    self.flush()
  File "/opt/workspace/Script/CEKO/hcomp/env/lib/python3.7/site-packages/smart_open/hdfs.py", line 144, in flush
    self._sub.stdin.flush()
BrokenPipeError: [Errno 32] Broken pipe
[cdprct@cdp-gateway-01-rct bin]$
[cdprct@cdp-gateway-01-rct bin]$
[cdprct@cdp-gateway-01-rct bin]$
[cdprct@cdp-gateway-01-rct bin]$ ./hcompressor /raw/ebk_web_device_history/16-Jan-2026/ebk_web_device_history_20250502.txt  /raw/ebk_web_device_history/16-Jan-2026

3315040 | 2026-01-28 13:36:14,359 | hcompressor | INFO | Arguments parsed successfully
3315040 | 2026-01-28 13:36:26,260 | hcompressor | INFO | Files to compress : 1
3315040 | 2026-01-28 13:36:26,260 | hcompressor | INFO | Compressing /raw/ebk_web_device_history/16-Jan-2026/ebk_web_device_history_20250502.txt to /raw/ebk_web_device_history/16-Jan-2026/ebk_web_device_history_20250502.txt.zstd
3315040 | 2026-01-28 13:36:35,315 | hcompressor | INFO | Time : 9.055138 seconds, compression type: zstd
