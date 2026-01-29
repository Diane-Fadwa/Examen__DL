
[cdprct@cdp-gateway-01-rct bin]$ ./hcompressor \
  --mode decompression \
  --compression_type zstd \
  --delete_input 0 \
  /raw/ebk_web_device_history/16-Jan-2026/ebk_web_device_history_20250502.txt.zstd \
  /raw/ebk_web_device_history/16-Jan-2026/ebk_web_device_history_20250502.txt

3890089 | 2026-01-29 11:55:59,971 | hcompressor | INFO | Arguments parsed successfully
3890089 | 2026-01-29 11:56:11,902 | hcompressor | INFO | 1 files to uncompress
3890089 | 2026-01-29 11:56:11,902 | hcompressor | INFO | Decompressing /raw/ebk_web_device_history/16-Jan-2026/ebk_web_device_history_20250502.txt.zstd to /raw/ebk_web_device_history/16-Jan-2026/ebk_web_device_history_20250502.txt/ebk_web_device_history_20250502.txt
put: Current inode is not a directory: ebk_web_device_history_20250502.txt(179596804, INodeFile@21d21df5, parentDir=16-Jan-2026/)
3890089 | 2026-01-29 11:56:20,292 | hcompressor | ERROR | Error while decompression files: [Errno 32] Broken pipe
Traceback (most recent call last):
  File "./hcompressor", line 22, in <module>
    hcompressor.hcompressor.main()
  File "/opt/workspace/Script/CEKO/hcomp/lib/hcompressor/hcompressor.py", line 91, in main
    raise e
  File "/opt/workspace/Script/CEKO/hcomp/lib/hcompressor/hcompressor.py", line 87, in main
    compressor_writer.decompress_write()
  File "/opt/workspace/Script/CEKO/hcomp/lib/hcompressor/compressors.py", line 204, in decompress_write
    self.run_file_decompression(chunk_size, input_file_path)
  File "/opt/workspace/Script/CEKO/hcomp/lib/hcompressor/compressors.py", line 218, in run_file_decompression
    chunk_size=chunk_size,
  File "/opt/workspace/Script/CEKO/hcomp/lib/hcompressor/compressors.py", line 371, in decompress_file_write
    file_out.write(chunk)
  File "/opt/workspace/Script/CEKO/hcomp/env/lib/python3.7/site-packages/smart_open/hdfs.py", line 155, in write
    self._sub.stdin.write(b)
BrokenPipeError: [Errno 32] Broken pipe
