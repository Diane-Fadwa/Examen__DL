machine_input_path = file_path.replace("hdfs://nameservice1", "", 1)
machine_output_path = machine_input_path.removesuffix(".zstd")
