[cdprct@cdp-gateway-01-rct hcompressor]$ cat hdfs_utils.py
from sys import stderr

from hcompressor.utils import clean_path


class HdfsUtils:
    """Hdfs utils class"""

    @staticmethod
    def get_recursive_hdfs_files_list(input_path: str):
        """Get recursive hdfs files list

        Args:
            input_path (str): Input path

        Returns:
            list: List of files

        Raises:
            InvalidHdfsPathException: Invalid hdfs path exception

        Examples:
            >>> get_recursive_hdfs_files_list("/path/to/folder")
            ["/path/to/folder/file1.csv", "/path/to/folder/file2.csv", "/path/to/folder/subfolder/file3.csv"]
        """

        HdfsUtils.validate_hdfs_path(input_path)
        files_list = []

        exit_code, stdo, stde = HdfsUtils.exec_command(f"hdfs dfs -ls -R {input_path}")
        stdo = stdo.decode("utf-8")
        if exit_code != 0:
            raise InvalidHdfsPathException(f"{input_path} is not a valid hdfs path ")

        for line in stdo.splitlines():
            if line.startswith("-"):
                files_list.append(line.split(" ")[-1])

        return files_list

    @staticmethod
    def validate_hdfs_path(input_path: str):
        """Validate hdfs path

        Args:
            input_path (str): Input path

        Raises:
            InvalidHdfsPathException: Invalid hdfs path exception

        Examples:
            >>> validate_hdfs_path("/path/to/folder")
            True
            >>> validate_hdfs_path("/path/to/folder/")
            False
            >>> validate_hdfs_path("/path/to/folder/file.csv")
            False
        """

        if not HdfsUtils.exists(input_path):
            raise InvalidHdfsPathException(f"{input_path} is not a valid hdfs path ")

    @staticmethod
    def ensure_hdfs_file_parent_folder_exists(file_path: str):
        """Ensure hdfs file parent folder exists

        Args:
            file_path (str): File path

        Examples:
            >>> ensure_hdfs_file_parent_folder_exists("/path/to/folder/file.csv")
        """

        parent_folder = clean_path(file_path, prefix=False, suffix=True).rsplit("/", 1)[
            0
        ]
        HdfsUtils.ensure_hdfs_directory_exists(parent_folder)

    @classmethod
    def ensure_hdfs_directory_exists(cls, path: str):
        """Ensure hdfs directory exists

        if the directory does not exist, it will be created

        Args:
            path (str): Path

        Examples:
            >>> ensure_hdfs_directory_exists("/path/to/folder")

        """

        if not HdfsUtils.exists(path):
            HdfsUtils.mkdir(path, verbose=True)

    @classmethod
    def check_is_directory(cls, path: str):
        """Check if path is a directory

        Args:
            path (str): Path

        Returns:
            bool: True if path is a directory, False otherwise

        Examples:
            >>> check_is_directory("/path/to/folder")
            True
            >>> check_is_directory("/path/to/folder/file.csv")
            False
        """
        return HdfsUtils.test(path, test="d")

    @classmethod
    def get_non_recursive_hdfs_files_list(cls, input_path: str):
        """Get non recursive hdfs files list

        Args:
            input_path (str): Input path

        Returns:
            list: List of files

        Raises:
            InvalidHdfsPathException: Invalid hdfs path exception

        Examples:
            >>> get_non_recursive_hdfs_files_list("/path/to/folder")
            ["/path/to/folder/file1.csv", "/path/to/folder/file2.csv"]
        """

        HdfsUtils.validate_hdfs_path(input_path)
        files_list = []

        exit_code, stdo, stde = HdfsUtils.exec_command(f"hdfs dfs -ls {input_path}")
        stdo = stdo.decode("utf-8")

        if exit_code != 0:
            raise InvalidHdfsPathException(f"{input_path} is not a valid hdfs path ")

        for line in stdo.splitlines():
            if line.startswith("-"):
                files_list.append(line.split(" ")[-1])

        return files_list

    @classmethod
    def ls(cls, hdfs_url="", recurse=False, full=False):
        """
        List the hdfs URL.  If the URL is a directory, the contents are returned.
        full=True ensures hdfs:// is prepended
        """

        if recurse:
            cmd = "lsr"
        else:
            cmd = "ls"

        command = "hadoop fs -%s %s" % (cmd, hdfs_url)

        exit_code, stdo, stde = HdfsUtils.exec_command(command)
        if exit_code != 0:
            raise ValueError("command failed with code %s: %s" % (exit_code, command))

        flist = []
        lines = stdo.split(b"\n")
        for line in lines:
            ls = line.split()
            if len(ls) == 8:
                # this is a file description line
                fname = ls[-1]
                if full:
                    fname = "hdfs://" + fname.decode("utf-8")
                flist.append(fname)

        return flist

    @classmethod
    def mkdir(cls, hdfs_url, verbose=False):
        """
        Equivalent of mkdir -p in unix
        """
        if verbose:
            print("mkdir", hdfs_url, file=stderr)

        command = "hadoop fs -mkdir -p " + hdfs_url
        exit_code, stdo, stde = HdfsUtils.exec_command(command)
        if exit_code != 0:
            raise RuntimeError("hdfs %s" % stde)

    @classmethod
    def test(cls, hdfs_url, test="e"):
        """
        Test the url.
        parameters
        ----------
        hdfs_url: string
            The hdfs url
        test: string, optional
            'e': existence
            'd': is a directory
            'z': zero length
            Default is an existence test, 'e'
        """
        command = """hadoop fs -test -%s %s""" % (test, hdfs_url)

        exit_code, stdo, stde = HdfsUtils.exec_command(command)

        if exit_code != 0:
            return False
        else:
            return True

    @classmethod
    def rm(cls, hdfs_url, recurse=False, verbose=False):
        """
        Remove the specified hdfs url
        """
        mess = "removing " + hdfs_url

        if recurse:
            cmd = "rmr"
            mess += " recursively"
        else:
            cmd = "rm"

        if verbose:
            print(mess, file=stderr)

        command = "hadoop fs -%s %s" % (cmd, hdfs_url)
        exit_code, stdo, stde = HdfsUtils.exec_command(command)
        if exit_code != 0:
            raise RuntimeError("hdfs %s" % stde)

    @classmethod
    def exec_command(cls, command):
        """
        Execute the command and return the exit status.
        """
        import subprocess
        from subprocess import PIPE

        pobj = subprocess.Popen(command, stdout=PIPE, stderr=PIPE, shell=True)

        stdo, stde = pobj.communicate()
        exit_code = pobj.returncode

        return exit_code, stdo, stde

    @classmethod
    def exists(cls, hdfs_url):
        """
        Test if the url exists.
        """
        return HdfsUtils.test(hdfs_url, test="e")


class InvalidHdfsPathException(Exception):
    pass
[cdprct@cdp-gateway-01-rct hcompressor]$
