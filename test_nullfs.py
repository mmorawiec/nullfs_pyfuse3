"""
Test suite for NullFS file system implementation.

The test suite contains:
- unit tests for the NullFS class (tests with fs_obj fixture)
- functional tests for the NullFS file system (tests with fs_run_dir fixture)
"""

import stat
import subprocess
import time
from pathlib import Path
import pytest
from pyfuse3 import FUSEError, ROOT_INODE
from nullfs import NullFS


@pytest.fixture
def fs_obj(tmp_path: Path) -> NullFS:
    """
    Prepare NullFS file system object with the temporary directory.
    """
    mountpoint: Path = tmp_path / "mount_dir"
    mountpoint.mkdir()
    return NullFS(mountpoint)


def test_fs_obj_root_inode(fs_obj: NullFS):
    """
    Test if NullFS root inode is present in new NullFS object.
    """
    root = fs_obj._get_inode(ROOT_INODE)
    assert root.attr.st_ino == ROOT_INODE


def test_fs_obj_file_add_and_remove(fs_obj: NullFS):
    """
    Test if file can be added and removed from the NullFS object.
    """
    parent_inode = ROOT_INODE
    name = "file"
    mode = stat.S_IFREG | 0o644
    uid = 1000
    gid = 1000
    umask = 0o022
    fs_obj._add_inode(parent_inode, name, mode, uid, gid, umask)

    inode_data = fs_obj._get_inode_by_name(parent_inode, name)
    assert inode_data.name == name
    assert inode_data.attr.st_mode == mode
    assert inode_data.attr.st_uid == uid
    assert inode_data.attr.st_gid == gid

    fs_obj._remove_inode(parent_inode, name)
    with pytest.raises(FUSEError):
        fs_obj._get_inode_by_name(parent_inode, name)


def test_fs_obj_file_open_and_close(fs_obj: NullFS):
    """
    Test if file can be opened and closed with the NullFS object.
    """
    parent_inode = ROOT_INODE
    name = "file"
    mode = stat.S_IFREG | 0o644
    uid = 1000
    gid = 1000
    umask = 0o022
    inode_data = fs_obj._add_inode(parent_inode, name, mode, uid, gid, umask)

    fh = fs_obj._open(inode_data.attr.st_ino)
    assert fs_obj._get_inode_by_fh(fh) == inode_data.attr.st_ino

    fs_obj._close(fh)
    with pytest.raises(FUSEError):
        fs_obj._get_inode_by_fh(fh)


def test_fs_obj_file_add_to_directory(fs_obj: NullFS):
    """
    Test if file can be added and removed from the directory added to NullFS object.
    """
    parent_inode = ROOT_INODE
    dir_name = "dir"
    dir_mode = stat.S_IFDIR | 0o755
    uid = 1000
    gid = 1000
    umask = 0o022
    fs_obj._add_inode(parent_inode, dir_name, dir_mode, uid, gid, umask)
    dir_inode = fs_obj._get_inode_by_name(parent_inode, dir_name)

    file_name = "file"
    file_mode = stat.S_IFREG | 0o644
    fs_obj._add_inode(dir_inode.attr.st_ino, file_name, file_mode, uid, uid, umask)
    fs_obj._get_inode_by_name(dir_inode.attr.st_ino, file_name)

    fs_obj._remove_inode(dir_inode.attr.st_ino, file_name)
    with pytest.raises(FUSEError):
        fs_obj._get_inode_by_name(dir_inode.attr.st_ino, file_name)


def test_fs_obj_dir_remove_nonempty(fs_obj: NullFS):
    """
    Test if non-empty directory cannot be removed from the NullFS object.
    """
    parent_inode = ROOT_INODE
    dir_name = "dir"
    dir_mode = stat.S_IFDIR | 0o755
    uid = 1000
    gid = 1000
    umask = 0o022
    dir_inode = fs_obj._add_inode(parent_inode, dir_name, dir_mode, uid, gid, umask)

    file_name = "file"
    file_mode = stat.S_IFREG | 0o644
    fs_obj._add_inode(dir_inode.attr.st_ino, file_name, file_mode, uid, uid, umask)

    with pytest.raises(FUSEError):
        fs_obj._remove_inode(parent_inode, dir_name)


@pytest.fixture(scope="session")
def fs_run_dir(tmp_path_factory: pytest.TempPathFactory):
    """
    Prepare running NullFS file system in the temporary directory for the test session duration.
    """
    mount_dir: Path = tmp_path_factory.mktemp("mount_dir")
    proc = subprocess.Popen(["python", "nullfs.py", str(mount_dir)], )

    # Ensure the NullFS is ready before yielding
    time.sleep(1)
    yield mount_dir

    proc.terminate()
    if mount_dir.is_mount():
        subprocess.run(["umount", str(mount_dir)], check=True)


def test_fs_run_mount_dir(fs_run_dir: Path):
    """
    Test if NullFS mount directory is valid and empty.
    """
    assert fs_run_dir.is_mount()
    assert not any(fs_run_dir.iterdir())


def test_fs_run_file_create_write_remove(fs_run_dir: Path):
    """
    Test if regular file:
    - can be created,
    - can be written,
    - cannot be read,
    - can be removed,
    in the NullFS file system.
    """
    file_path: Path = fs_run_dir / "testfile.txt"
    assert not file_path.exists()
    subprocess.run(["touch", file_path], check=True)
    assert file_path.is_file()

    subprocess.run(["echo", "Hello, World!", ">", file_path], check=True)

    with pytest.raises(subprocess.CalledProcessError):
        subprocess.run(["cat", file_path], check=True)

    subprocess.run(["rm", file_path], check=True)
    assert not file_path.exists()


def test_fs_run_dir_create_remove(fs_run_dir: Path):
    """
    Test if directory can be created and removed in the NullFS file system.
    """
    dir_path: Path = fs_run_dir / "testdir"
    assert not dir_path.exists()
    subprocess.run(["mkdir", dir_path], check=True)
    assert dir_path.is_dir()

    subprocess.run(["rmdir", dir_path], check=True)
    assert not dir_path.exists()


def test_fs_run_dir_multi_create_remove(fs_run_dir: Path):
    """
    Test if:
    - 3 nested directories can be created,
    - file can be created, written only and removed from the last directory,
    - created directories and file can be removed recursively,
    in the NullFS file system.
    It mostly executes the same operations as other tests, but in a nested directory structure.
    """
    dir1_path: Path = fs_run_dir / "testdir1"
    assert not dir1_path.exists()
    dir2_path: Path = dir1_path / "testdir2"
    assert not dir2_path.exists()
    dir3_path: Path = dir2_path / "testdir3"
    assert not dir3_path.exists()
    subprocess.run(["mkdir", "-p", dir3_path], check=True)
    assert dir1_path.is_dir()
    assert dir2_path.is_dir()
    assert dir3_path.is_dir()

    file_path: Path = dir3_path / "testfile.txt"
    assert not file_path.exists()
    subprocess.run(["touch", file_path], check=True)
    assert file_path.is_file()

    subprocess.run(["echo", "Hello, World!", ">", file_path], shell=True, check=True)

    with pytest.raises(subprocess.CalledProcessError):
        subprocess.run(["cat", file_path], check=True)

    subprocess.run(["rm", "-fr", dir1_path], check=True)
    assert not dir1_path.exists()
    assert not dir2_path.exists()
    assert not dir3_path.exists()
    assert not file_path.exists()
