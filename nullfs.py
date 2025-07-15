#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
nullfs.py - write-only file system using pyfuse3.

This program creates a write-only file system which allows to:
- create and remove files and folders,
- open files in write-only mode,
- write to files (written data are discarded though),
- list its content.
File system data is stored in memory and not persisted.
See also comments/notes for pyfuse3.Operations overriden methods
for additional details and limitations.

Based on "Single-file, Read-only File System" example from pyfuse3
(https://pyfuse3.readthedocs.io/en/latest/example.html).

Prerequisites:
- linux packages: fuse3 libfuse3-dev
- Python packages: pyfuse3 typing_extensions

Copyright © 2025 Michal Morawiec <mmorawiec at gmail dot com>

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

import errno
import faulthandler
import itertools
import logging
import os
import sys
from argparse import ArgumentParser, Namespace
from dataclasses import dataclass, field
from pathlib import Path
from time import time_ns
from typing import Optional, Sequence, Tuple
from typing_extensions import Self
import pyfuse3
import trio
from pyfuse3 import (EntryAttributes,
                     FileHandleT,
                     FileInfo,
                     FileNameT,
                     FlagT,
                     FUSEError,
                     InodeT,
                     ModeT,
                     readdir_reply,
                     ReaddirToken,
                     RequestContext,
                     ROOT_INODE,
                     SetattrFields,
                     XAttrNameT)


class NullFS(pyfuse3.Operations):
    """
    Implements pyfuse3 request handler methods.
    """

    # File system name.
    NAME = 'nullfs_pyfuse3'

    def __init__(self, mount_dir: Path) -> None:
        super().__init__()

        self._free_inode = itertools.count(ROOT_INODE + 1)
        self._inode_data: dict[InodeT, NullFS.InodeData] = {}
        self._free_file_handle = itertools.count(0)
        self._file_handle_inode: dict[FileHandleT, InodeT] = {}
        self._set_root_inode(mount_dir)

# region Inode management

    @dataclass
    class InodeData:
        """
        Holds file system node data.
        """
        name: FileNameT
        attr: EntryAttributes
        parent_inode: Self | None
        child_inodes: list[Self] = field(default_factory=list)

        def add_child(self, inode: Self) -> None:
            self.child_inodes.append(inode)

        def remove_child(self, inode: Self) -> None:
            self.child_inodes.remove(inode)

        def get_child(self, name: FileNameT) -> Self | None:
            return next((inode for inode in self.child_inodes if inode.name == name), None)

    def _get_inode(self, inode: InodeT) -> InodeData:
        inode_data = self._inode_data.get(inode)
        if inode_data is None:
            raise FUSEError(errno.ENOENT)

        return inode_data

    def _get_inode_by_name(self, parent_inode: InodeT, name: FileNameT) -> InodeData:
        parent_inode_data = self._inode_data.get(parent_inode)
        if parent_inode_data is None:
            raise FUSEError(errno.ENOENT)

        child_inode_data = parent_inode_data.get_child(name)
        if child_inode_data is None:
            raise FUSEError(errno.ENOENT)

        return child_inode_data

    def _get_inode_by_fh(self, fh: FileHandleT) -> InodeT:
        inode = self._file_handle_inode.get(fh)
        if inode is None:
            raise FUSEError(errno.ENOENT)

        return inode

    def _set_root_inode(self, root_dir_path: Path) -> None:
        # Root inode must be set first and only once
        if self._inode_data:
            raise FUSEError(errno.EINVAL)

        assert root_dir_path.is_dir(), f'Root dir {str(root_dir_path)} is not directory'
        root_dir_stat = root_dir_path.stat()
        attr = EntryAttributes()
        attr.st_ino = ROOT_INODE
        # generation, entry_timeout, attr_timeout attributes are not used.
        # attr.generation = 0
        # attr.entry_timeout = 0
        # attr.attr_timeout = 0
        attr.st_mode = root_dir_stat.st_mode
        attr.st_nlink = root_dir_stat.st_nlink
        attr.st_uid = root_dir_stat.st_uid
        attr.st_gid = root_dir_stat.st_gid
        attr.st_rdev = root_dir_stat.st_rdev
        attr.st_size = root_dir_stat.st_size
        attr.st_blksize = root_dir_stat.st_blksize
        attr.st_blocks = root_dir_stat.st_blocks
        attr.st_atime_ns = root_dir_stat.st_atime_ns
        attr.st_ctime_ns = root_dir_stat.st_ctime_ns
        attr.st_mtime_ns = root_dir_stat.st_mtime_ns
        # st_birthtime available under BSD and OS X only. It is zero on Linux.
        attr.st_birthtime_ns = 0
        self._inode_data[ROOT_INODE] = self.InodeData(root_dir_path.name, attr, None)

    def _add_inode(
        self,
        parent_inode: InodeT,
        name: FileNameT,
        mode: ModeT,
        uid: int,
        gid: int,
        umask: int
    ) -> InodeData:
        parent_inode_data = self._get_inode(parent_inode)

        attr = EntryAttributes()
        attr.st_ino = next(self._free_inode)
        # generation, entry_timeout, attr_timeout attributes are not used.
        # attr.generation = 0
        # attr.entry_timeout = 0
        # attr.attr_timeout = 0
        attr.st_mode = mode & ~umask
        attr.st_nlink = 0
        attr.st_uid = uid
        attr.st_gid = gid
        # st_rdev is used for device files (unsupported by this file system).
        # It is set to zero for regular files and directories.
        attr.st_rdev = 0
        attr.st_size = 0
        attr.st_blksize = 4096
        attr.st_blocks = 0
        time_stamp_ns: int = time_ns()
        attr.st_atime_ns = time_stamp_ns
        attr.st_ctime_ns = time_stamp_ns
        attr.st_mtime_ns = time_stamp_ns
        # st_birthtime available under BSD and OS X only. It is zero on Linux.
        attr.st_birthtime_ns = 0

        inode_data = self.InodeData(name, attr, parent_inode_data)
        self._inode_data[attr.st_ino] = inode_data
        parent_inode_data.add_child(inode_data)

        return inode_data

    def _remove_inode(self, parent_inode: InodeT, name: FileNameT) -> None:
        child_inode_data = self._get_inode_by_name(parent_inode, name)

        if len(child_inode_data.child_inodes) > 0:
            raise FUSEError(errno.ENOTEMPTY)

        child_inode_data.parent_inode.remove_child(child_inode_data)
        del self._inode_data[child_inode_data.attr.st_ino]

    def _open(self, inode: InodeT) -> FileHandleT:
        if inode not in self._inode_data:
            raise FUSEError(errno.ENOENT)

        fh: FileHandleT = next(self._free_file_handle)
        self._file_handle_inode[fh] = inode

        return fh

    def _close(self, fh: FileHandleT) -> None:
        try:
            del self._file_handle_inode[fh]
        except KeyError as e:
            raise FUSEError(errno.ENOENT) from e

# endregion

# region pyfuse3.Operations

    async def create(
        self,
        parent_inode: InodeT,
        name: FileNameT,
        mode: ModeT,
        _flags: FlagT,
        ctx: RequestContext
    ) -> Tuple[FileInfo, EntryAttributes]:
        """
        pyfuse3.Operations.create override
        Note:
        - permissions are not checked
        - flags are ignored, only write-only mode is supported
        """
        inode_data = self._add_inode(parent_inode, name, mode, ctx.uid, ctx.gid, ctx.umask)
        fh = self._open(inode_data.attr.st_ino)

        return (FileInfo(fh), inode_data.attr)

    async def flush(
        self,
        _fh: FileHandleT
    ) -> None:
        """
        pyfuse3.Operations.flush override
        Note:
        - flush is not implemented
        """

    async def forget(
        self,
        _inode_list: Sequence[Tuple[InodeT, int]]
    ) -> None:
        """
        pyfuse3.Operations.forget override
        Note:
        - lookup count is not implemented
        """

    async def getattr(
        self,
        inode: InodeT,
        _ctx: RequestContext
    ) -> EntryAttributes:
        """
        pyfuse3.Operations.getattr override
        Note:
        - permissions are not checked
        """
        return self._get_inode(inode).attr

    async def getxattr(
        self,
        _inode: InodeT,
        name: XAttrNameT,
        _ctx: RequestContext
    ) -> bytes:
        """
        pyfuse3.Operations.getxattr override
        Note:
        - extended attributes are not supported, just name is printed
        - permissions are not checked
        """
        print(f'xattr: {name!r}')
        raise FUSEError(pyfuse3.ENOATTR)

    async def lookup(
        self,
        parent_inode: InodeT,
        name: FileNameT,
        _ctx: RequestContext
    ) -> EntryAttributes:
        """
        pyfuse3.Operations.lookup override
        Note:
        - permissions are not checked
        """
        return self._get_inode_by_name(parent_inode, name).attr

    async def mkdir(
        self,
        parent_inode: InodeT,
        name: FileNameT,
        mode: ModeT,
        ctx: RequestContext
    ) -> EntryAttributes:
        """
        pyfuse3.Operations.mkdir override
        Note:
        - permissions are not checked
        """
        return self._add_inode(parent_inode, name, mode, ctx.uid, ctx.gid, ctx.umask).attr

    async def open(
        self,
        inode: InodeT,
        flags: FlagT,
        _ctx: RequestContext
    ) -> FileInfo:
        """
        pyfuse3.Operations.open override
        Note:
        - permissions are not checked
        - write-only mode is supported only
        """
        if flags & os.O_WRONLY:
            return FileInfo(self._open(inode))

        raise FUSEError(errno.EACCES)

    async def opendir(
        self,
        inode: InodeT,
        _ctx: RequestContext
    ) -> FileHandleT:
        """
        pyfuse3.Operations.opendir override
        Note:
        - permissions are not checked
        """
        return self._open(inode)

    async def readdir(
        self,
        fh: FileHandleT,
        start_id: int,
        token: ReaddirToken
    ) -> None:
        """
        pyfuse3.Operations.readdir override
        Note:
        - ensure correct multiple calls for single token
        """
        inode = self._get_inode_by_fh(fh)

        child_inodes = self._inode_data[inode].child_inodes
        if 0 <= start_id < len(child_inodes):
            for index, child_inode_data in enumerate(child_inodes[start_id:]):
                next_id = start_id + index + 1
                if not readdir_reply(token, child_inode_data.name, child_inode_data.attr, next_id):
                    break

    async def release(
        self,
        fh: FileHandleT
    ) -> None:
        """
        pyfuse3.Operations.release override
        """
        self._close(fh)

    async def releasedir(
        self,
        fh: FileHandleT
    ) -> None:
        """
        pyfuse3.Operations.releasedir override
        """
        self._close(fh)

    async def rmdir(
        self,
        parent_inode: InodeT,
        name: FileNameT,
        _ctx: RequestContext
    ) -> None:
        """
        pyfuse3.Operations.rmdir override
        Note:
        - permissions are not checked
        """
        self._remove_inode(parent_inode, name)

    async def setattr(
        self,
        inode: InodeT,
        attr: EntryAttributes,
        fields: SetattrFields,
        _fh: Optional[FileHandleT],
        _ctx: RequestContext
    ) -> EntryAttributes:
        """
        pyfuse3.Operations.setattr override
        Note:
        - permissions are not checked
        """
        inode_data = self._get_inode(inode)

        if fields.update_atime:
            inode_data.attr.st_atime_ns = attr.st_atime_ns
        if fields.update_mtime:
            inode_data.attr.st_mtime_ns = attr.st_mtime_ns
        if fields.update_ctime:
            inode_data.attr.st_ctime_ns = attr.st_ctime_ns
        if fields.update_mode:
            inode_data.attr.st_mode = attr.st_mode
        if fields.update_uid:
            inode_data.attr.st_uid = attr.st_uid
        if fields.update_gid:
            inode_data.attr.st_gid = attr.st_gid
        if fields.update_size:
            inode_data.attr.st_size = attr.st_size

        return inode_data.attr

    async def unlink(
        self,
        parent_inode: InodeT,
        name: FileNameT,
        _ctx: RequestContext
    ) -> None:
        """
        pyfuse3.Operations.write override
        Note:
        - permissions are not checked
        """
        self._remove_inode(parent_inode, name)

    async def write(
        self,
        fh: FileHandleT,
        _off: int,
        buf: bytes
    ) -> int:
        """
        pyfuse3.Operations.write override
        Note:
        - file size is not adjusted, written data are discarded
        """
        # Ensure that the file handle is valid.
        self._get_inode_by_fh(fh)

        # Return the length of the buffer as if it was written.
        return len(buf)

# endregion


# region app

def init_logging(debug=False) -> None:
    formatter = logging.Formatter('%(asctime)s.%(msecs)03d %(threadName)s: '
                                  '[%(name)s] %(message)s', datefmt="%Y-%m-%d %H:%M:%S")

    handler = logging.StreamHandler()
    handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.addHandler(handler)

    if debug:
        faulthandler.enable()
        handler.setLevel(logging.DEBUG)
        root_logger.setLevel(logging.DEBUG)
    else:
        handler.setLevel(logging.INFO)
        root_logger.setLevel(logging.INFO)


def parse_args() -> Namespace:
    parser = ArgumentParser()
    parser.add_argument('mount_dir', type=str,
                        help='File system mount directory')
    parser.add_argument('--debug', action='store_true', default=False,
                        help='Enable debugging output')
    parser.add_argument('--debug-fuse', action='store_true', default=False,
                        help='Enable FUSE debugging output')
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    init_logging(args.debug)

    mount_dir = Path(args.mount_dir)
    if not mount_dir.is_dir():
        logging.getLogger().error('Mount directory %s is not directory', str(mount_dir))
        sys.exit(1)

    fuse_options = set(pyfuse3.default_options)
    fuse_options.add(f'fsname={NullFS.NAME}')
    if args.debug_fuse:
        fuse_options.add('debug')

    pyfuse3.init(NullFS(mount_dir), str(mount_dir), fuse_options)

    try:
        trio.run(pyfuse3.main)
    finally:
        pyfuse3.close()


if __name__ == '__main__':
    main()

# endregion
