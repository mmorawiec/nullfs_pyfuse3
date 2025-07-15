# nullfs_pyfuse3

nullfs_pyfuse3 is a write-only FUSE file system implemented in Python using [pyfuse3](https://pyfuse3.readthedocs.io/en/latest/).

The main intent was to create file system that would behave similar to `/dev/null` file.
This allows to drop the unwanted data from programs that create it in many files/subdirectories and
save on disk/memory space especially on memory constrained devices like Raspberry Pi.

The nullfs_pyfuse3 file system allows to:
- create and remove files and folders,
- open files in write-only mode,
- write to files (written data are discarded though),
- list its content.

File system data is stored in memory and not persisted.

See also comments/notes for pyfuse3.Operations overriden methods in `nullfs.py`
for additional details and limitations.

## Prerequisites

- Python 3.8+
- linux packages: FUSE 3 (`fuse3`, `libfuse3-dev`)
- Python packages: [pyfuse3](https://pypi.org/project/pyfuse3/) [typing_extensions](https://pypi.org/project/typing-extensions/)

Install dependencies:

```sh
sudo apt install fuse3 libfuse3-dev
pip install pyfuse3 typing_extensions
```

## Usage

Mount the file system:

```sh
sudo python3 nullfs.py /path/to/mount_directory
```

Optional flags:

- `--debug` : Enable debugging output
- `--debug-fuse` : Enable FUSE debugging output

## Testing

Tests are provided in `test_nullfs.py` and can be run with [pytest](https://pytest.org/).

