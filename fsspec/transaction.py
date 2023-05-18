from typing import TYPE_CHECKING, Any, List, Type, Optional
from types import TracebackType

if TYPE_CHECKING:
    from fsspec.spec import AbstractBufferedFile, AbstractFileSystem


class Transaction(object):
    """Filesystem transaction write context

    Gathers files for deferred commit or discard, so that several write
    operations can be finalized semi-atomically. This works by having this
    instance as the ``.transaction`` attribute of the given filesystem
    """

    def __init__(self, fs: "AbstractFileSystem") -> None:
        """
        Parameters
        ----------
        fs: FileSystem instance
        """
        self.fs = fs
        self.files: List["AbstractBufferedFile"] = []

    def __enter__(self) -> None:
        self.start()

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: BaseException,
        exc_tb: TracebackType,
    ) -> None:
        """End transaction and commit, if exit is not due to exception"""
        # only commit if there was no exception
        self.complete(commit=exc_type is None)
        self.fs._intrans = False
        self.fs._transaction = None

    def start(self) -> None:
        """Start a transaction on this FileSystem"""
        self.files = []  # clean up after previous failed completions
        self.fs._intrans = True

    def complete(self, commit: bool = True) -> None:
        """Finish transaction: commit or discard all deferred files"""
        for f in self.files:
            if commit:
                f.commit()
            else:
                f.discard()
        self.files = []
        self.fs._intrans = False


class FileActor(object):
    def __init__(self) -> None:
        self.files: List["AbstractBufferedFile"] = []

    def commit(self) -> None:
        for f in self.files:
            f.commit()
        self.files.clear()

    def discard(self) -> None:
        for f in self.files:
            f.discard()
        self.files.clear()

    def append(self, f: "AbstractBufferedFile") -> None:
        self.files.append(f)


class DaskTransaction(Transaction):
    def __init__(self, fs: "AbstractFileSystem") -> None:
        """
        Parameters
        ----------
        fs: FileSystem instance
        """
        import distributed

        super().__init__(fs)
        client = distributed.default_client()
        self.files: Any = client.submit(FileActor, actor=True).result()

    def complete(self, commit: bool = True) -> None:
        """Finish transaction: commit or discard all deferred files"""
        if commit:
            self.files.commit().result()
        else:
            self.files.discard().result()
        self.fs._intrans = False
