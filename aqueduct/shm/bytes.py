from typing import Union

from .base import SharedData, SharedMemoryWrapper


class BytesSharedData(SharedData):
    def __init__(self, shm_wrapper: SharedMemoryWrapper, size: int):
        self._size = size
        super().__init__(shm_wrapper)

    def get_data(self) -> memoryview:
        if self.shm_wrapper.buf is None:
            raise ValueError('No shared memory buffer')
        return self.shm_wrapper.buf[0:self._size]

    @classmethod
    def create_from_data(cls, data: Union[bytes, bytearray, memoryview]) -> 'BytesSharedData':
        nbytes = len(data)
        shm_wrapper = SharedMemoryWrapper(nbytes)
        shared_data = cls(shm_wrapper, nbytes)
        shm_wrapper.buf[0:nbytes] = data
        return shared_data
