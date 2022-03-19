import os
import numpy as np
import h5py
import struct
import time
from mpi4py import MPI

def parallel_write_array(filename, local_array, axis=0, version=None, comm=None):
    """
    Parallelly write a ndarray to an NPY file

    :param filename: str
        Name of the file to write array into
    :param local_array: ndarray

    :param axis: intThe subarray to this process that will be written to disk
    :param version:
    :param comm:
    :return:
    """

    # if no MPI, or only 1 MPI process, call np.save directly
    if comm is None or comm.size == 1:
        np.save(filename, local_array)
        return

    if local_array.dtype.hasobject:
        # contain Python objects
        raise RuntimeError('Currently not support array that contains Python objects')
    if local_array.flags.f_contiguous and not local_array.flags.c_contiguous:
        raise RuntimeError('Currently not support Fortran ordered array')

    local_shape = local_array.shape # shape of local_array
    local_axis_len = local_shape[axis]
    local_axis_lens = comm.allgather(local_axis_len)
    global_axis_len = sum(local_axis_lens)
    global_shape = list(local_shape)
    global_shape[axis] = global_axis_len
    global_shape = tuple(global_shape) # shape of global array
    local_start = [0] * len(global_shape)
    local_start[axis] = np.cumsum([0] + local_axis_lens)[comm.rank] # start of local_array in global array

    # open the file in write only mode
    fh = MPI.File.Open(comm, filename, amode=MPI.MODE_CREATE | MPI.MODE_WRONLY)




comm = MPI.COMM_WORLD
rank = comm.rank
size = comm.size

order_id_path = "./data/100x100x100/order_id2.h5"
direction_path = "./data/100x100x100/direction2.h5"
price_path = "./data/100x100x100/price2.h5"
volume_path = "./data/100x100x100/volume2.h5"
type_path = "./data/100x100x100/type2.h5"

print("===============data in start============")
start = time.time()
order_id_mtx = h5py.File(order_id_path, 'r')['order_id'][:]
direction_mtx = h5py.File(direction_path, 'r')['direction'][:]
price_mtx = h5py.File(price_path, 'r')['price'][:]
prev_price_mtx = h5py.File(price_path, 'r')['prev_close'][:]
volume_mtx = h5py.File(volume_path, 'r')['volume'][:]
type_mtx = h5py.File(type_path, 'r')['type'][:]
print("===============data in end============", time.time() - start)

# 合理的价格变动区间，对于type=0的限价交易
price_range = []
for i in prev_price_mtx:
    price_range.append((round(i * 0.9, 2), round(i * 1.1, 2)))
print("价格变动范围", price_range)

x_len, y_len, z_len = order_id_mtx.shape
print("本次读入数据规模是：", order_id_mtx.shape)


# reshape the matrix
length = x_len * y_len * z_len

print("Hello World (from process %d)"%rank)
