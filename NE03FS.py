"""
The file is partitioned into a complete binary tree
(0 for the starting of the file)
     0
   /  \
  1   2
 / \ /
3  45
A single node in the tree contains a maximum of 64 KB payload
(Actually 65536-128=65408 Bytes of payload)
Each node is relayed as a transaction on Neo3 blockchain.
Downloading:
    Download node 0, discovering scripthashes of 1 and 2.
    Push 1 and 2 to the queue for downloading
    Download 1 and 2, discovering 3,4,5, and push 3,4,5 to the queue
    A 21-layer tree can store 64KB * (2**20) == 64 GB of payload,
    with an acceptable memory cost of 2**20 == 1e6 nodes
Uploading:
    The lower layers of the binary tree is firstly uploaded
"""

from gevent import monkey
monkey.patch_all()
import gevent
import gevent.pool
import gevent.queue
from gevent.greenlet import Greenlet
from neo3.core.types import UInt256

import os
import random
from itertools import cycle
from typing import Union, Tuple
from base64 import b64encode, b64decode
from tests.utils import Hash160Str, Hash256Str
from mmap import mmap, ACCESS_READ, ACCESS_WRITE
from neo_test_with_rpc import TestClient


import logging
import sys
logging.basicConfig(stream=sys.stdout, level=logging.INFO)


class FileReader:
    def __init__(self, file_path: str, mode='rb'):
        self.file_path = file_path
        self.file_obj = open(file_path, mode)
        self.file_size = os.path.getsize(file_path)
        if mode=='rb':
            self.mmap = mmap(self.file_obj.fileno(), 0, access=ACCESS_READ)
        elif mode == 'rw':
            self.mmap = mmap(self.file_obj.fileno(), 0, access=ACCESS_WRITE)
    
    def __enter__(self):
        return self.file_obj
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.mmap.close()
        self.file_obj.close()
        return True
    
    def close(self):
        self.mmap.close()
        self.file_obj.close()
    
    def __getitem__(self, item):
        return self.mmap.__getitem__(item)
    
    def __len__(self):
        return self.file_size


class FilePartition:
    max_bytes_payload_per_node = 65536 - 128  # 65408 Bytes
    
    def __init__(self, file_path_or_content: Union[str, bytes], max_bytes=None):
        """
        :param file_path_or_content: str considered as file path; bytes considered as file content
        """
        # memoryview for performance; avoid copying on slicing[start:end]
        if type(file_path_or_content) is str:
            self.file_path = file_path_or_content
            self.file_content = FileReader(file_path_or_content)
        else:
            self.file_path = None
            self.file_content = memoryview(file_path_or_content)
        self.len_file = len(self.file_content)
        if self.len_file <= 0:
            raise ValueError(f'File length is {self.len_file} <= 0')
        if max_bytes:
            self.max_bytes_payload_per_node = max_bytes

    @staticmethod
    def calc_binary_tree_layer_and_count_last_layer_nodes(node_count: int):
        exponential = 0
        while 2 ** exponential <= node_count:
            exponential += 1
        layer_of_binary_tree = exponential
        last_layer_nodes_count = node_count - 2 ** (exponential - 1) + 1
        return layer_of_binary_tree, last_layer_nodes_count
    
    def count_file_nodes(self, max_bytes=None):
        if not max_bytes:
            max_bytes = self.max_bytes_payload_per_node
        len_file = self.len_file
        remainder = len_file % max_bytes
        nodes_count = len_file // max_bytes + min(1, remainder)
        layer_of_binary_tree, last_layer_nodes_count = self.calc_binary_tree_layer_and_count_last_layer_nodes(nodes_count)
        return nodes_count, layer_of_binary_tree, last_layer_nodes_count

    def get_file_partition(self, file_node_id: int, max_bytes=None) -> Tuple[bytes, int]:
        if not max_bytes:
            max_bytes = self.max_bytes_payload_per_node
        file = self.file_content
        len_file = self.len_file
        start = max_bytes * file_node_id
        if start >= len_file:
            raise ValueError(f'Too large file_node_id {file_node_id}. '
                             f'Maximum is {len_file//max_bytes + min(1, len_file%max_bytes) - 1}')
        end = min(len_file, start + max_bytes)
        return bytes(file[start:end]), end-start


class ConvertFileAndScript:
    @staticmethod
    def file_bytes_to_script(file: bytes,
                             left_child_scripthash: Hash256Str = None,
                             right_child_scripthash: Hash256Str = None,
                             len_file: int = None) -> bytes:
        """
        :return: @ PUSHDATA LEN_DATA LEFT_HASH256 RIGHT_HASH256 FILE
        """
        def select_opcode_and_byte_count(_len_file: int):
            if _len_file < 2 ** 8:
                _opcode = b'\x0c'
                _byte_count = 1
            elif _len_file < 2 ** 16:
                _opcode = b'\x0d'
                _byte_count = 2
            elif _len_file < 2 ** 32:
                _opcode = b'\x0e'
                _byte_count = 4
            else:
                raise NotImplementedError(f'File length {_len_file} too long')
            return _opcode, _byte_count
    
        if not len_file:
            len_file = len(file)
        len_file += 64  # two hash256
        if not left_child_scripthash:
            left_child_scripthash = Hash256Str('0x0000000000000000000000000000000000000000000000000000000000000000')
        if not right_child_scripthash:
            right_child_scripthash = Hash256Str('0x0000000000000000000000000000000000000000000000000000000000000000')
        opcode, byte_count = select_opcode_and_byte_count(len_file)
        return b'@'+opcode+len_file.to_bytes(byte_count, 'little') + \
               left_child_scripthash.to_UInt256().to_array() + right_child_scripthash.to_UInt256().to_array() + file
    
    @staticmethod
    def script_to_file_bytes(script: bytes) -> Tuple[bytes, Hash256Str, Hash256Str]:
        def get_file_bytes_len_from_opcode(_opcode: bytes):
            if _opcode == b'\x0c':
                return 1
            if _opcode == b'\x0d':
                return 2
            if _opcode == b'\x0e':
                return 4
            raise ValueError(f'Unexpected opcode {_opcode}')
        opcode = script[1:2]
        len_file_bytes = get_file_bytes_len_from_opcode(opcode)
        start = 2+len_file_bytes
        left_child_hash256 = Hash256Str.from_UInt256(UInt256(script[start:start+32]))
        right_child_hash256 = Hash256Str.from_UInt256(UInt256(script[start+32:start+64]))
        file = script[start+64:]
        return file, left_child_hash256, right_child_hash256


class Uploader:
    concurrency_per_endpoint = 4
    endpoint_urls = ['http://localhost:10332']
    
    def __init__(self, file_path_or_content: Union[str, bytes],
                 wallet_scripthash: Hash160Str, wallet_address: str,
                 wallet_path: str, wallet_password: str, max_bytes=None):
        self.filename = os.path.split(file_path_or_content)[1]  # for user's reading only
        self.file_partition = FilePartition(file_path_or_content, max_bytes=max_bytes)
        self.wallet_scripthash = wallet_scripthash
        self.wallet_address = wallet_address
        self.wallet_path = wallet_path
        self.wallet_password = wallet_password
        self.pool = gevent.pool.Pool(self.concurrency_per_endpoint * len(self.endpoint_urls))
        
    def upload(self):
        def upload_node(file_node_id: int) -> Hash256Str:
            left_child_node_id = file_node_id * 2 + 1
            if left_child_node_id < file_nodes_count:
                left_child_hash256 = gevent.spawn(upload_node, left_child_node_id)
            else:
                left_child_hash256 = None
            right_child_node_id = file_node_id * 2 + 2
            if right_child_node_id < file_nodes_count:
                right_child_hash256 = gevent.spawn(upload_node, right_child_node_id)
            else:
                right_child_hash256 = None
            if left_child_hash256 is not None:  # do not write `if left_child_hash256:`
                left_child_hash256.join()
                left_child_hash256 = left_child_hash256.value
                assert left_child_hash256, f"Failed to upload file node {left_child_node_id}"
            if right_child_hash256 is not None:  # do not write `if right_child_hash256:`
                right_child_hash256.join()
                right_child_hash256 = right_child_hash256.value
                assert right_child_hash256, f"Failed to upload file node {right_child_node_id}"
                
            file_node_bytes, file_node_length = self.file_partition.get_file_partition(file_node_id)
            script = ConvertFileAndScript.file_bytes_to_script(
                file_node_bytes, left_child_hash256, right_child_hash256, len_file=file_node_length)
            script_b64 = b64encode(script)
            # logging.debug(f"File node {file_node_id}: {str(file_node_bytes)}")
            # logging.debug(f"File node {file_node_id}: {str(script_b64)}")
            client: TestClient = next(endpoint_url_generator)

            invokescript_task = self.pool.spawn(client.invokescript, script_b64)
            invokescript_task.join()
            _, raw_result, _ = invokescript_task.value
            assert raw_result, f"Failed to invokescript for file node {file_node_id}"
            tx = raw_result['result']['tx']
            
            relay_task = gevent.spawn(client.sendrawtransaction, tx)
            relay_task.join()
            _, raw_result, _ = relay_task.value
            assert raw_result and raw_result['result']['hash'], \
                f"Failed to relay file node {file_node_id} with\n" \
                f"left child: {left_child_hash256}\n" \
                f"right child: {right_child_hash256}\n" \
                f"and payload: {str(file_node_bytes)}"
            tx_hash = raw_result['result']['hash']
            logging.info(f"File node {file_node_id} relayed in transaction {tx_hash}")
            return Hash256Str(tx_hash)

        dummy_contract_hash = Hash160Str('0x0000000000000000000000000000000000000000')
        clients = [TestClient(target_url, dummy_contract_hash,
                              self.wallet_scripthash, self.wallet_address,
                              self.wallet_path, self.wallet_password,
                              with_print=False, verbose_return=True)
                   for target_url in self.endpoint_urls]
        logging.info('Opening wallets')
        gevent.joinall([gevent.spawn(client.openwallet) for client in clients])
        logging.info('Open wallets OK')
        endpoint_urls_for_load_balancing = clients * self.concurrency_per_endpoint
        random.shuffle(endpoint_urls_for_load_balancing)
        endpoint_url_generator = cycle(endpoint_urls_for_load_balancing)
        
        file_nodes_count, layer_of_binary_tree, last_layer_nodes_count = self.file_partition.count_file_nodes()
        logging.info(f"{file_nodes_count} file nodes, {layer_of_binary_tree} layers of binary tree "
                     f"with {last_layer_nodes_count} nodes in the last layer")
        root_result = self.pool.spawn(upload_node, 0)
        root_result.join()
        assert root_result.value, f"Failed to upload file node 0"
        return root_result.value


class Downloader:
    concurrency_per_endpoint = 1
    endpoint_urls = ['http://localhost:10332', 'http://seed1t4.neo.org:20332',
                     'http://seed2t4.neo.org:20332', 'http://seed3t4.neo.org:20332',
                     'http://seed4t4.neo.org:20332', 'http://seed5t4.neo.org:20332']
    
    def __init__(self, tx_hash: Hash256Str, filename_and_path: str):
        self.tx_hash = tx_hash
        self.file_name_and_path = filename_and_path
        self.pool = gevent.pool.Pool(len(self.endpoint_urls) * self.concurrency_per_endpoint)
    
    def download(self):
        dummy_contract_hash = Hash160Str('0x0000000000000000000000000000000000000000')
        clients = [TestClient(target_url, dummy_contract_hash,
                              dummy_contract_hash, 'dummy_address',
                              'dummy_path', 'dummy_password',
                              with_print=False, verbose_return=True)
                   for target_url in self.endpoint_urls]
        endpoint_urls_for_load_balancing = clients * self.concurrency_per_endpoint
        random.shuffle(endpoint_urls_for_load_balancing)
        endpoint_url_generator = cycle(endpoint_urls_for_load_balancing)
        task_queue = gevent.queue.Queue()
        
        client = next(endpoint_url_generator)
        get_node_task: Greenlet = self.pool.spawn(client.getrawtransaction, self.tx_hash, verbose=True)
        task_queue.put((get_node_task, self.tx_hash))

        null_hash256 = Hash256Str('0x0000000000000000000000000000000000000000000000000000000000000000')
        with open(self.file_name_and_path, 'wb') as f:
            while task_queue.qsize():
                latest_task, tx_hash = task_queue.get()
                if latest_task is None:
                    break
                latest_task.join()
                assert latest_task.value, f"Failed to download file node {tx_hash}"
                tx_script = b64decode(latest_task.value[0]['script'])
                file, left_child_hash256, right_child_hash256 = ConvertFileAndScript.script_to_file_bytes(tx_script)
                f.write(file)
                logging.info(f"Downloaded file node {tx_hash}"
                             f"with left child {left_child_hash256} and right child {right_child_hash256}")
                if left_child_hash256 != null_hash256:
                    client = next(endpoint_url_generator)
                    get_node_task: Greenlet = self.pool.spawn(client.getrawtransaction, left_child_hash256, verbose=True)
                    task_queue.put((get_node_task, left_child_hash256))
                if right_child_hash256 != null_hash256:
                    client = next(endpoint_url_generator)
                    get_node_task: Greenlet = self.pool.spawn(client.getrawtransaction, right_child_hash256, verbose=True)
                    task_queue.put((get_node_task, right_child_hash256))


if __name__ == '__main__':
    assert FilePartition.calc_binary_tree_layer_and_count_last_layer_nodes(1) == (1, 1)
    assert FilePartition.calc_binary_tree_layer_and_count_last_layer_nodes(2) == (2, 1)
    assert FilePartition.calc_binary_tree_layer_and_count_last_layer_nodes(3) == (2, 2)
    assert FilePartition.calc_binary_tree_layer_and_count_last_layer_nodes(4) == (3, 1)
    assert FilePartition.calc_binary_tree_layer_and_count_last_layer_nodes(7) == (3, 4)
    assert FilePartition.calc_binary_tree_layer_and_count_last_layer_nodes(2 ** 50 - 1) == (50, 2 ** 49)
    assert FilePartition.calc_binary_tree_layer_and_count_last_layer_nodes(2 ** 50) == (51, 1)
    from tests.config import testnet_wallet_hash, testnet_wallet_address
    # uploader = Uploader('.gitignore', testnet_wallet_hash, testnet_wallet_address, 'testnet.json', '1', max_bytes=4)
    # root_tx_hash = uploader.upload()
    # print(root_tx_hash)
    # print(f"ne03fs://{root_tx_hash}?filename={uploader.filename}")
    # test_file_hash = Hash256Str("""0xbcdbe88f90d1e1a74ca76288f70249559e104e5bda398b884a62662f3540d526""")  # 4 bytes
    test_file_hash = Hash256Str("""0xc12ab251e915b54392f9b53a19990fd1aeed0238c6bc120aa9c52dea8c5ce4c1""")  # 4 bytes
    downloader = Downloader(test_file_hash, '.gitignore.download')
    downloader.download()
