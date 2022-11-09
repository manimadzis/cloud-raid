import hashlib

from Crypto import Random
from Crypto.Cipher import AES

import entity
from crypto.cipher_base import CipherBase


class Aes(CipherBase):
    def __init__(self, key: entity.Key):
        self._key = key
        self._hashed_key = hashlib.sha256(key.key.encode('ascii')).digest()

    @staticmethod
    def _pad(s):
        return s + b'\0' * (AES.block_size - len(s) % AES.block_size)

    def encrypt(self, data: bytes) -> bytes:
        message = self._pad(data)
        iv = Random.new().read(AES.block_size)
        cipher = AES.new(self._hashed_key, AES.MODE_CBC, iv)
        return iv + cipher.encrypt(message)

    def decrypt(self, data: bytes) -> bytes:
        iv = data[:AES.block_size]
        cipher = AES.new(self._hashed_key, AES.MODE_CBC, iv)
        plaintext = cipher.decrypt(data[AES.block_size:])
        return plaintext.rstrip(b'\0')

    def key(self):
        return self._key
