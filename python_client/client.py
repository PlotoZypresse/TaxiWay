import socket
import struct


class JobQueueClient:
    def __init__(self) -> None:
        pass

    # notes for building. Use struct.pack to convert data into bytes
    # use socket for connecting to the server
    def submitJob(self, data):
        """
        submitJob takes data that should be stored in the job queue as input.
        The data is converted to bytes using struct.pack.
        Before sending the data we need to add the 1 byte opcode(1) and
        the 4 data length bytes. Resulting in 1byte + 4bytes + data bytes
        being send. The server returns the job id of the job or an error code.
        To get "arbitrary input" we need to first convert the data to utf-8
        """
        pass

    def getJob(self):
        """
        To recive the first item from the job queue we only send the correct opcode(2).
        After that the server will send back job id and the data we stored.
        Meaning we recive back 4bytes + 4bytes + databytes or an error code.
        Using struckt.unpack we can then reconstruct the data.
        """
        pass
