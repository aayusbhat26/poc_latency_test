from huggingface_hub import HfFileSystem
import os

fs = HfFileSystem()
print(fs.ls("aayushbhat26/poc_testing_latency"))
