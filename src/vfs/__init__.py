from fs.memoryfs import MemoryFS

m = MemoryFS()
m.tree()

m.makedirs("/home/user")
m.makedirs("/home/user2")
m.makedirs("/home2")
m.upload()
m.setinfo()

m.tree()