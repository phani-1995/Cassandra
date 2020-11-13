import pandas as pd

chunk_size = 500
batch_no = 1

for chunk in pd.read_csv("/home/phani/PycharmProjects/Cassandra/project/transactions.csv", chunksize=chunk_size):
    chunk.to_csv("transactions" + str(batch_no) +'.csv', index=False)
    batch_no+=1

