import time
from achillesdb import AchillesClient

c = AchillesClient("localhost", 8180)
db_name = "test_multi_insert_bug"
try:
    c.delete_database(db_name)
except:
    pass

db = c.create_database(db_name)
c1 = db.create_collection("c1")
c2 = db.create_collection("c2")

c1.add_documents(ids=["1", "2"], documents=["a", "b"], embeddings=[[0.1], [0.1]])
time.sleep(1)
c2.add_documents(ids=["3", "4"], documents=["c", "d"], embeddings=[[0.1], [0.1]])

res1 = c1.query_documents(top_k=10, query_embedding=[0.1])
res2 = c2.query_documents(top_k=10, query_embedding=[0.1])

print("Res1 (first inserted):", len(res1))
print("Res2 (second inserted):", len(res2))
