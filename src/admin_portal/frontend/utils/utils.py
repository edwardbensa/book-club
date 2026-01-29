# Import modules
from bson import ObjectId

def convert_objectids(doc):
    if isinstance(doc, dict):
        return {k: convert_objectids(v) for k, v in doc.items()}
    elif isinstance(doc, list):
        return [convert_objectids(i) for i in doc]
    elif isinstance(doc, ObjectId):
        return str(doc)
    else:
        return doc

def get_collection_names(db):
    return db.list_collection_names()

def fetch_documents(db, collection_name, limit=100):
    return list(db[collection_name].find().limit(limit))

def insert_document(db, collection_name, doc):
    return db[collection_name].insert_one(doc)

def update_document(db, collection_name, doc_id, updates):
    return db[collection_name].update_one({"_id": ObjectId(doc_id)}, {"$set": updates})

def delete_document(db, collection_name, doc_id):
    return db[collection_name].delete_one({"_id": ObjectId(doc_id)})
