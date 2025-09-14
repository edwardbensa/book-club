# Import modules
import json
import streamlit as st
from bson import ObjectId
import pandas as pd
from src.admin_interface.utils.utils import fetch_documents, insert_document, convert_objectids
from src.db.utils.connectors import connect_mongodb

db, client = connect_mongodb()

st.sidebar.title("Nommo Admin Portal")
collection = st.sidebar.selectbox("Select Collection", sorted(db.list_collection_names()))
operation = st.sidebar.radio("Operation", ["View", "Add", "Update", "Delete"])

if operation == "View":
    docs = fetch_documents(db, collection)
    df = pd.DataFrame([convert_objectids(doc) for doc in docs])
    st.dataframe(df)

elif operation == "Add":
    st.subheader(f"Add new document to '{collection}'")
    new_doc = st.text_area("Paste JSON here")
    if st.button("Insert"):
        try:
            doc = json.loads(new_doc)
            doc["_id"] = ObjectId()
            insert_document(db, collection, doc)
            st.success("Document inserted.")
        except json.JSONDecodeError as e:
            st.error(f"Invalid JSON: {e}")
        except TypeError as e:
            st.error(f"Type error: {e}")
        except ValueError as e:
            st.error(f"Value error: {e}")

# Similar logic for Edit and Delete...
