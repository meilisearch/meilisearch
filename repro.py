import requests

HOST = "http://localhost:7700"
HEADERS = {"X-Meili-API-Key": "masterKey"}

def create_documents():
    # Create a document with 65,535 unique fields
    doc1 = {"id": 1}
    for i in range(65535):
        doc1[f"field_{i}"] = f"value_{i}"

    # Send the first document
    requests.post(
        f"{HOST}/indexes/test/documents",
        headers=HEADERS,
        json=[doc1]
    )
    print("Add one document with 65,535 unique fields")

    # Create a second document with a new unique field
    doc2 = {
        "id": 2,
        "new_unique_field": "new_value"
    }

    # Send the second document
    requests.post(
        f"{HOST}/indexes/test/documents",
        headers=HEADERS,
        json=[doc2]
    )
    print("Add a second document with a new unique field")

if __name__ == "__main__":
    create_documents()
