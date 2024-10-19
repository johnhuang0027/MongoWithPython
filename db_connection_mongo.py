#-------------------------------------------------------------------------
# AUTHOR: John Huang
# FILENAME: db_connection_mongo
# SPECIFICATION: methods to connect, insert, update, delete, and retrieve indexes from a database
# FOR: CS 4250- Assignment #2
# TIME SPENT: 2 hours
#-----------------------------------------------------------*/

#IMPORTANT NOTE: DO NOT USE ANY ADVANCED PYTHON LIBRARY TO COMPLETE THIS CODE SUCH AS numpy OR pandas. You have to work here only with
# standard arrays

from pymongo import MongoClient

def connectDataBase():

    client = MongoClient("mongodb://localhost:27017/")
    db = client['test']
    return db

def createDocument(col, docId, docText, docTitle, docDate, docCat):

    # create a dictionary (document) to count how many times each term appears in the document.
    # Use space " " as the delimiter character for terms and remember to lowercase them.
    term_counts = {}
    words = docText.lower().split()
    for word in words:
        if word in term_counts:
            term_counts[word] +=1
        else:
            term_counts[word] = 1

    # create a list of dictionaries (documents) with each entry including a term, its occurrences, and its num_chars. Ex: [{term, count, num_char}]
    terms_list = [{"term": term, "count": count, "num_chars": len(term)} for term, count in term_counts.items()]

    #Producing a final document as a dictionary including all the required fields
    doc = {
        "_id": int(docId),
        "text": docText,
        "title": docTitle,
        "date": docDate,
        "category": docCat,
        "terms": terms_list
    }

    # Insert the document
    col.insert_one(doc)
    print(f"Document with ID {docId} inserted.")

def deleteDocument(col, docId):

    # Delete the document from the database
    result = col.delete_one({"_id": int(docId)})
    if result.deleted_count > 0:
        print(f"Document with ID {docId} deleted.")
    else:
        print(f"Document with ID {docId} not found.")

def updateDocument(col, docId, docText, docTitle, docDate, docCat):

    # Delete the document
    deleteDocument(col, docId)

    # Create the document with the same id
    createDocument(col, docId, docText, docTitle, docDate, docCat)

def getIndex(col):

    # Query the database to return the documents where each term occurs with their corresponding count. Output example:
    # {'baseball':'Exercise:1','summer':'Exercise:1,California:1,Arizona:1','months':'Exercise:1,Discovery:3', ...}
    # We are simulating an inverted index here in memory.
    docs = col.find()

    inverted_index = {}

    for doc in docs:
        title = doc["title"]
        for term_entry in doc["terms"]:
            term = term_entry["term"]
            count = term_entry["count"]
            
            # Build the inverted index
            if term in inverted_index:
                inverted_index[term] += f", {title}:{count}"
            else:
                inverted_index[term] = f"{title}:{count}"
    sorted_index = dict(sorted(inverted_index.items()))

    return sorted_index