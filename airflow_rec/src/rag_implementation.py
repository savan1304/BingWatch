query = "lgbtq friendly?"

from langchain.llms import OpenAI
from langchain.chains import RetrievalQA

from langchain_pinecone import PineconeVectorStore
from langchain.embeddings import OpenAIEmbeddings
import os

os.environ['PINECONE_API_KEY'] = 'PINECONE_API_KEY'
os.environ["OPENAI_API_KEY"] = 'OPENAI_API_KEY'

vector_store = PineconeVectorStore(
    PineconeVectorStore.get_pinecone_index("bot"),
    embedding=OpenAIEmbeddings(),
    namespace="movies"
)
llm = OpenAI()

# Creating a RetrievalQA instance
qa = RetrievalQA.from_chain_type(
    llm=llm,
    chain_type="stuff",
    retriever=vector_store.as_retriever(),
    return_source_documents=True,
)

result = qa({"query": query})

# Printing the result
movie_ids = ''
for doc in result['source_documents']:
        movie_ids += f"{doc.metadata['unique_id']}"
        if doc != result['source_documents'][-1]:
          movie_ids += ", "

print(movie_ids)