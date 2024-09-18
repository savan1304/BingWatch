from openai import OpenAI
import os
from dotenv import load_dotenv
import base64
import streamlit as st
from langchain_pinecone import PineconeVectorStore
from langchain.vectorstores import Pinecone as pic
from langchain.embeddings import OpenAIEmbeddings
import os

load_dotenv()
api_key = os.getenv("openai_api_key")
pinecone_key = os.getenv("PINECONE_API_KEY")

client = OpenAI(api_key=api_key)

def speech_to_text(audio_data):
    try:
        with open(audio_data, "rb") as audio_file:
            transcript = client.audio.transcriptions.create(
                model="whisper-1",
                response_format="text",
                file=audio_file
            )
        return transcript, None        
    except Exception as e:
        return None, str(e)
    
def query_to_id(query, content_type):

    if content_type == "tv_show":
        namespace = "tv shows"
    else:
        namespace = "movies"

    embeddings = OpenAIEmbeddings()
    vector_store = PineconeVectorStore(
        pic.get_pinecone_index("bot"),
        embedding=embeddings,
        namespace=namespace
    )

    results = vector_store.similarity_search(query, k=16)
    ids = ','.join(result.metadata['unique_id'] for result in results)

    return ids    


    

