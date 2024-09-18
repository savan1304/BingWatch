# BINGEWATCH

**Recommendation System** for personalised recommendations for OTT Content.

## Technologies Used

[![Google Cloud](https://img.shields.io/badge/Google_Cloud-4285F4?style=for-the-badge&logo=googlecloud&logoColor=white)](https://cloud.google.com)
[![Python](https://img.shields.io/badge/python-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://python.org)
[![Pinecone](https://img.shields.io/badge/Pinecone-FF6F00?style=for-the-badge&logo=pinecone&logoColor=white)](https://www.pinecone.io)
[![Streamlit](https://img.shields.io/badge/streamlit-FF4B4B?style=for-the-badge&logo=streamlit&logoColor=white)](https://streamlit.io)
[![JWT](https://img.shields.io/badge/JWT-black?style=for-the-badge&logo=jsonwebtokens&logoColor=white)](https://jwt.io)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-4169E1?style=for-the-badge&logo=postgresql&logoColor=white)](https://www.postgresql.org)
[![The Movie Database](https://img.shields.io/badge/TMDB-01D277?style=for-the-badge&logo=themoviedatabase&logoColor=white)](https://www.themoviedb.org)
[![Kaggle](https://img.shields.io/badge/Kaggle-20BEFF?style=for-the-badge&logo=kaggle&logoColor=white)](https://www.kaggle.com)
[![Apache Airflow](https://img.shields.io/badge/Apache_Airflow-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white)](https://airflow.apache.org)
[![FastAPI](https://img.shields.io/badge/FastAPI-009688?style=for-the-badge&logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com)
[![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)](https://www.docker.com)

## Data Sources

- **Kaggle**: Curated datasets from a broad range of streaming content.
- **TMDb API**: Real-time data access to movies and TV shows metadata.


## Project Structure

``` 
.github
   |-- workflows
   |   |-- ci.yml
.gitignore
Dockerfile
README.md
airflow_rec
   |-- dags
   |   |-- __init__.py
   |   |-- data_extraction.py
   |-- src
   |   |-- __init__.py
   |   |-- data_cleaning.py
   |   |-- rag_implementation.py
   |   |-- similarity_search.py
   |   |-- snowflake_upload.py
   |   |-- trailer_extract.py
bingewatch.py
contentapi.py
db_module
   |-- __init__.py
   |-- db.py
   |-- user_handler.py
docker-compose.yaml
main.py
models
   |-- __init__.py
   |-- pydantic_validators.py
   |-- user_model.py
requirements.txt
unit_testing.py
utils.py
```

## How to Run Application Locally

1. Clone the repository.
2. Install the dependencies.
```
pip install -r requirements.txt
```
3. Run the Dockerfile to generate the image and run the Docker compose file to bring up the instances.
```
docker build -t <image_name> .
```
```
docker compose up -d
```
This is create an airflow instance, postgresDB, streamlit for frontend and fastapi for backend.

## References & Dataset

Dataset: https://www.kaggle.com/datasets/shivamb/netflix-shows/data
- RAG - https://www.pinecone.io/learn/retrieval-augmented-generation/
- Speech Input - https://blog.futuresmart.ai/building-a-conversational-voice-chatbot-integrating-openais-speech-to-text-text-to-speech
- Recommendation systems - https://medium.com/dailymotion/reinvent-your-recommender-system-using-vector-database-and-opinion-mining-a4fadf97d020



