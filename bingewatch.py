import streamlit as st
from sqlalchemy import create_engine, select, ForeignKey, DateTime
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String
from passlib.context import CryptContext  # Import CryptContext
from db_module.user_handler import create_user
import requests
from audio_recorder_streamlit import audio_recorder
from streamlit_float import *
from utils import speech_to_text, query_to_id
import os
import uuid
from sqlalchemy.dialects.postgresql import UUID
import datetime

float_init()

# Defining the User model
Base = declarative_base()
class User(Base):
    __tablename__ = 'users'
    username = Column(String, primary_key=True)
    password = Column(String)

class Like(Base):
    __tablename__ = 'likes'
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    username = Column(String, ForeignKey('users.username'))
    content_id = Column(String)
    content_type= Column(String)

# Database connection
DATABASE_URL = "postgresql://airflow:airflow@postgres/airflow"
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Initializing password context
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# Initializing session state for loggedIn and userCreated flags
if 'loggedIn' not in st.session_state:
    st.session_state['loggedIn'] = False
if 'showCreateUser' not in st.session_state:
    st.session_state['showCreateUser'] = False

headerSection = st.container()
userSection = st.container()

def verify_user(username, entered_password):
    with SessionLocal() as session:
        user = session.query(User).filter(User.username == username).first()
        if user and pwd_context.verify(entered_password, user.password):
            return True
        else:
            return False

def LoggedIn_Clicked(username, password):
    if verify_user(username, password):
        st.session_state['loggedIn'] = True
        st.session_state['username'] = username
        st.session_state['username'] = username
    else:
        st.error("Invalid username or password")

def LoggedOut_Clicked():
    st.session_state['loggedIn'] = False

def show_login_page():
    st.subheader("Login")
    username = st.text_input("Username", key="login_username", placeholder="Enter your username")
    password = st.text_input("Password", key="login_password", placeholder="Enter your password", type="password")
    st.button("Login", on_click=LoggedIn_Clicked, args=(username, password))
    if st.button("Create New Account"):
        st.session_state['showCreateUser'] = True
        st.experimental_rerun()

def show_create_user_page():
    st.subheader("Create User")
    with st.form(key='create_user_form'):
        username = st.text_input("Username", key="create_username", placeholder="Enter your username")
        password = st.text_input("Password", key="create_password", placeholder="Enter your password", type="password")
        submit_button = st.form_submit_button("Create User")
        if submit_button:
            hashed_password = pwd_context.hash(password)
            user_details = {"username": username, "password": hashed_password}
            result = create_user(user_details, engine)
            if result['status_code'] == 201:
                st.success("User created successfully. Please login.")
                st.session_state['showCreateUser'] = False
                st.session_state['userCreated'] = True
                st.experimental_rerun()
            else:
                st.error(f"Failed to create user: {result['description']}")


def store_like(username, content_id, content_type):
    with SessionLocal() as session:
        existing_like = session.query(Like).filter_by(username=username, content_id=content_id, content_type=content_type).first()
        if not existing_like:
            like = Like(username=username, content_id=content_id, content_type=content_type)
            session.add(like)
            session.commit()
            return "Liked successfully"
        else:
            return "Already liked"
        
def get_liked_content(username):
    with SessionLocal() as session:
        likes= session.query(Like).filter(Like.username == username).all()
    if not likes:
        return []
    content_ids_by_type = {'movie': [], 'tv_show': []}
    for like in likes:
        content_ids_by_type[like.content_type].append(like.content_id)

    liked_content_details = []
    for content_type, ids in content_ids_by_type.items():
        if ids:
            # Fetching content details using FastAPI
            response = requests.get(f'http://fastapi:8001/content', params={'unique_ids': ids, 'content_type': content_type})
            if response.status_code == 200:
                content_details = response.json()
                liked_content_details.extend(content_details)
            else:
                print(f"Failed to fetch {content_type} details: {response.text}")
    
    return liked_content_details



def clear_input():
    """ Clear the input field by resetting the session state variable. """
    st.session_state.query = ''

def clear_input_name():
    """ Clear the input field by resetting the session state variable. """
    st.session_state.movie_name = ''    
# Create empty space on the left to push buttons to the right
def show_main_page():
    st.button("Logout", on_click=LoggedOut_Clicked)
    st.title('Binge Watch!')
    with st.popover("Pick a Type!"):
        content_type = st.radio("", ["movie", "tv_show"], index=0, on_change=clear_input)

# Allowing user to enter a comma-separated list of IDs

    if 'transcript_query' not in st.session_state:
        st.session_state.transcript_query = ''
    input1, input2 = st.columns([0.8, 0.2])
    with input2:
        audio_bytes = audio_recorder()

    if audio_bytes:
        with st.spinner("Transcribing..."):
            # Writing the audio bytes to a temporary file
            webm_file_path = "temp_audio.mp3"
            with open(webm_file_path, "wb") as f:
                f.write(audio_bytes)

            # Converting the audio to text using the speech_to_text function
            transcript = speech_to_text(webm_file_path)
            if transcript:
                st.session_state.transcript_query = transcript[0].strip()
                os.remove(webm_file_path)

    with input1:

        if st.session_state.transcript_query:
            # If there is a transcription, updating the query and clearing the transcription variable
            query = st.text_input('What do you feel like?', value=st.session_state.transcript_query, key="query")
            st.session_state.transcript_query = ''
        else:
            query = st.text_input('What do you feel like?', value=st.session_state.get('query', ''), key="query")

    if query:

        unique_ids = query_to_id(query, content_type)
        unique_ids_list = unique_ids.split(',')
        
        # Sending a GET request to the FastAPI endpoint with the list of IDs
        response = requests.get(f'http://fastapi:8001/content', params={'unique_ids': unique_ids_list, 'content_type': content_type})
        
        if response.status_code == 200:
            movies = response.json()
            
            # Defining the number of columns for the grid
            columns_per_row = 4
            row = None
            
            for index, movie in enumerate(movies):
                # Creating a new row every 'columns_per_row' movies
                if index % columns_per_row == 0:
                    row = st.columns(columns_per_row)
                
                # Determinng the current position in the row
                col = row[index % columns_per_row]
                
                # Displaying movie details in the corresponding column
                with col:
                    if movie['thumbnail'] and movie['thumbnail'] != "No thumbnail available":
                        st.image(movie['thumbnail'], width=150)
                    else:
                        st.error("Thumbnail not available")
                    st.subheader(f"{movie['title']}")
                    st.write(f"Director: {movie['director']}")
                    st.write(f"Cast: {movie['cast_member']}")
                    st.write(f"Release Year: {movie['release_year']}")
                    st.write(f"Duration: {movie['duration']}")
                    st.write(f"Available on: {movie['available_on']}")
                    if movie['trailer'] != "No trailer available":
                        st.video(movie['trailer'], format='video/mp4', start_time=0)
                    else:
                        st.error("Trailer not available")

                    btn_col1, btn_col2 = st.columns(2)
                    st.markdown("---")
                    with btn_col1:
                        if st.button(f"👍",key=f"like_{movie.get('unique_id', index)}"):
                            response=store_like(st.session_state['username'], movie.get('unique_id'), content_type)
                            if response == "Liked successfully":
                                st.write('❤️')
                            elif response == "Already liked":
                                st.warning("You have already liked this movie.")
                    with btn_col2:
                        if st.button(f"👎",key=f"dislike_{movie.get('unique_id', index)}"):
                            st.write('💔')
        else:
            st.error("Result not found!")   
    else:
        return None        
    

def show_search_page():
    st.title("Search and Watch!")

    with st.popover("Pick a Type!"):
        content_type = st.radio("", ["movie", "tv_show"], index=0, on_change=clear_input_name)


    if 'movie_name' not in st.session_state:
        st.session_state.movie_name = ''

    input1, input2 = st.columns([0.8, 0.2])
    with input2:
        audio_bytes = audio_recorder()

    if audio_bytes:
        with st.spinner("Transcribing..."):
            # Writing the audio bytes to a temporary file
            webm_file_path = "temp_audio.mp3"
            with open(webm_file_path, "wb") as f:
                f.write(audio_bytes)

            # Converting the audio to text using the speech_to_text function
            transcript, error = speech_to_text(webm_file_path)
            if transcript:
                st.session_state.movie_name = transcript.strip()
                os.remove(webm_file_path)
            elif error:
                st.warning("Sorry, I couldn't catch that.")   

            else:
                return None     

    with input1:
        movie_name = st.text_input("Enter movie name to search:", value=st.session_state.get('movie_name', ''), key="movie_name")
    
    
    if st.button("Search"):
        response = requests.get(f"http://fastapi:8001/content", params={"name": movie_name ,'content_type': content_type})
        if response.status_code == 200:
            movies = response.json()
            if movies:  # Checking if the movies list is not empty
                columns_per_row = 4
                row = None
                
                for index, movie in enumerate(movies):
                    if index % columns_per_row == 0:
                        row = st.columns(columns_per_row)  # Creating a new row every 'columns_per_row' movies
                    
                    col = row[index % columns_per_row]  # Determining the current position in the row
                    
                    with col:
                        # Displaying movie details in the corresponding column
                        thumbnail = movie.get('thumbnail', "No thumbnail available")
                        if thumbnail != "No thumbnail available":
                            st.image(thumbnail, width=150)
                        else:
                            st.error("Thumbnail not available")

                        st.subheader(f"{movie.get('title', 'Title not available')}")
                        st.write(f"Director: {movie.get('director', 'Director information not available')}")
                        st.write(f"Cast: {movie.get('cast_member', 'Cast information not available')}")
                        st.write(f"Release Year: {movie.get('release_year', 'Release year not available')}")
                        st.write(f"Available on: {movie.get('available_on', 'Availability information not available')}")

                        trailer = movie.get('trailer', "No trailer available")
                        if trailer != "No trailer available":
                            st.video(trailer, format='video/mp4', start_time=0)
                            st.markdown("---")
                        else:
                            st.error("Trailer not available")
                            st.markdown("---")
            else:
                st.error("No movies found matching the criteria.")
        else:
            st.error("Failed to fetch movies.")
    
def show_history_page():
    st.title("Your content!")
    username = st.session_state['username']  # Making sure this is set when the user logs in
    liked_contents = get_liked_content(username)
    
    if not liked_contents:
        st.write("You haven't liked any content yet.")
    else:
        columns_per_row = 4
        row = None
                
        for index, content in enumerate(liked_contents):
            if index % columns_per_row == 0:
                row = st.columns(columns_per_row)  # Creating a new row every 'columns_per_row' movies
            
            col = row[index % columns_per_row]  # Determining the current position in the row
            
            with col:
                st.subheader(f"{content['title']}")
                st.image(content['thumbnail'], use_column_width=True)
                st.write(f"Director: {content['director']}")
                st.write(f"Cast: {content['cast_member']}")
                st.write(f"Release Year: {content['release_year']}")
                st.write(f"Duration: {content['duration']}")
                st.write(f"Available on: {content['available_on']}")
                if content['trailer'] != "No trailer available":
                    st.video(content['trailer'], format='video/mp4')
                st.markdown("---")

def main():
    if not st.session_state['loggedIn']:
        if st.session_state['showCreateUser']:
            show_create_user_page()
        else:
            show_login_page()
    else:
        pages = {
            "Main Page": show_main_page,
            "Search Page": show_search_page,
            "History Page": show_history_page
            # Add more pages as needed
        }
        st.sidebar.title("Navigation")
        page = st.sidebar.radio("", list(pages.keys()))
        pages[page]()  # Displaying the selected page function

# Calling main function to start the app
main()