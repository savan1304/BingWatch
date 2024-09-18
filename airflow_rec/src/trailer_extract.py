import pandas as pd
from dotenv import load_dotenv
import os
import requests
import time


load_dotenv()

api_key = os.getenv("TMDB_API_KEY")

RATE_LIMIT = 40  # 40 requests per 10 seconds
REQUEST_SPACING = 5



def fetch_trailer_and_thumbnail(title, type, directors, actors, api_key):
    # Initial search using title and release year
    if type=='Movie':
        search_url = f"https://api.themoviedb.org/3/search/movie?api_key={api_key}&query={title}"
        search_response = requests.get(search_url).json()
        
        if 'results' not in search_response or not search_response['results']:
            return None, None, None, f"No results found for {title}"

        ## Splitting directors and actors for multiple entries
        director_list = [d.strip() for d in directors.split(",")] if pd.notna(directors) else []
        actor_list = [a.strip() for a in actors.split(",")] if pd.notna(actors) else []
        
        # Iterating over results to find the best match
        for result in search_response['results']:
            movie_id = result['id']
            details_url = f"https://api.themoviedb.org/3/movie/{movie_id}?api_key={api_key}&append_to_response=credits,videos,keywords"
            details_response = requests.get(details_url).json()
            

            # Checking matches in director and actor
            if 'credits' not in details_response:
                continue
            found_director = False
            if 'crew' in details_response['credits']:
                for crew_member in details_response['credits']['crew']:
                    if crew_member.get('job') == 'Director' and crew_member.get('name') in director_list:
                        found_director = True
                        break

            # Checking for actor matches in the cast section
            found_actor = False
            if 'cast' in details_response['credits']:
                for cast_member in details_response['credits']['cast']:
                    if cast_member.get('name') in actor_list:
                        found_actor = True
                        break
                    

            # If a director or a main actor matches, considering this a successful match
            if found_director or found_actor:
                videos = details_response.get('videos', {}).get('results', [])
                poster_path = details_response.get('poster_path')
                keywords_list=details_response.get('keywords', {}).get('keywords', [])
                trailer_url = next((f"https://www.youtube.com/watch?v={video['key']}" for video in videos if video['type'] == "Trailer"), None)
                thumbnail_url = f"https://image.tmdb.org/t/p/original{poster_path}" if poster_path else None
                keywords = [keyword['name'] for keyword in keywords_list] if keywords_list else []
                
                if trailer_url or thumbnail_url or keywords:
                    return trailer_url, thumbnail_url, keywords, None
        
    else:
        search_url = f"https://api.themoviedb.org/3/search/tv?api_key={api_key}&query={title}"
        search_response = requests.get(search_url).json()
        
        if 'results' not in search_response or not search_response['results']:
            return None, None, None, f"No results found for {title}"

        ## Splitting directors and actors for multiple entries
        director_list = [d.strip() for d in directors.split(",")] if pd.notna(directors) else []
        actor_list = [a.strip() for a in actors.split(",")] if pd.notna(actors) else []
        
        # Iterating over results to find the best match
        for result in search_response['results']:
            tv_id = result['id']
            details_url = f"https://api.themoviedb.org/3/tv/{tv_id}?api_key={api_key}&append_to_response=credits,videos,keywords"
            details_response = requests.get(details_url).json()
        

            # Checking matches in director and actor
            if 'credits' not in details_response:
                continue
            found_director = False
            if 'crew' in details_response['credits']:
                for crew_member in details_response['credits']['crew']:
                    if crew_member.get('job') == 'Director' and crew_member.get('name') in director_list:
                        found_director = True
                        break

            # Checking for actor matches in the cast section
            found_actor = False
            if 'cast' in details_response['credits']:
                for cast_member in details_response['credits']['cast']:
                    if cast_member.get('name') in actor_list:
                        found_actor = True
                        break
                    

            # If a director or a main actor matches, considering this a successful match
            if found_director or found_actor:
                videos = details_response.get('videos', {}).get('results', [])
                poster_path = details_response.get('poster_path')
                keywords_list= details_response.get('keywords', {}).get('results', [])
                trailer_url = next((f"https://www.youtube.com/watch?v={video['key']}" for video in videos if video['type'] == "Trailer"), None)
                thumbnail_url = f"https://image.tmdb.org/t/p/original{poster_path}" if poster_path else None
                keywords = [keyword['name'] for keyword in keywords_list] if keywords_list else []
                
                if trailer_url or thumbnail_url or keywords:
                    return trailer_url, thumbnail_url, keywords, None
        
                    
            
    return None, None, None, f"No suitable matches found for {title} with given criteria."        


def process_csv(input_csv, output_csv, api_key):
    df = pd.read_csv(input_csv)
    df['trailer'] = None
    df['thumbnail'] = None
    df['keywords'] = None
    df['error'] = None
    request_count = 0

    for index, row in df.iterrows():
        if request_count >= RATE_LIMIT:
            print("Rate limit reached, sleeping...")
            time.sleep(REQUEST_SPACING)
            request_count = 0

        trailer_url, thumbnail_url, keywords, error = fetch_trailer_and_thumbnail(
            row['title'], row['type'], row['director'], row['cast'], api_key)
        df.at[index, 'trailer'] = trailer_url
        df.at[index, 'thumbnail'] = thumbnail_url
        df.at[index, 'keywords'] = keywords
        df.at[index, 'error'] = error if error else (None if trailer_url and thumbnail_url else "Trailer or Thumbnail not available")
        request_count += 1

    df.to_csv(output_csv, index=False)
    print("Processing complete. Data written to:", output_csv)


if __name__ == "__main__":
    input_csv= '/opt/airflow/src/dataset.csv'
    output_csv= '/opt/airflow/src/final_dataset.csv'