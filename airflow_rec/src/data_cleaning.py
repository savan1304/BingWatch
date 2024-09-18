import pandas as pd
import uuid
import ast

def clean(csv,csv1,csv2):
    try:
        final_df=pd.read_csv(csv)
    except Exception as e:
        print(f"Failed to read CSV: {e}")
        return
    
    try:
        movies_df = final_df[final_df['type'] == 'Movie']
        movies_df['duration'] = movies_df['duration'].str.replace(' min', '')
        movies_df['duration'] = pd.to_numeric(movies_df['duration'], errors='coerce')
        movies_df.fillna('NA', inplace=True)
        movies_df.drop(columns=['show_id','error'], inplace=True)
        movies_df = movies_df.drop_duplicates(subset=['title', 'trailer'], keep='first')
        movies_df['keywords'] = movies_df['keywords'].apply(
        lambda x: ', '.join(x) if isinstance(x, list) else ', '.join(ast.literal_eval(x)) if isinstance(x, str) else x
        )

    except Exception as e:
        print(f"Failed in {e}")  
        return 
    
    try:
        tv_shows_df = final_df[final_df['type'] == 'TV Show']
        tv_shows_df.fillna('NA', inplace=True)
        tv_shows_df.drop(columns=['show_id','error'], inplace=True)
        tv_shows_df = tv_shows_df.drop_duplicates(subset=['title', 'trailer'], keep='first')
        tv_shows_df['keywords'] = tv_shows_df['keywords'].apply(
        lambda x: ', '.join(x) if isinstance(x, list) else ', '.join(ast.literal_eval(x)) if isinstance(x, str) else x
        )
    except Exception as e:
        print(f"Failed in {e}")  
        return 


    try:
        movies_df['sentences'] = None
        
        movies_df['unique_id'] = [str(uuid.uuid4()) for _ in range(len(movies_df))]
        
        # Generate a sentence for each row in the DataFrame and update the DataFrame
        for index, row in movies_df.iterrows():
            sentence = (
                f"The {row['type']} titled {row['title']} released in {row['release_year']} "
                f"is available on {row['available_on']}. The rating for this {row['type']} is {row['rating']}."
                f"The director and cast information is {row['director']}, {row['cast']}. "
                f"It can be described as {row['listed_in']}, {row['keywords']}."
                f"Here is a small summary: {row['description']}."
            )
            movies_df.at[index, 'sentences'] = sentence
        
        movies_df.to_csv(csv1 ,sep=',', index=False, encoding='utf-8')

    except Exception as e:
        print(f"Failed in {e}")  
        return    


    try:
        tv_shows_df['sentences'] = None
        
        # Initialize a new column for unique identifiers
        tv_shows_df['unique_id'] = [str(uuid.uuid4()) for _ in range(len(tv_shows_df))]
        
        # Generate a sentence for each row in the DataFrame and update the DataFrame
        for index, row in tv_shows_df.iterrows():
            sentence = (
                f"The {row['type']} titled {row['title']} released in {row['release_year']} "
                f"is available on {row['available_on']}. The rating for this {row['type']} is {row['rating']}."
                f"The director and cast information is {row['director']}, {row['cast']}. "
                f"It can be described as {row['listed_in']}, {row['keywords']}."
                f"Here is a small summary: {row['description']}."
            )
            tv_shows_df.at[index, 'sentences'] = sentence
        
    
        tv_shows_df.to_csv(csv2 ,sep=',', index=False, encoding='utf-8')

    except Exception as e:
        print(f"Failed in {e}")  
        return     

if __name__ == '__main__':
    csv='/opt/airflow/src/final_dataset.csv'
    csv1='/opt/airflow/src/final_movies.csv'
    csv2='/opt/airflow/src/final_tvshows.csv'
    







