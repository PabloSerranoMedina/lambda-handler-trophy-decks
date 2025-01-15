#Installing packages
import os

import pandas as pd
import numpy as np #for skewed random wait times

import requests



from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError 

import time
from io import StringIO
import random
import csv

#MySQL connection part
db_url = os.getenv("DATABASE_URL")
engine = create_engine(db_url)


def scrape_and_update_db():
    print('Starting data fetch using requests...')
    # Define the URL
    expansion='FDN' #I will leave these just in case I want to develop further the customizations of requests
    format='PremierDraft'
    url = f"https://www.17lands.com/data/trophies?expansion={expansion}&format={format}"

    try:
        # Fetch JSON data from the URL
        response = requests.get(url)
        if response.status_code != 200:
            print(f"Failed to fetch data: HTTP {response.status_code}")
            return
                # Parse JSON response
        trophy_deck_data = response.json()

        # Process rows and create a DataFrame
        rows = []
        for entry in trophy_deck_data:
            rows.append({
                'date': entry['time'],
                'expansion': expansion.lower(),
                'colors': entry['colors'],
                'losses': entry['losses'],
                'result_rank': shorten_rank(entry['end_rank']),
                'deck_id': entry['aggregate_id'],  # Use aggregate_id as Deck ID
            })

        df_new = pd.DataFrame(rows)
        print("Parsed trophy data into DataFrame.")

        # Filter out decks that are already in the main database

        query = "SELECT deck_id FROM trophy_decks;"

        existing_decks_df = pd.read_sql(query, engine)
        existing_decks = existing_decks_df['deck_id'].tolist()
        df_new = df_new[~df_new['deck_id'].isin(existing_decks)]
        
        # Insert the DataFrame directly into the database
        df_new.to_sql("trophy_decks", con=engine, if_exists="append", index=False)
        print(f"{len(df_new)} new entries added to the 'trophy_decks' table.")
        
        return df_new

    except Exception as e:
            print(f"An error occurred: {e}")
            return pd.DataFrame()

def expand_to_cards(df_new_decks):
    print("Starting expansion to cards...")

    if df_new_decks.empty:
        print("No new decks to process.")
        return

    total_decks = len(df_new_decks)
    deck_counter = 0

    # Loop through each link in the Deck column
    for _, row in df_new_decks.iterrows():

        #Creating the request url from the deck url
        deck_id = row['deck_id']
        deck_url = f"https://www.17lands.com/data/deck?draft_id={deck_id}"
        card_counts = {} #clean the dict for every deck

        try:
            response = requests.get(deck_url, timeout=20)
            if response.status_code != 200:
                print(f"Failed to fetch deck data: HTTP {response.status_code}, Deck ID: {deck_id}")
                continue

            deck_data = response.json()

            # Extract card data from the Maindeck group, add data from scrifall and then insert it in online database
            for group in deck_data["groups"]:
                if group["name"] == "Maindeck":  # Only process the "Maindeck" group
                    for card in group["cards"]:
                        card_details = deck_data["cards"].get(str(card))
                        if card_details:
                            # Unique key for a card in the deck
                            card_name = card_details["name"]
                            card_key = (deck_id, card_details["name"])
                            card_info = get_card_data(card_name)
                            if not card_info:
                                print(f"Failed to fetch data for card: {card_name}")
                                continue
                            # Count copies
                            if card_key in card_counts:
                                card_counts[card_key]["copies"] += 1
                            else:
                                card_tags = get_tags(card_name)
                                card_counts[card_key] = {
                                    "deck_id": deck_id,
                                    "card_name": card_info["name"],
                                    "copies": 1,  # Initial count
                                    "cmc": int(card_info["cmc"]),
                                    "type_line": card_info["type_line"],
                                    "color_identity": card_info["color_identity"],
                                    "set_code": card_info["set"],
                                    "rarity": shorten_rarity(card_info["rarity"]),
                                    "tags": card_tags 
                                    }
            # Insert card data for the current deck into the database
            if card_counts:
                deck_cards = pd.DataFrame(card_counts.values())
                try:
                    deck_cards.to_sql("trophy_cards", con=engine, if_exists="append", index=False)
                    print(f"Inserted {len(deck_cards)} cards for deck '{deck_id}' into 'trophy_cards'.")
                except Exception as e:
                    print(f"Error inserting data for deck '{deck_id}' into MySQL: {e}")

            # Log progress for every 10 decks processed
            deck_counter += 1
            if deck_counter % 10 == 0:
                print(f"Processed {deck_counter}/{total_decks} decks...")
            if deck_counter % 50 == 0:
                print("Paused for 5-6 min")
                time.sleep(random.uniform(300, 360)) #every 50 decks, scripts pauses for a while
                reconnect_engine()  # Refresh the database connection
                print("Resuming deck processing") 
        except Exception as e:
            print(f"Failed to load or scrape {deck_url}: {e}")

        finally:
            # random pauses to avoid hitting the server too hard and avoid banning
            delay = np.random.exponential(scale=1)  # `scale=1` controls the skew; smaller numbers make the skew more pronounced
            delay = min(max(0.05, delay), 0.2)
            time.sleep(delay)

def get_card_data(card_name):
    # Replace spaces with "+" for the API request
    formatted_name = card_name.replace(" ", "+")
    url = f'https://api.scryfall.com/cards/named?fuzzy={formatted_name}'
    
    try:
        response = requests.get(url, timeout=10)  # Add timeout for the request
        response.raise_for_status()  # Raise an exception for HTTP errors

        # Parse the response JSON and extract specific fields
        data = response.json()
        card_info = {
            'name': data.get('name'),
            'cmc': data.get('cmc'),
            'type_line': data.get('type_line'),
            'color_identity': ','.join(data.get('color_identity', [])),
            'set': data.get('set'),
            'rarity': data.get('rarity')
        }
        return card_info

    except requests.exceptions.RequestException as e:  # Catch network and HTTP errors
        print(f"Failed to fetch data for {card_name}: {e}")
        return None

def shorten_rank(rank):
    if pd.isna(rank):
        return None
    parts = rank.split('-')
    if len(parts) == 2:
        return parts[0][0].upper() + parts[1]  # Capitalize first letter and append the number
    return rank

def shorten_rarity(rarity):
    # Map full rarity names to abbreviations
    rarity_map = {
        'common': 'C',
        'uncommon': 'U',
        'rare': 'R',
        'mythic': 'M'
    }
    return rarity_map.get(rarity.lower(), rarity)  # Default to original if not found

def get_tags(card_name):
    url = "https://pavloatlas.com/trophy_decks/cards_tagged.csv"
    try:
        # Fetch the CSV file from your server
        response = requests.get(url, timeout=10)
        response.raise_for_status()  # Raise an error for bad status codes

        # Parse the CSV content
        csv_content = response.text
        reader = csv.DictReader(StringIO(csv_content))
        
        for row in reader:
            if row['name'] == card_name:
                return row['tags'].strip()
    except Exception as e:
        print(f"Error fetching tags from server: {e}")
    
    return ''

def reconnect_engine():
    global engine
    try:
        engine.dispose()  # Dispose of the existing engine
        engine = create_engine("mysql+pymysql://u901708261_pavlo:A3c1d5f7e2@pavloatlas.com/u901708261_trophy_decks")
        print("Database connection refreshed.")
    except Exception as e:
        print(f"Failed to reconnect to the database: {e}")

def lambda_function():
    print("Starting scheduled tasks...")
    df_new_decks = scrape_and_update_db()  # Scrape and update the decks database
    expand_to_cards(df_new_decks)       # Process new decks and expand to cards database
    print("Scheduled tasks completed.")

lambda_function()
