import time
import requests
import datetime
import json
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

# Load config 
with open('./config/config.json') as f:
    config = json.load(f)

# Globals
access_token = None
token_expiry = None

# Reference data caches
genres_map = {}
platforms_map = {}
game_modes_map = {}
themes_map = {}
player_perspectives_map = {}
involved_companies_map = {}

es = Elasticsearch(
    hosts=["http://localhost:9200"],
    http_auth=("kibana_system_user", "kibanapass123"),
    max_retries=10,
    retry_on_timeout=True
)

def get_access_token():
    """Get OAuth token from Twitch."""
    global access_token, token_expiry
    
    if access_token and token_expiry and datetime.datetime.now() < token_expiry:
        return access_token
    
    print("Getting access token...")
    response = requests.post("https://id.twitch.tv/oauth2/token", params={
        "client_id": config["IGDBClientId"],
        "client_secret": config["IGDBClientSecret"],
        "grant_type": "client_credentials"
    }, timeout=10)
    
    data = response.json()
    access_token = data["access_token"]
    token_expiry = datetime.datetime.now() + datetime.timedelta(seconds=data["expires_in"] - 300)
    print(f"Token obtained (expires in {data['expires_in']}s)")
    return access_token

def load_reference_data(endpoint, limit=500):
    """Load reference data from IGDB (genres, platforms, etc.)"""
    token = get_access_token()
    all_data = []
    offset = 0
    
    while True:
        query = f"fields id, name; limit {limit}; offset {offset};"
        
        try:
            response = requests.post(
                f"https://api.igdb.com/v4/{endpoint}",
                headers={
                    "Client-ID": config["IGDBClientId"],
                    "Authorization": f"Bearer {token}"
                },
                data=query,
                timeout=15
            )
            
            if response.status_code == 200:
                batch = response.json()
                if not batch:
                    break
                all_data.extend(batch)
                if len(batch) < limit:
                    break
                offset += limit
                time.sleep(0.3)  # Rate limiting
            else:
                print(f"Error loading {endpoint}: {response.status_code}")
                break
                
        except Exception as e:
            print(f"Error loading {endpoint}: {e}")
            break
    
    return {item['id']: item['name'] for item in all_data}

def load_all_reference_data():
    """Load all reference data at startup"""
    global genres_map, platforms_map, game_modes_map, themes_map, player_perspectives_map
    
    print("\n Loading reference data from IGDB...")
    
    genres_map = load_reference_data('genres')
    print(f"Genres: {len(genres_map)} loaded")
    
    platforms_map = load_reference_data('platforms')
    print(f"Platforms: {len(platforms_map)} loaded")
    
    game_modes_map = load_reference_data('game_modes')
    print(f"Game Modes: {len(game_modes_map)} loaded")
    
    themes_map = load_reference_data('themes')
    print(f"Themes: {len(themes_map)} loaded")
    
    player_perspectives_map = load_reference_data('player_perspectives')
    print(f"Player Perspectives: {len(player_perspectives_map)} loaded")
    
    print("Reference data loaded successfully\n")

def translate_ids_to_names(ids, reference_map):
    """Translate list of IDs to names using reference map"""
    if not ids:
        return []
    return [reference_map.get(id, f"Unknown_{id}") for id in ids]

def fetch_games_batch(offset=0, limit=500):
    """Get batch of games from IGDB released from 2020 onwards."""
    token = get_access_token()
    timestamp_2020 = int(datetime.datetime(2005, 1, 1).timestamp())
    
    query = f"""fields id, name, category, status, rating, rating_count, aggregated_rating, aggregated_rating_count, total_rating, total_rating_count, hypes, follows, first_release_date, franchises, genres, platforms, themes, game_modes, player_perspectives, involved_companies, screenshots, videos, artworks, websites, similar_games, expansions, bundles;
            where first_release_date >= {timestamp_2020};
            limit {limit};
            offset {offset};
            sort first_release_date asc;"""
    
    try:
        response = requests.post(
            "https://api.igdb.com/v4/games",
            headers={
                "Client-ID": config["IGDBClientId"],
                "Authorization": f"Bearer {token}"
            },
            data=query,
            timeout=30
        )
        
        if response.status_code != 200:
            print(f"Error: HTTP {response.status_code}")
            print(f"Response: {response.text[:200]}")
            return []
        
        return response.json()
            
    except Exception as e:
        print(f"Error fetching games: {e}")
        return []

def calculate_metrics(game):
    """Calculate derived metrics for a game."""
    
    # Date calculations
    release_date = game.get('first_release_date')
    if release_date:
        release_dt = datetime.datetime.fromtimestamp(release_date)
        current_dt = datetime.datetime.now()
        days_since_release = (current_dt - release_dt).days
        days_to_release = (release_dt - current_dt).days
        is_released = days_since_release >= 0
        is_upcoming = days_to_release >= 0
    else:
        release_dt = None
        days_since_release = None
        days_to_release = None
        is_released = False
        is_upcoming = False
    
    # Count arrays
    franchise_count = len(game.get('franchises', []))
    platform_count = len(game.get('platforms', []))
    genre_count = len(game.get('genres', []))
    company_count = len(game.get('involved_companies', []))
    video_count = len(game.get('videos', []))
    screenshot_count = len(game.get('screenshots', []))
    website_count = len(game.get('websites', []))
    
    # Hype score (for upcoming games)
    hype_score = None
    if is_upcoming and days_to_release is not None and days_to_release >= 0:
        hypes = game.get('hypes', 0) or 0
        follows = game.get('follows', 0) or 0
        rating_count = game.get('rating_count', 0) or 0
        
        hype_component = hypes * 2.0
        follow_component = follows * 1.5
        anticipation_component = rating_count * 0.5
        
        time_factor = max(0, 1 - (days_to_release / 365.0)) if days_to_release <= 365 else 0.3
        raw_score = (hype_component + follow_component + anticipation_component) * time_factor
        hype_score = min(100.0, raw_score / 10.0)
    
    # Longevity score (for released games)
    longevity_score = None
    if is_released and days_since_release is not None and days_since_release > 0:
        rating = game.get('rating', 0) or 0
        rating_count = game.get('rating_count', 0) or 0
        
        quality_factor = rating / 100.0
        popularity_factor = min(1.0, rating_count / 1000.0)
        franchise_factor = min(1.0, franchise_count / 5.0)
        platform_factor = min(1.0, platform_count / 10.0)
        
        age_years = days_since_release / 365.0
        age_factor = min(1.0, age_years / 10.0) if age_years > 1 else 0.5
        
        longevity_score = (quality_factor * 40 + popularity_factor * 30 + 
                          franchise_factor * 15 + platform_factor * 10 + age_factor * 5)
    
    # Success level classification
    rating = game.get('rating')
    rating_count = game.get('rating_count')
    success_level = None
    if rating and rating_count:
        if rating >= 80 and rating_count >= 500:
            success_level = 2  # Hit
        elif rating >= 60 and rating_count >= 100:
            success_level = 1  # Medium
        else:
            success_level = 0  # Flop
    
    return {
        'release_date_formatted': release_dt.isoformat() if release_dt else None,
        'days_since_release': days_since_release,
        'days_to_release': days_to_release,
        'is_released': is_released,
        'is_upcoming': is_upcoming,
        'franchise_count': franchise_count,
        'platform_count': platform_count,
        'genre_count': genre_count,
        'company_count': company_count,
        'video_count': video_count,
        'screenshot_count': screenshot_count,
        'website_count': website_count,
        'marketing_intensity': video_count + screenshot_count + len(game.get('artworks', [])),
        'hype_score': hype_score,
        'longevity_score': longevity_score,
        'success_level': success_level,
        'has_multiplayer': len(game.get('game_modes', [])) > 1,
        'has_expansions': len(game.get('expansions', [])) > 0,
    }

def transform_game(game):
    """Transform game data for Elasticsearch."""
    
    metrics = calculate_metrics(game)
    genres_raw = game.get('genres', [])
    platforms_raw = game.get('platforms', [])
    game_modes_raw = game.get('game_modes', [])
    themes_raw = game.get('themes', [])
    perspectives_raw = game.get('player_perspectives', [])
    
    def process_field(raw_data, reference_map):
        if not raw_data:
            return [], []
        
        ids = []
        names = []
        
        for item in raw_data:
            if isinstance(item, int):
                ids.append(item)
                names.append(reference_map.get(item, f"Unknown_{item}"))
            elif isinstance(item, str):
                names.append(item)
                id_found = None
                for id_key, name_val in reference_map.items():
                    if name_val == item:
                        id_found = id_key
                        break
                if id_found:
                    ids.append(id_found)
        
        return ids, names
    
    genre_ids, genre_names = process_field(genres_raw, genres_map)
    platform_ids, platform_names = process_field(platforms_raw, platforms_map)
    game_mode_ids, game_mode_names = process_field(game_modes_raw, game_modes_map)
    theme_ids, theme_names = process_field(themes_raw, themes_map)
    perspective_ids, perspective_names = process_field(perspectives_raw, player_perspectives_map)
    
    doc = {
        # Basic info
        'game_id': game.get('id'),
        'name': game.get('name'),
        
        # Ratings
        'rating': game.get('rating'),
        'rating_count': game.get('rating_count'),
        'total_rating': game.get('total_rating'),
        'total_rating_count': game.get('total_rating_count'),
        'aggregated_rating': game.get('aggregated_rating'),
        'aggregated_rating_count': game.get('aggregated_rating_count'),
        
        # Engagement
        'hypes': game.get('hypes', 0),
        'follows': game.get('follows', 0),
        
        # Metadata - IDs
        'category': game.get('category'),
        'status': game.get('status'),
        'franchises': game.get('franchises', []),
        'genre_ids': genre_ids,
        'platform_ids': platform_ids,
        'theme_ids': theme_ids,
        'game_mode_ids': game_mode_ids,
        'player_perspective_ids': perspective_ids,
        'involved_companies': game.get('involved_companies', []),
        
        # Metadata - Names
        'genres': genre_names,
        'platforms': platform_names,
        'game_modes': game_mode_names,
        'themes': theme_names,
        'player_perspectives': perspective_names,
        
        # Assets
        'screenshots': game.get('screenshots', []),
        'videos': game.get('videos', []),
        'artworks': game.get('artworks', []),
        'websites': game.get('websites', []),
        
        # Related content
        'similar_games': game.get('similar_games', []),
        'expansions': game.get('expansions', []),
        'bundles': game.get('bundles', []),
        
        # Calculated metrics
        **metrics,
        
        # Timestamps
        'release_date': game.get('first_release_date'),
        'ingestion_time': datetime.datetime.utcnow().isoformat(),
        'processed_timestamp': int(time.time())
    }
    
    return doc

def create_elasticsearch_action(game, index_name='games'):
    """Create bulk action for Elasticsearch."""
    doc = transform_game(game)
    
    return {
        '_op_type': 'index',
        '_index': index_name,
        '_id': f"{doc['game_id']}_{int(time.time() * 1000)}",
        '_source': doc
    }

def fetch_all_games_from_2020():
    """Fetch all games from 2020 to now and send to Elasticsearch."""
    print("IGDB GAMES BULK IMPORT (2020 - Present)")
    
    # Load reference data first
    load_all_reference_data()
    
    # Check Elasticsearch connection
    try:
        if not es.ping():
            print("Cannot connect to Elasticsearch")
            return
    except Exception as e:
        print(f"Cannot connect to Elasticsearch: {e}")
        return
    
    print("Connected to Elasticsearch\n")
    
    offset = 0
    limit = 500
    total_games = 0
    total_sent = 0
    batch_number = 1
    
    while True:
        print(f"\n Batch #{batch_number} (offset: {offset})")
        
        # Fetch batch
        games = fetch_games_batch(offset=offset, limit=limit)
        
        if not games:
            print("No more games to fetch")
            break
        
        print(f"Retrieved: {len(games)} games")
        total_games += len(games)
        
        # Prepare bulk actions
        actions = [create_elasticsearch_action(game) for game in games]
        
        # Send to Elasticsearch
        try:
            success, failed = bulk(es, actions, raise_on_error=False, stats_only=False)
            total_sent += success
            
            if failed:
                print(f"Failed: {len(failed)} documents")
            
            print(f"Sent to ES: {success}/{len(games)} games")
            
            # Print examples with translated data
            for i, game in enumerate(games[:3]):
                name = game.get('name', 'Unknown')[:40]
                release = game.get('first_release_date')
                if release:
                    release_str = datetime.datetime.fromtimestamp(release).strftime('%Y-%m-%d')
                else:
                    release_str = 'N/A'
                
                # Show translated genres
                genres_raw = game.get('genres', [])[:2]
                genres_display = []
                for item in genres_raw:
                    if isinstance(item, int):
                        genres_display.append(genres_map.get(item, f"Unknown_{item}"))
                    elif isinstance(item, str):
                        genres_display.append(item)
                genres_str = ', '.join(genres_display) if genres_display else 'N/A'
                
                print(f"      [{i+1}] {name:<40} | {release_str} | {genres_str}")
            
        except Exception as e:
            print(f"Error sending to Elasticsearch: {e}")
        
        # Check if last page
        if len(games) < limit:
            print("\n Reached end of results")
            break
        
        offset += limit
        batch_number += 1
        
        # Rate limiting
        print("Waiting 1 second...")
        time.sleep(1)
    
    print(f"IMPORT COMPLETED")
    print(f"Total games fetched: {total_games}")
    print(f"Total sent to ES: {total_sent}")

if __name__ == "__main__":
    fetch_all_games_from_2020()