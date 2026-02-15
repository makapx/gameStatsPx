import schedule
import time
import requests
import datetime
import json

# Load config 
with open('./config/config.json') as f:
    config = json.load(f)

# Globals
access_token = None
token_expiry = None
last_processed_offset = 0

# Reference data caches
genres_map = {}
platforms_map = {}
game_modes_map = {}
themes_map = {}
player_perspectives_map = {}

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
    print("-" * 60)
    
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
    
    print("-" * 60)
    print("Reference data loaded successfully\n")

def translate_ids_to_names(ids, reference_map):
    """Translate list of IDs to names using reference map"""
    if not ids:
        return []
    return [reference_map.get(id, f"Unknown_{id}") for id in ids]

def fetch_recent_and_upcoming_games(limit=50):
    """Get videogames from IGDB released in last 2 months or upcoming."""
    global last_processed_offset
    
    token = get_access_token()
    
    two_months_ago = datetime.datetime.now() - datetime.timedelta(days=60)
    timestamp_two_months_ago = int(two_months_ago.timestamp())
    
    query = f"""
        fields id, name, rating, rating_count, hypes, follows, first_release_date, 
       category, status, total_rating, total_rating_count,
       aggregated_rating, aggregated_rating_count, franchises, genres, platforms,
       involved_companies, game_modes, player_perspectives, themes,
       screenshots, videos, artworks, websites, similar_games, expansions, bundles;
     where first_release_date >= {timestamp_two_months_ago} ;
     limit 100;
     sort hypes desc;
    """

    try:
        response = requests.post(
            "https://api.igdb.com/v4/games",
            headers={
                "Client-ID": config["IGDBClientId"],
                "Authorization": f"Bearer {token}"
            },
            data=query,
            timeout=15
        )
        
        if response.status_code == 200:
            games = response.json()
            if games and len(games) == limit:
                last_processed_offset += limit
            else:
                last_processed_offset = 0
            
            return games
        else:
            print(f"IGDB API Error: {response.status_code}")
            print(f"Response: {response.text[:200]}")
            return []
            
    except Exception as e:
        print(f"Error fetching games: {e}")
        return []

def transform_game(game):
    """Transform game data and add calculated fields."""

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
    
    game['days_since_release'] = days_since_release
    game['days_to_release'] = days_to_release
    game['is_released'] = is_released
    game['is_upcoming'] = is_upcoming
    game['release_date_formatted'] = release_dt.isoformat() if release_dt else None
    
    game['genre_ids'] = genre_ids
    game['platform_ids'] = platform_ids
    game['game_mode_ids'] = game_mode_ids
    game['theme_ids'] = theme_ids
    game['player_perspective_ids'] = perspective_ids
    
    game['genres'] = genre_names
    game['platforms'] = platform_names
    game['game_modes'] = game_mode_names
    game['themes'] = theme_names
    game['player_perspectives'] = perspective_names
    
    game['franchise_count'] = len(game.get('franchises', []))
    game['platform_count'] = len(platform_names)
    game['genre_count'] = len(genre_names)
    game['company_count'] = len(game.get('involved_companies', []))
    game['video_count'] = len(game.get('videos', []))
    game['screenshot_count'] = len(game.get('screenshots', []))
    game['website_count'] = len(game.get('websites', []))
    game['marketing_intensity'] = (game['video_count'] + 
                                    game['screenshot_count'] + 
                                    len(game.get('artworks', [])))
    # Hype score
    if is_upcoming and days_to_release is not None and days_to_release >= 0:
        hypes = game.get('hypes', 0) or 0
        follows = game.get('follows', 0) or 0
        rating_count = game.get('rating_count', 0) or 0
        
        hype_component = hypes * 2.0
        follow_component = follows * 1.5
        anticipation_component = rating_count * 0.5
        
        time_factor = max(0, 1 - (days_to_release / 365.0)) if days_to_release <= 365 else 0.3
        raw_score = (hype_component + follow_component + anticipation_component) * time_factor
        game['hype_score'] = min(100.0, raw_score / 10.0)
    else:
        game['hype_score'] = None
    
    # Longevity score for upcoming games
    if is_released and days_since_release is not None and days_since_release > 0:
        rating = game.get('rating', 0) or 0
        rating_count = game.get('rating_count', 0) or 0
        franchise_count = game['franchise_count']
        platform_count = game['platform_count']
        
        quality_factor = rating / 100.0
        popularity_factor = min(1.0, rating_count / 1000.0)
        franchise_factor = min(1.0, franchise_count / 5.0)
        platform_factor = min(1.0, platform_count / 10.0)
        
        age_years = days_since_release / 365.0
        age_factor = min(1.0, age_years / 10.0) if age_years > 1 else 0.5
        
        game['longevity_score'] = (quality_factor * 40 + popularity_factor * 30 + 
                                   franchise_factor * 15 + platform_factor * 10 + age_factor * 5)
    else:
        game['longevity_score'] = None
    
    # Success level
    rating = game.get('rating')
    rating_count = game.get('rating_count')
    if rating and rating_count:
        if rating >= 80 and rating_count >= 500:
            game['success_level'] = 2  # Hit
        elif rating >= 60 and rating_count >= 100:
            game['success_level'] = 1  # Medium
        else:
            game['success_level'] = 0  # Flop
    else:
        game['success_level'] = None
    
    game['processed_timestamp'] = int(time.time())
    game['ingestion_time'] = datetime.datetime.utcnow().isoformat()
    
    return game

def send_to_fluentbit(data):
    """Send data to FluentBit"""
    try:
        requests.post(
            config.get("FluentBitURL", "http://fluentbit:9090"),
            json=data,
            headers={"Content-Type": "application/json"},
            timeout=10
        )
        return True
    except Exception as e:
        print(f"Error in sending to Fluentbit: {e}")
        return False

def job():
    """Main job activated"""
    print(f"\n{'='*60}")
    print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 🎮 Starting crawling...")
    print(f"{'='*60}")
    
    try:
        games = fetch_recent_and_upcoming_games(limit=config.get("BatchSize", 50))
        print(f"Retrieved {len(games)} games")
        
        if not games:
            print("No games found")
            return
        
        released_games = []
        upcoming_games = []
        
        for game in games:
            transformed = transform_game(game)
            if transformed.get('is_upcoming'):
                upcoming_games.append(transformed)
            else:
                released_games.append(transformed)
        
        print(f"Released (last 2 months): {len(released_games)}")
        print(f"Upcoming: {len(upcoming_games)}")
        
        # Send to FluentBit
        success = sum(1 for g in games if send_to_fluentbit(transform_game(g)))
        print(f"{success}/{len(games)} sent successfully")
        
        # Show examples
        if released_games:
            print(f"\n Recently Released:")
            for game in released_games[:3]:
                name = game.get('name', 'Unknown')[:40]
                days = game.get('days_since_release', 0)
                rating = game.get('rating', 0)
                genres = ', '.join(game.get('genres', [])[:2]) if game.get('genres') else 'N/A'
                platforms = ', '.join(game.get('platforms', [])[:2]) if game.get('platforms') else 'N/A'
                print(f"   • {name:<40}")
                print(f"     ├─ Released: {days} days ago | Rating: {rating:.1f}")
                print(f"     └─ {genres} | {platforms}")
        
        if upcoming_games:
            print(f"\n Upcoming Games:")
            for game in upcoming_games[:3]:
                name = game.get('name', 'Unknown')[:40]
                days = game.get('days_to_release', 0)
                hype = game.get('hype_score', 0)
                genres = ', '.join(game.get('genres', [])[:2]) if game.get('genres') else 'N/A'
                platforms = ', '.join(game.get('platforms', [])[:2]) if game.get('platforms') else 'N/A'
                print(f"   • {name:<40}")
                print(f"     ├─ In {days} days | Hype: {hype:.1f}")
                print(f"     └─ {genres} | {platforms}")
        
        print(f"\n{'='*60}\n")
        
    except Exception as e:
        print(f"Main job error: {e}")

print("IGDB Recent & Upcoming Games Crawler")

load_all_reference_data()

schedule.every(10).minutes.do(job)

print(f"Scheduled to run every 10 minutes")
print(f"Fetching games from last 2 months + upcoming releases\n")

job()

while True:
    schedule.run_pending()
    time.sleep(1)