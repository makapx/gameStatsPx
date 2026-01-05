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
last_processed_id = 0

def get_access_token():
    """Get OAuth token from Twitch."""
    global access_token, token_expiry
    
    if access_token and token_expiry and datetime.datetime.now() < token_expiry:
        return access_token
    
    print("Get access token...")
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

def fetch_games(limit=50):
    """Get videogames from IGDB."""
    global last_processed_id
    
    token = get_access_token()
    query = f"""
    fields id, name, rating, rating_count, first_release_date, 
           genres.name, platforms.name, summary, cover.url,
           involved_companies.company.name, involved_companies.developer,
           involved_companies.publisher;
    where rating_count > 5 & id > {last_processed_id};
    limit {limit};
    sort id asc;
    """
    
    response = requests.post(
        "https://api.igdb.com/v4/games",
        headers={
            "Client-ID": config["IGDBClientId"],
            "Authorization": f"Bearer {token}"
        },
        data=query,
        timeout=15
    )
    
    games = response.json()
    if games:
        last_processed_id = max(g['id'] for g in games)
    return games

def transform_game(game):
    """Transform game data."""
    devs = []
    pubs = []
    for comp in game.get('involved_companies', []):
        name = comp.get('company', {}).get('name', '')
        if comp.get('developer'):
            devs.append(name)
        if comp.get('publisher'):
            pubs.append(name)
    
    return {
        "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
        "source": "igdb",
        "game": {
            "id": game.get("id"),
            "name": game.get("name", ""),
            "rating": round(game["rating"], 2) if game.get("rating") else None,
            "rating_count": game.get("rating_count", 0),
            "release_date": datetime.datetime.fromtimestamp(
                game["first_release_date"]
            ).strftime("%Y-%m-%d") if game.get("first_release_date") else None,
            "genres": [g["name"] for g in game.get("genres", [])],
            "platforms": [p["name"] for p in game.get("platforms", [])],
            "developers": devs,
            "publishers": pubs,
            "summary": game.get("summary", ""),
            "cover_url": game.get("cover", {}).get("url", "")
        }
    }

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
    print(f"[{datetime.datetime.now()}] Starting crawling...")
    
    try:
        games = fetch_games(limit=config.get("BatchSize", 50))
        print(f"{len(games)} games retrived")
        
        success = sum(1 for g in games if send_to_fluentbit(transform_game(g)))
        print(f"{success}/{len(games)} sent successfully (last ID: {last_processed_id})")
        
    except Exception as e:
        print(f"Main job error {e}")

# Cron
schedule.every(10).minutes.do(job)
job()

while True:
    schedule.run_pending()
    time.sleep(1)