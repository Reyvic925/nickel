import logging, os, datetime, time, json, threading, requests, httpx, tls_client, pickle
from concurrent.futures import ThreadPoolExecutor, as_completed

# ---------- Read configuration ----------
if 'DISCORD_TOKEN' in os.environ:
    token = os.environ.get('DISCORD_TOKEN')
    # Support both single and multiple configurations
    guild_ids_str = os.environ.get('DISCORD_GUILD_ID', '')
    channel_ids_str = os.environ.get('DISCORD_CHANNEL_ID', '')
    webhook = os.environ.get('DISCORD_WEBHOOK')
    proxy = os.environ.get('DISCORD_PROXY', '')
    blacklistedRoles = json.loads(os.environ.get('DISCORD_BLACKLISTED_ROLES', '[]'))
    blacklistedUsers = json.loads(os.environ.get('DISCORD_BLACKLISTED_USERS', '[]'))
    scan_interval = int(os.environ.get('SCAN_INTERVAL', '60'))
    BATCH_SIZE = int(os.environ.get('BATCH_SIZE', '20'))
    INDIVIDUAL_THRESHOLD = int(os.environ.get('INDIVIDUAL_THRESHOLD', '5'))
    
    # Parse multiple guild/channel pairs
    guild_ids = [g.strip() for g in guild_ids_str.split(',') if g.strip()]
    channel_ids = [c.strip() for c in channel_ids_str.split(',') if c.strip()]
    
    if len(guild_ids) != len(channel_ids):
        raise ValueError("Number of GUILD_IDs must match number of CHANNEL_IDs")
    
    # Create list of (guild_id, channel_id) tuples
    guild_channel_pairs = list(zip(guild_ids, channel_ids))
else:
    from json import load
    config = load(open('config.json'))
    webhook = config.get('webhook')
    proxy = config.get('proxy', '')
    blacklistedRoles = config.get('blacklistedRoles', [])
    blacklistedUsers = config.get('blacklistedUsers', [])
    scan_interval = 60
    BATCH_SIZE = 20
    INDIVIDUAL_THRESHOLD = 5
    
    # Support both old single format and new multi format
    if 'guildChannelPairs' in config:
        guild_channel_pairs = [(pair['guildId'], pair['channelId']) for pair in config['guildChannelPairs']]
    elif 'guildID' in config and 'channelId' in config:
        guild_channel_pairs = [(config['guildID'], config['channelId'])]
    else:
        raise ValueError("Configuration must include either guildChannelPairs or guildID/channelId")
    
    token = config.get('token')

if not token:
    raise ValueError("DISCORD_TOKEN is not set.")
if not guild_channel_pairs:
    raise ValueError("No guild/channel pairs configured.")
if not webhook:
    raise ValueError("DISCORD_WEBHOOK is not set.")

try:
    import websocket
except:
    os.system("pip install websocket-client")
    import websocket

logging.basicConfig(
    level=logging.INFO,
    format="\x1b[38;5;9m[\x1b[0m%(asctime)s\x1b[38;5;9m]\x1b[0m %(message)s\x1b[0m",
    datefmt="%H:%M:%S"
)

JOIN_WINDOW_SECONDS = 2 * 24 * 60 * 60
NOTIFIED_CACHE_FILE = "notified_members.pkl"

if os.path.exists(NOTIFIED_CACHE_FILE):
    with open(NOTIFIED_CACHE_FILE, 'rb') as f:
        notified_members = pickle.load(f)
else:
    notified_members = set()

def save_notified_cache():
    with open(NOTIFIED_CACHE_FILE, 'wb') as f:
        pickle.dump(notified_members, f)

# ---------- Utils ----------
class Utils:
    def rangeCorrector(ranges):
        return ranges

    def getRanges(index, multiplier, memberCount):
        start = index * multiplier
        end = start + 99
        return [[start, end]]

    def parseGuildMemberListUpdate(response):
        memberdata = {
            "online_count": response["d"].get("online_count", 0),
            "member_count": response["d"].get("member_count", 0),
            "id": response["d"].get("id"),
            "guild_id": response["d"].get("guild_id"),
            "hoisted_roles": response["d"].get("groups", []),
            "types": [],
            "locations": [],
            "updates": []
        }
        for chunk in response['d'].get('ops', []):
            memberdata['types'].append(chunk['op'])
            if chunk['op'] in ('SYNC', 'INVALIDATE'):
                memberdata['locations'].append(chunk.get('range'))
                if chunk['op'] == 'SYNC':
                    memberdata['updates'].append(chunk.get('items', []))
                else:
                    memberdata['updates'].append([])
            elif chunk['op'] in ('INSERT', 'UPDATE', 'DELETE'):
                memberdata['locations'].append(chunk.get('index'))
                if chunk['op'] == 'DELETE':
                    memberdata['updates'].append([])
                else:
                    memberdata['updates'].append(chunk.get('item'))
        return memberdata

# ---------- WebSocket ----------
class DiscordSocket(websocket.WebSocketApp):
    def __init__(self, token, guild_id, channel_id):
        self.token = token
        self.guild_id = guild_id
        self.channel_id = channel_id
        self.blacklisted_roles = [str(r) for r in blacklistedRoles]
        self.blacklisted_users = [str(u) for u in blacklistedUsers]

        self.socket_headers = {
            "Accept-Encoding": "gzip, deflate, br",
            "Accept-Language": "en-US,en;q=0.9",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "Sec-WebSocket-Extensions": "permessage-deflate; client_max_window_bits",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:94.0) Gecko/20100101 Firefox/94.0",
        }
        super().__init__(
            "wss://gateway.discord.gg/?encoding=json&v=9",
            header=self.socket_headers,
            on_open=lambda ws: self.sock_open(ws),
            on_message=lambda ws, msg: self.sock_message(ws, msg),
            on_close=lambda ws, close_code, close_msg: self.sock_close(ws, close_code, close_msg)
        )
        self.endScraping = False
        self.guilds = {}
        self.members = {}
        self.current_range_start = 0
        self.packets_recv = 0

    def run(self):
        # Use default ping settings to avoid library errors
        self.run_forever()
        return self.members

    def scrapeUsers(self):
        if self.endScraping:
            return
        payload = {
            "op": 14,
            "d": {
                "guild_id": self.guild_id,
                "typing": True,
                "activities": True,
                "threads": True,
                "channels": {self.channel_id: [[self.current_range_start, self.current_range_start + 99]]}
            }
        }
        self.send(json.dumps(payload))

    def sock_open(self, ws):
        identify = {
            "op": 2,
            "d": {
                "token": self.token,
                "capabilities": 125,
                "properties": {
                    "os": "Windows",
                    "browser": "Firefox",
                    "device": "",
                    "system_locale": "it-IT",
                    "browser_user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:94.0) Gecko/20100101 Firefox/94.0",
                    "browser_version": "94.0",
                    "os_version": "10",
                    "referrer": "",
                    "referring_domain": "",
                    "referrer_current": "",
                    "referring_domain_current": "",
                    "release_channel": "stable",
                    "client_build_number": 103981,
                    "client_event_source": None
                },
                "presence": {"status": "online", "since": 0, "activities": [], "afk": False},
                "compress": False,
                "client_state": {
                    "guild_hashes": {},
                    "highest_last_message_id": "0",
                    "read_state_version": 0,
                    "user_guild_settings_version": -1,
                    "user_settings_version": -1
                }
            }
        }
        self.send(json.dumps(identify))

    def heartbeatThread(self, interval):
        try:
            while True:
                self.send('{"op":1,"d":' + str(self.packets_recv) + '}')
                time.sleep(interval)
        except Exception:
            return

    def _process_member_item(self, item):
        """Extract and store a member if valid."""
        if not isinstance(item, dict) or "member" not in item:
            return
        mem = item["member"]
        user = mem.get("user", {})
        if not user:
            return
        user_id = user.get("id")
        if not user_id:
            return
        if user.get("bot"):
            return
        if user_id in self.blacklisted_users:
            return
        if set(self.blacklisted_roles).intersection(mem.get("roles", [])):
            return
        username = user.get('username', 'Unknown')
        discrim = user.get('discriminator', '0')
        tag = f"{username}#{discrim}" if discrim != "0" else f"@{username}"
        joined_at = mem.get('joined_at')
        self.members[user_id] = (tag, joined_at)

    def sock_message(self, ws, message):
        try:
            decoded = json.loads(message)
            if not isinstance(decoded, dict):
                return

            op = decoded.get("op")
            t = decoded.get("t")

            if op != 11:
                self.packets_recv += 1

            if op == 10:
                threading.Thread(
                    target=self.heartbeatThread,
                    args=(decoded["d"]["heartbeat_interval"] / 1000,),
                    daemon=True
                ).start()

            if t == "READY":
                for guild in decoded.get("d", {}).get("guilds", []):
                    self.guilds[guild["id"]] = {"member_count": guild.get("member_count", 0)}

            if t == "READY_SUPPLEMENTAL":
                # Start scraping from the beginning of the member list
                self.current_range_start = 0
                self.scrapeUsers()

            elif t == "GUILD_MEMBER_LIST_UPDATE":
                parsed = Utils.parseGuildMemberListUpdate(decoded)
                if parsed.get('guild_id') != self.guild_id:
                    return

                # Process all operations in this update
                for elem, index in enumerate(parsed["types"]):
                    updates = parsed["updates"][elem]
                    if isinstance(updates, dict):
                        updates = [updates]
                    elif not isinstance(updates, list):
                        updates = []

                    if index == "SYNC":
                        if len(updates) == 0:
                            # Empty SYNC means we reached the end of the visible members
                            self.endScraping = True
                            break
                        
                        for item in updates:
                            self._process_member_item(item)
                            
                    elif index in ("UPDATE", "INSERT"):
                        for item in updates:
                            self._process_member_item(item)

                if self.endScraping:
                    logging.info(f"✅ Finished scraping guild {self.guild_id}. Total members captured: {len(self.members)}")
                    self.close()
                else:
                    # Move to the next range of 100
                    self.current_range_start += 100
                    # Safety limit: don't scrape more than 5000 members to prevent abuse/timeouts
                    if self.current_range_start >= 5000:
                        logging.info(f"✅ Reached safety limit (5000 members) for guild {self.guild_id}. Stopping scrape.")
                        self.endScraping = True
                        self.close()
                    else:
                        self.scrapeUsers()

        except Exception as e:
            logging.error(f"Error in sock_message for guild {self.guild_id}: {e}")

    def sock_close(self, ws, close_code, close_msg):
        pass

# ---------- Helpers ----------
def autoSnitch(token, guild_id, channel_id):
    sb = DiscordSocket(token, guild_id, channel_id)
    return sb.run()

def rotateProxy():
    if proxy:
        return {'http': 'http://%s' % proxy, 'https': 'http://%s' % proxy}
    return None

def session(token):
    sess = tls_client.Session(client_identifier='chrome_105')
    sess.headers.update({
        'accept': '*/*',
        'accept-encoding': 'application/json',
        'accept-language': 'en-US,en;q=0.8',
        'Content-Type': 'application/json',
        'Authorization': token,
        'referer': 'https://discord.com/channels/@me',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'sec-gpc': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36',
        'x-context-properties': 'eyJsb2NhdGlvbiI6IlVzZXIgUHJvZmlsZSJ9',
        'x-debug-options': 'bugReporterEnabled',
        'x-discord-locale': 'en-US',
        'x-super-properties': 'eyJvcyI6IldpbmRvd3MiLCJicm93c2VyIjoiRGlzY29yZCBDbGllbnQiLCJyZWxlYXNlX2NoYW5uZWwiOiJjYW5hcnkiLCJjbGllbnRfdmVyc2lvbiI6IjEuMC41OSIsIm9zX3ZlcnNpb24iOiIxMC4wLjIyNjIxIiwib3NfYXJjaCI6Ing2NCIsInN5c3RlbV9sb2NhbGUiOiJlbi1VUyIsImNsaWVudF9idWlsZF9udW1iZXIiOjE4MTk2NywibmF0aXZlX2J1aWxkX251bWJlciI6MzA4NTIsImNsaWVudF9ldmVudF9zb3VyY2UiOm51bGwsImRlc2lnbl9pZCI6MH0='
    })
    return sess

# ---------- Webhook Sending ----------
def send_single_webhook(member_id, tag, join_time, guild_name, max_retries=3):
    attempt = 0
    wait_time = 2
    while attempt <= max_retries:
        try:
            sess = session(token)
            if tag.startswith('@'):
                clean_username = tag[1:]
            elif '#' in tag:
                clean_username = tag.split('#')[0]
            else:
                clean_username = tag
            join_str = join_time.strftime("%m-%d-%Y on %I:%M %p")
            payload = {
                "content": f"@here New User Joined",
                "embeds": [{
                    "color": 161791,
                    "author": {"name": "Snitched Successful"},
                    "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                    "fields": [
                        {"name": "Username", "value": clean_username, "inline": True},
                        {"name": "User ID", "value": member_id, "inline": True},
                        {"name": "Joined Server", "value": join_str, "inline": False},
                        {"name": "Mention", "value": f"<@{member_id}>", "inline": True},
                        {"name": "Guild", "value": guild_name, "inline": True}
                    ]
                }]
            }
            response = requests.post(webhook, json=payload)
            if response.status_code == 204:
                logging.info(f"✅ Webhook sent for {member_id} in {guild_name}")
                return
            elif response.status_code == 429:
                try:
                    data = response.json()
                    retry_after = data.get('retry_after', wait_time)
                except:
                    retry_after = wait_time
                wait_time = max(wait_time, retry_after)
                logging.warning(f"Rate limited for {member_id}, waiting {wait_time}s...")
                time.sleep(wait_time)
                attempt += 1
                wait_time = wait_time * 2
                continue
            else:
                logging.error(f"Webhook failed with status {response.status_code}: {response.text[:200]}")
                return
        except Exception as e:
            logging.error(f"Webhook exception: {e}")
            attempt += 1
            time.sleep(2)

def send_batch_webhook(batch, guild_name, max_retries=3):
    if not batch:
        return
    attempt = 0
    wait_time = 2
    while attempt <= max_retries:
        try:
            fields = []
            for item in batch:
                member_id = item['member_id']
                tag = item['tag']
                join_time = item['join_time']
                clean_username = tag[1:] if tag.startswith('@') else tag.split('#')[0] if '#' in tag else tag
                join_str = join_time.strftime("%m-%d-%Y %I:%M %p")
                fields.append({
                    "name": "New Member",
                    "value": f"**{clean_username}**\nID: `{member_id}`\nJoined: {join_str}\nGuild: {guild_name}",
                    "inline": False
                })
            embed = {
                "color": 161791,
                "author": {"name": f"Snitched Successful ({len(batch)} new members)"},
                "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                "fields": fields,
                "footer": {"text": f"Guild: {guild_name}"}
            }
            payload = {"embeds": [embed]}

            response = requests.post(webhook, json=payload)
            if response.status_code == 204:
                logging.info(f"✅ Batch webhook sent for {len(batch)} members in {guild_name}.")
                return
            elif response.status_code == 429:
                try:
                    data = response.json()
                    retry_after = data.get('retry_after', wait_time)
                except:
                    retry_after = wait_time
                wait_time = max(wait_time, retry_after)
                logging.warning(f"Batch rate limited, waiting {wait_time}s...")
                time.sleep(wait_time)
                attempt += 1
                wait_time = wait_time * 2
                continue
            else:
                logging.error(f"Batch webhook failed with status {response.status_code}: {response.text[:200]}")
                return
        except Exception as e:
            logging.error(f"Batch webhook exception: {e}")
            attempt += 1
            time.sleep(2)

# ---------- Processing with smart individual/batch ----------
def process_new_members(new_members_dict, guild_names_map):
    if not new_members_dict:
        return

    now = datetime.datetime.now(datetime.timezone.utc)
    pending_by_guild = {}

    for member_id, (tag, joined_at, guild_id) in new_members_dict.items():
        if not joined_at:
            continue
        if not isinstance(joined_at, str):
            continue
        try:
            join_time = datetime.datetime.fromisoformat(joined_at.replace('Z', '+00:00'))
            age = (now - join_time).total_seconds()
            if age <= JOIN_WINDOW_SECONDS:
                if member_id in notified_members:
                    continue
                guild_name = guild_names_map.get(guild_id, f"Unknown Guild ({guild_id})")
                if guild_id not in pending_by_guild:
                    pending_by_guild[guild_id] = []
                pending_by_guild[guild_id].append({
                    'member_id': member_id,
                    'tag': tag,
                    'join_time': join_time,
                    'guild_name': guild_name
                })
                notified_members.add(member_id)
        except Exception as e:
            logging.warning(f"Error processing {member_id}: {e}")

    if not pending_by_guild:
        return

    # Process each guild's new members
    for guild_id, pending in pending_by_guild.items():
        guild_name = pending[0]['guild_name'] if pending else "Unknown"
        
        # Decide: individual or batch?
        if len(pending) <= INDIVIDUAL_THRESHOLD:
            logging.info(f"📨 Sending {len(pending)} members individually from {guild_name}.")
            for item in pending:
                send_single_webhook(item['member_id'], item['tag'], item['join_time'], item['guild_name'])
                time.sleep(2)
        else:
            logging.info(f"📦 Sending {len(pending)} members in batches of {BATCH_SIZE} from {guild_name}.")
            for i in range(0, len(pending), BATCH_SIZE):
                batch = pending[i:i+BATCH_SIZE]
                send_batch_webhook(batch, guild_name)
                if i + BATCH_SIZE < len(pending):
                    time.sleep(2)

    save_notified_cache()
    logging.info("✅ Finished processing new members.")

# ---------- Scrape multiple guilds ----------
def scrape_all_guilds(guild_channel_pairs):
    """Scrape all configured guilds and return combined member data."""
    all_members = {}
    
    def scrape_single_guild(pair):
        guild_id, channel_id = pair
        try:
            logging.info(f"Scraping guild {guild_id} via channel {channel_id}...")
            members = autoSnitch(token, guild_id, channel_id)
            logging.info(f"✅ Scraped {len(members)} members from guild {guild_id}")
            return guild_id, members
        except Exception as e:
            logging.error(f"❌ Failed to scrape guild {guild_id}: {e}")
            return guild_id, {}
    
    # Use ThreadPoolExecutor to scrape multiple guilds concurrently
    with ThreadPoolExecutor(max_workers=min(len(guild_channel_pairs), 5)) as executor:
        futures = {executor.submit(scrape_single_guild, pair): pair for pair in guild_channel_pairs}
        
        for future in as_completed(futures):
            pair = futures[future]
            guild_id, channel_id = pair
            try:
                result_guild_id, members = future.result()
                # Add guild_id to each member's data
                for user_id, (tag, joined_at) in members.items():
                    all_members[user_id] = (tag, joined_at, result_guild_id)
            except Exception as e:
                logging.error(f"Exception scraping guild {guild_id}: {e}")
    
    return all_members

# ---------- Get guild names ----------
def get_guild_names(guild_ids):
    """Get names for all guilds."""
    guild_names = {}
    sess = session(token)
    
    for guild_id in guild_ids:
        try:
            resp = sess.get(f'https://discord.com/api/v9/guilds/{guild_id}')
            if resp.status_code == 200:
                data = resp.json()
                guild_names[guild_id] = data.get('name', f'Unknown ({guild_id})')
            else:
                guild_names[guild_id] = f'Unknown ({guild_id})'
        except Exception as e:
            logging.warning(f"Could not fetch name for guild {guild_id}: {e}")
            guild_names[guild_id] = f'Unknown ({guild_id})'
    
    return guild_names

# ---------- Startup webhook check ----------
def wait_for_webhook_ready():
    logging.info("Checking webhook availability...")
    attempt = 0
    wait_time = 2
    while True:
        try:
            payload = {"content": "Startup check"}
            response = requests.post(webhook, json=payload, timeout=10)
            if response.status_code == 204:
                logging.info("✅ Webhook is ready.")
                return True
            elif response.status_code == 429:
                try:
                    data = response.json()
                    retry_after = data.get('retry_after', wait_time)
                except:
                    retry_after = wait_time
                wait_time = max(wait_time, retry_after)
                logging.warning(f"Webhook rate-limited on startup, waiting {wait_time}s...")
                time.sleep(wait_time)
                attempt += 1
                wait_time = wait_time * 2
                continue
            else:
                logging.warning(f"Webhook check returned {response.status_code}. Proceeding anyway.")
                return True
        except Exception as e:
            logging.warning(f"Webhook check exception: {e}. Proceeding anyway.")
            return True

# ---------- Main ----------
if __name__ == '__main__':
    logging.info("Starting multi-guild snitch (%ds interval, 2-day join window)...", scan_interval)
    logging.info("Monitoring %d guild(s):", len(guild_channel_pairs))
    for i, (guild_id, channel_id) in enumerate(guild_channel_pairs, 1):
        logging.info("  %d. Guild: %s, Channel: %s", i, guild_id, channel_id)

    try:
        from http.server import HTTPServer, BaseHTTPRequestHandler
        class HealthCheckHandler(BaseHTTPRequestHandler):
            def do_GET(self):
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b"OK")
            def do_HEAD(self):
                self.send_response(200)
                self.end_headers()
            def log_message(self, format, *args):
                return  # Suppress HTTP server logs
        def run_http_server():
            server = HTTPServer(('0.0.0.0', int(os.environ.get('PORT', 10000))), HealthCheckHandler)
            server.serve_forever()
        threading.Thread(target=run_http_server, daemon=True).start()
        logging.info("HTTP health check server started on port %s", os.environ.get('PORT', 10000))
    except Exception as e:
        logging.warning("Could not start HTTP server: %s", e)

    webhook_mask = webhook[:40] + "..." if len(webhook) > 40 else webhook
    logging.info("Configuration: token starts with %s..., webhook: %s", token[:8], webhook_mask)

    # Wait for webhook to be ready (not rate-limited)
    wait_for_webhook_ready()

    # Get all unique guild IDs
    all_guild_ids = list(set([gid for gid, _ in guild_channel_pairs]))
    
    # Get guild names
    logging.info("Fetching guild names...")
    guild_names_map = get_guild_names(all_guild_ids)
    for guild_id, name in guild_names_map.items():
        logging.info("  Guild %s: %s", guild_id, name)

    logging.info("Building initial baseline...")
    current_members_raw = scrape_all_guilds(guild_channel_pairs)
    current_ids = set(current_members_raw.keys())
    logging.info("Baseline built: %s members visible across all guilds.", len(current_ids))

    logging.info("Checking baseline members for recent joins...")
    process_new_members(current_members_raw, guild_names_map)

    while True:
        logging.info("🔄 Starting new scan cycle...")
        new_members_raw = scrape_all_guilds(guild_channel_pairs)
        new_ids = set(new_members_raw.keys())
        logging.info("Scanned: %s members visible across all guilds.", len(new_ids))

        diff_ids = new_ids - current_ids
        if diff_ids:
            diff_dict = {uid: new_members_raw[uid] for uid in diff_ids}
            logging.info("Found %s new IDs not in previous scan.", len(diff_dict))
            process_new_members(diff_dict, guild_names_map)
        else:
            logging.info("No new members detected in this scan.")

        current_ids = new_ids
        logging.info("Sleeping %s seconds...", scan_interval)
        time.sleep(scan_interval)
