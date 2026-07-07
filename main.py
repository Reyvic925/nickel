import logging, os, datetime, time, json, threading, requests, httpx, tls_client, pickle
from concurrent.futures import ThreadPoolExecutor, as_completed

# ---------- Read configuration ----------
if 'DISCORD_TOKEN' in os.environ:
    token = os.environ.get('DISCORD_TOKEN')
    guildId = os.environ.get('DISCORD_GUILD_ID')
    channelId = os.environ.get('DISCORD_CHANNEL_ID')
    webhook = os.environ.get('DISCORD_WEBHOOK')
    proxy = os.environ.get('DISCORD_PROXY', '')
    blacklistedRoles = json.loads(os.environ.get('DISCORD_BLACKLISTED_ROLES', '[]'))
    blacklistedUsers = json.loads(os.environ.get('DISCORD_BLACKLISTED_USERS', '[]'))
    scan_interval = int(os.environ.get('SCAN_INTERVAL', '60'))
    BATCH_SIZE = int(os.environ.get('BATCH_SIZE', '20'))
    INDIVIDUAL_THRESHOLD = int(os.environ.get('INDIVIDUAL_THRESHOLD', '5'))
else:
    from json import load
    config = load(open('config.json'))
    guildId = config.get('guildID')
    channelId = config.get('channelId')
    token = config.get('token')
    webhook = config.get('webhook')
    proxy = config.get('proxy', '')
    blacklistedRoles = config.get('blacklistedRoles', [])
    blacklistedUsers = config.get('blacklistedUsers', [])
    scan_interval = 60
    BATCH_SIZE = 20
    INDIVIDUAL_THRESHOLD = 5

if not token:
    raise ValueError("DISCORD_TOKEN is not set.")
if not guildId:
    raise ValueError("DISCORD_GUILD_ID is not set.")
if not channelId:
    raise ValueError("DISCORD_CHANNEL_ID is not set.")
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
        self.ranges = [[0, 0]]
        self.online_count = 0
        self.ranges_requested = 0
        self.ranges_received = 0
        self.packets_recv = 0
        self.last_scrape_time = time.time()

    def run(self):
        # Add timeout so we don't hang forever
        self.run_forever(ping_timeout=30, ping_interval=10)
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
                "channels": {self.channel_id: self.ranges}
            }
        }
        self.ranges_requested += len(self.ranges)
        logging.info(f"Requesting {len(self.ranges)} range(s): {self.ranges}")
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
        """Extract and store a member if valid. Returns True if member was added."""
        if not isinstance(item, dict) or "member" not in item:
            return False
        mem = item["member"]
        user = mem.get("user", {})
        if not user:
            return False
        user_id = user.get("id")
        if not user_id:
            return False
        if user.get("bot"):
            return False
        if user_id in self.blacklisted_users:
            return False
        if set(self.blacklisted_roles).intersection(mem.get("roles", [])):
            return False
        username = user.get('username', 'Unknown')
        discrim = user.get('discriminator', '0')
        tag = f"{username}#{discrim}" if discrim != "0" else f"@{username}"
        joined_at = mem.get('joined_at')
        self.members[user_id] = (tag, joined_at)
        return True

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
                    self.guilds[guild["id"]] = {
                        "member_count": guild.get("member_count", 0),
                        "online_count": guild.get("presence_count", 0)
                    }

            if t == "READY_SUPPLEMENTAL":
                guild_data = self.guilds.get(self.guild_id, {})
                self.online_count = guild_data.get("online_count", 0)
                logging.info(f"Guild online count from READY_SUPPLEMENTAL: {self.online_count}")

                if self.online_count > 0:
                    # Request all ranges at once (in batches of 10 to avoid rate limits)
                    self._request_all_ranges()
                else:
                    logging.warning("⚠️ Online count is 0 – cannot scrape.")
                    self.endScraping = True
                    self.close()

            elif t == "GUILD_MEMBER_LIST_UPDATE":
                parsed = Utils.parseGuildMemberListUpdate(decoded)
                if parsed.get('guild_id') != self.guild_id:
                    return

                # Update online count if provided in the update
                if parsed.get('online_count'):
                    self.online_count = parsed['online_count']

                # Track how many SYNC responses we've received
                sync_count = 0
                for elem, index in enumerate(parsed["types"]):
                    updates = parsed["updates"][elem]
                    if isinstance(updates, dict):
                        updates = [updates]
                    elif not isinstance(updates, list):
                        updates = []

                    if index == "SYNC":
                        sync_count += 1
                        self.ranges_received += 1
                        
                        if len(updates) == 0:
                            # Empty SYNC means no more members in this range
                            continue
                        
                        for item in updates:
                            self._process_member_item(item)

                    elif index in ("UPDATE", "INSERT"):
                        for item in updates:
                            self._process_member_item(item)

                # Check if we've received all ranges we requested
                if self.ranges_received >= self.ranges_requested and self.ranges_requested > 0:
                    logging.info(
                        f"✅ Finished scraping. Received {self.ranges_received} range(s). "
                        f"Total online members captured: {len(self.members)}"
                    )
                    self.endScraping = True
                    self.close()
                else:
                    # Check if we've captured enough members already
                    if self.online_count > 0 and len(self.members) >= self.online_count:
                        logging.info(
                            f"✅ Captured all {len(self.members)} online members. Closing."
                        )
                        self.endScraping = True
                        self.close()

            # Safety timeout - if no messages for 15 seconds after scraping started, close
            if time.time() - self.last_scrape_time > 15 and self.ranges_requested > 0:
                if not self.endScraping:
                    logging.warning("⚠️ Timeout - no updates received. Closing connection.")
                    self.endScraping = True
                    self.close()

        except Exception as e:
            logging.error(f"Error in sock_message: {e}")

    def _request_all_ranges(self):
        """Request all ranges for online members in batches."""
        if self.online_count <= 0:
            self.endScraping = True
            self.close()
            return

        # Calculate all needed ranges (100 members per range)
        all_ranges = []
        for i in range(0, self.online_count, 100):
            all_ranges.append([i, min(i + 99, self.online_count - 1)])

        logging.info(
            f"📊 Online members: {self.online_count}. "
            f"Need {len(all_ranges)} range(s) to scrape."
        )

        # Send in batches of 10 ranges to avoid rate limits
        BATCH_RANGES = 10
        self.ranges_requested = 0
        self.ranges_received = 0

        for i in range(0, len(all_ranges), BATCH_RANGES):
            batch = all_ranges[i:i + BATCH_RANGES]
            self.ranges = batch
            self.scrapeUsers()
            if i + BATCH_RANGES < len(all_ranges):
                time.sleep(0.5)  # Small delay between batches

        self.last_scrape_time = time.time()

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
def send_single_webhook(member_id, tag, join_time, max_retries=3):
    attempt = 0
    wait_time = 2
    while attempt <= max_retries:
        try:
            sess = session(token)
            guild_resp = sess.get(f'https://discord.com/api/v9/guilds/{guildId}')
            guild_name = guild_resp.json().get('name', 'Unknown')
            if tag.startswith('@'):
                clean_username = tag[1:]
            elif '#' in tag:
                clean_username = tag.split('#')[0]
            else:
                clean_username = tag
            join_str = join_time.strftime("%m-%d-%Y on %I:%M %p")
            payload = {
                "content": f"@here New User Joined {guildId}",
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
                logging.info(f"✅ Webhook sent for {member_id}")
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

def send_batch_webhook(batch, max_retries=3):
    if not batch:
        return
    attempt = 0
    wait_time = 2
    while attempt <= max_retries:
        try:
            sess = session(token)
            guild_resp = sess.get(f'https://discord.com/api/v9/guilds/{guildId}')
            guild_name = guild_resp.json().get('name', 'Unknown')

            fields = []
            for item in batch:
                member_id = item['member_id']
                tag = item['tag']
                join_time = item['join_time']
                clean_username = tag[1:] if tag.startswith('@') else tag.split('#')[0] if '#' in tag else tag
                join_str = join_time.strftime("%m-%d-%Y %I:%M %p")
                fields.append({
                    "name": "New Member",
                    "value": f"**{clean_username}**\nID: `{member_id}`\nJoined: {join_str}",
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
                logging.info(f"✅ Batch webhook sent for {len(batch)} members.")
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
def process_new_members(new_members_dict):
    if not new_members_dict:
        return

    now = datetime.datetime.now(datetime.timezone.utc)
    pending = []

    for member_id, (tag, joined_at) in new_members_dict.items():
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
                pending.append({
                    'member_id': member_id,
                    'tag': tag,
                    'join_time': join_time
                })
                notified_members.add(member_id)
        except Exception as e:
            logging.warning(f"Error processing {member_id}: {e}")

    if not pending:
        return

    # Decide: individual or batch?
    if len(pending) <= INDIVIDUAL_THRESHOLD:
        logging.info(f"📨 Sending {len(pending)} members individually.")
        for item in pending:
            send_single_webhook(item['member_id'], item['tag'], item['join_time'])
            time.sleep(2)
    else:
        logging.info(f"📦 Sending {len(pending)} members in batches of {BATCH_SIZE}.")
        for i in range(0, len(pending), BATCH_SIZE):
            batch = pending[i:i+BATCH_SIZE]
            send_batch_webhook(batch)
            if i + BATCH_SIZE < len(pending):
                time.sleep(2)

    save_notified_cache()
    logging.info("✅ Finished processing new members.")

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
    logging.info("Starting snitch (%ds interval, 2-day join window)...", scan_interval)

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
    logging.info("Configuration: guildId=%s, channelId=%s, token starts with %s..., webhook: %s",
                 guildId, channelId, token[:8], webhook_mask)

    # Wait for webhook to be ready (not rate-limited)
    wait_for_webhook_ready()

    logging.info("Building initial baseline...")
    current_members_raw = autoSnitch(token, guildId, channelId)
    current_ids = set(current_members_raw.keys())
    logging.info("Baseline built: %s members visible.", len(current_ids))

    logging.info("Checking baseline members for recent joins...")
    process_new_members(current_members_raw)

    while True:
        logging.info("🔄 Starting new scan cycle...")
        new_members_raw = autoSnitch(token, guildId, channelId)
        new_ids = set(new_members_raw.keys())
        logging.info("Scanned: %s members visible.", len(new_ids))

        diff_ids = new_ids - current_ids
        if diff_ids:
            diff_dict = {uid: new_members_raw[uid] for uid in diff_ids}
            logging.info("Found %s new IDs not in previous scan.", len(diff_dict))
            process_new_members(diff_dict)
        else:
            logging.info("No new members detected in this scan.")

        current_ids = new_ids
        logging.info("Sleeping %s seconds...", scan_interval)
        time.sleep(scan_interval)
