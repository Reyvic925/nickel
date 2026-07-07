import logging, os, datetime, time, json, threading, requests, tls_client, pickle
import websocket

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

logging.basicConfig(
    level=logging.INFO,
    format="\x1b[38;5;9m[\x1b[0m%(asctime)s\x1b[38;5;9m]\x1b[0m %(message)s\x1b[0m",
    datefmt="%H:%M:%S"
)

JOIN_WINDOW_SECONDS = 2 * 24 * 60 * 60
SEEN_CACHE_FILE = "seen_members.pkl"

if os.path.exists(SEEN_CACHE_FILE):
    with open(SEEN_CACHE_FILE, 'rb') as f:
        seen_members = pickle.load(f)
else:
    seen_members = set()

def save_seen_cache():
    with open(SEEN_CACHE_FILE, 'wb') as f:
        pickle.dump(seen_members, f)

# ---------- Utils ----------
class Utils:
    def parseGuildMemberListUpdate(response):
        memberdata = {
            "online_count": response["d"]["online_count"],
            "member_count": response["d"]["member_count"],
            "id": response["d"]["id"],
            "guild_id": response["d"]["guild_id"],
            "hoisted_roles": response["d"]["groups"],
            "types": [],
            "locations": [],
            "updates": []
        }
        for chunk in response['d']['ops']:
            memberdata['types'].append(chunk['op'])
            if chunk['op'] in ('SYNC', 'INVALIDATE'):
                memberdata['locations'].append(chunk['range'])
                if chunk['op'] == 'SYNC':
                    memberdata['updates'].append(chunk['items'])
                else:
                    memberdata['updates'].append([])
            elif chunk['op'] in ('INSERT', 'UPDATE', 'DELETE'):
                memberdata['locations'].append(chunk['index'])
                if chunk['op'] == 'DELETE':
                    memberdata['updates'].append([])
                else:
                    memberdata['updates'].append(chunk['item'])
        return memberdata

# ---------- WebSocket (with auto‑reconnect) ----------
class DiscordSocket(websocket.WebSocketApp):
    def __init__(self, token, guild_id, channel_id, on_ready=None):
        self.token = token
        self.guild_id = guild_id
        self.channel_id = channel_id
        self.blacklisted_roles = [str(r) for r in blacklistedRoles]
        self.blacklisted_users = [str(u) for u in blacklistedUsers]
        self.on_ready_callback = on_ready

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
            on_close=lambda ws, code, msg: self.sock_close(ws, code, msg),
            on_error=lambda ws, err: self.sock_error(ws, err)
        )
        self.endScraping = False
        self.guilds = {}
        self.members = {}
        self.chunk_size = 1000
        self.current_start = 0
        self.total_member_count = 0
        self.packets_recv = 0
        self.connected = False
        self.heartbeat_thread = None

    def run(self):
        self.run_forever(ping_interval=10, ping_timeout=5)

    def scrapeUsers(self):
        if self.endScraping:
            return
        end = self.current_start + self.chunk_size - 1
        self.ranges = [[self.current_start, end]]
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
            while not self.endScraping:
                self.send('{"op":1,"d":' + str(self.packets_recv) + '}')
                time.sleep(interval)
        except Exception:
            return

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
                interval = decoded["d"]["heartbeat_interval"] / 1000
                if self.heartbeat_thread and self.heartbeat_thread.is_alive():
                    return
                self.heartbeat_thread = threading.Thread(target=self.heartbeatThread, args=(interval,), daemon=True)
                self.heartbeat_thread.start()

            if t == "READY":
                for guild in decoded.get("d", {}).get("guilds", []):
                    self.guilds[guild["id"]] = {"member_count": guild.get("member_count", 0)}

            if t == "READY_SUPPLEMENTAL":
                member_count = self.guilds.get(self.guild_id, {}).get("member_count", 0)
                if member_count:
                    self.total_member_count = member_count
                    self.current_start = 0
                    self.connected = True
                    logging.info("WebSocket ready, starting scrape.")
                    if self.on_ready_callback:
                        self.on_ready_callback()
                    self.scrapeUsers()
                else:
                    logging.warning("⚠️ Member count is 0 – cannot scrape.")

            elif t == "GUILD_MEMBER_LIST_UPDATE":
                parsed = Utils.parseGuildMemberListUpdate(decoded)
                if parsed['guild_id'] != self.guild_id:
                    return

                self.total_member_count = parsed.get('member_count', self.total_member_count)

                for elem, op_type in enumerate(parsed["types"]):
                    updates = parsed["updates"][elem]
                    if isinstance(updates, dict):
                        updates = [updates]
                    elif not isinstance(updates, list):
                        updates = []

                    if op_type in ("SYNC", "UPDATE"):
                        for item in updates:
                            if "member" in item:
                                mem = item["member"]
                                user = mem.get("user", {})
                                if not user:
                                    continue
                                user_id = user.get("id")
                                if not user_id:
                                    continue
                                if set(self.blacklisted_roles).intersection(mem.get("roles", [])):
                                    continue
                                if user.get("bot"):
                                    continue
                                if user_id in self.blacklisted_users:
                                    continue
                                username = user.get('username', 'Unknown')
                                discrim = user.get('discriminator', '0')
                                tag = f"{username}#{discrim}" if discrim != "0" else f"@{username}"
                                joined_at = mem.get('joined_at')
                                self.members[user_id] = (tag, joined_at)

                if not self.endScraping:
                    self.current_start += self.chunk_size
                    if self.total_member_count > 0 and self.current_start >= self.total_member_count:
                        self.endScraping = True
                        self.close()
                    else:
                        self.scrapeUsers()

        except Exception as e:
            logging.error(f"Error in sock_message: {e}")

    def sock_close(self, ws, close_code, close_msg):
        logging.warning(f"WebSocket closed: code={close_code}, msg={close_msg}")
        self.connected = False
        if not self.endScraping:
            # Reconnect after a delay
            logging.info("Reconnecting in 5 seconds...")
            time.sleep(5)
            threading.Thread(target=self.run, daemon=True).start()

    def sock_error(self, ws, err):
        logging.error(f"WebSocket error: {err}")

# ---------- Helpers ----------
def autoSnitch(token, guild_id, channel_id):
    # Wait for the socket to finish scraping
    ready_event = threading.Event()
    def on_ready():
        ready_event.set()

    sb = DiscordSocket(token, guild_id, channel_id, on_ready=on_ready)
    # Run in a separate thread so we can wait
    thread = threading.Thread(target=sb.run, daemon=True)
    thread.start()
    # Wait until READY_SUPPLEMENTAL is received (or timeout)
    ready_event.wait(timeout=30)
    if not ready_event.is_set():
        logging.warning("Initial ready timeout, closing socket.")
        sb.close()
        return {}
    # Wait for scraping to complete
    while not sb.endScraping:
        time.sleep(1)
    return sb.members

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

def send_single_webhook(member_id, tag, join_time, max_retries=3):
    attempt = 0
    wait_time = 2
    while attempt <= max_retries:
        try:
            sess = session(token)
            guild_resp = sess.get(f'https://discord.com/api/v9/guilds/{guildId}')
            guild_name = guild_resp.json().get('name', 'Unknown')
            clean_username = tag[1:] if tag.startswith('@') else tag.split('#')[0] if '#' in tag else tag
            join_str = join_time.strftime("%m-%d-%Y on %I:%M %p")
            payload = {
                "content": f"<@{member_id}> just joined!",
                "embeds": [{
                    "color": 161791,
                    "author": {"name": "Snitched Successful"},
                    "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                    "fields": [
                        {"name": "Username", "value": clean_username, "inline": True},
                        {"name": "User ID", "value": member_id, "inline": True},
                        {"name": "Joined Server", "value": join_str, "inline": False},
                        {"name": "Guild", "value": guild_name, "inline": True}
                    ]
                }]
            }
            response = requests.post(webhook, json=payload)
            if response.status_code == 204:
                logging.info(f"✅ Webhook sent for {member_id}")
                return
            elif response.status_code == 429:
                data = response.json()
                retry_after = data.get('retry_after', wait_time)
                wait_time = max(wait_time, retry_after)
                logging.warning(f"Rate limited, waiting {wait_time}s...")
                time.sleep(wait_time)
                attempt += 1
                wait_time *= 2
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
                data = response.json()
                retry_after = data.get('retry_after', wait_time)
                wait_time = max(wait_time, retry_after)
                logging.warning(f"Batch rate limited, waiting {wait_time}s...")
                time.sleep(wait_time)
                attempt += 1
                wait_time *= 2
                continue
            else:
                logging.error(f"Batch webhook failed with status {response.status_code}: {response.text[:200]}")
                return
        except Exception as e:
            logging.error(f"Batch webhook exception: {e}")
            attempt += 1
            time.sleep(2)

def process_new_members(new_members_dict):
    if not new_members_dict:
        return

    now = datetime.datetime.now(datetime.timezone.utc)
    pending_notify = []
    global seen_members

    for member_id, (tag, joined_at) in new_members_dict.items():
        if not joined_at or not isinstance(joined_at, str):
            seen_members.add(member_id)
            continue
        try:
            join_time = datetime.datetime.fromisoformat(joined_at.replace('Z', '+00:00'))
            age = (now - join_time).total_seconds()
            if age <= JOIN_WINDOW_SECONDS:
                pending_notify.append({
                    'member_id': member_id,
                    'tag': tag,
                    'join_time': join_time
                })
        except Exception as e:
            logging.warning(f"Error parsing join time for {member_id}: {e}")
        seen_members.add(member_id)

    if pending_notify:
        if len(pending_notify) <= INDIVIDUAL_THRESHOLD:
            logging.info(f"📨 Sending {len(pending_notify)} members individually.")
            for item in pending_notify:
                send_single_webhook(item['member_id'], item['tag'], item['join_time'])
                time.sleep(2)
        else:
            logging.info(f"📦 Sending {len(pending_notify)} members in batches of {BATCH_SIZE}.")
            for i in range(0, len(pending_notify), BATCH_SIZE):
                batch = pending_notify[i:i+BATCH_SIZE]
                send_batch_webhook(batch)
                if i + BATCH_SIZE < len(pending_notify):
                    time.sleep(2)

    save_seen_cache()
    logging.info(f"✅ Finished processing. Seen members now: {len(seen_members)}")

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
                data = response.json()
                retry_after = data.get('retry_after', wait_time)
                wait_time = max(wait_time, retry_after)
                logging.warning(f"Webhook rate-limited, waiting {wait_time}s...")
                time.sleep(wait_time)
                attempt += 1
                wait_time *= 2
                continue
            else:
                logging.warning(f"Webhook check returned {response.status_code}. Proceeding anyway.")
                return True
        except Exception as e:
            logging.warning(f"Webhook check exception: {e}. Proceeding anyway.")
            return True

# ---------- Main ----------
if __name__ == '__main__':
    # 1. Start HTTP server FIRST
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
        def run_http_server():
            port = int(os.environ.get('PORT', 10000))
            server = HTTPServer(('0.0.0.0', port), HealthCheckHandler)
            logging.info(f"HTTP health check server started on port {port}")
            server.serve_forever()
        threading.Thread(target=run_http_server, daemon=True).start()
    except Exception as e:
        logging.warning("Could not start HTTP server: %s", e)

    # 2. Log config
    webhook_mask = webhook[:40] + "..." if len(webhook) > 40 else webhook
    logging.info("Starting snitch (%ds interval, 2-day join window)...", scan_interval)
    logging.info("Configuration: guildId=%s, channelId=%s, token starts with %s..., webhook: %s",
                 guildId, channelId, token[:8], webhook_mask)

    # 3. Run everything else in a background thread
    def main_loop():
        # Check webhook (non‑blocking)
        wait_for_webhook_ready()

        # Initial baseline
        logging.info("Building initial baseline and processing unseen members...")
        current_members = autoSnitch(token, guildId, channelId)
        logging.info("Scanned %s members.", len(current_members))

        new_members = {uid: data for uid, data in current_members.items() if uid not in seen_members}
        if new_members:
            logging.info("Found %s members not previously seen. Processing...", len(new_members))
            process_new_members(new_members)
        else:
            logging.info("No new members found in baseline.")

        # Main scan loop
        while True:
            current_members = autoSnitch(token, guildId, channelId)
            logging.info("Scanned %s members.", len(current_members))

            new_members = {uid: data for uid, data in current_members.items() if uid not in seen_members}
            if new_members:
                logging.info("Found %s new members.", len(new_members))
                process_new_members(new_members)
            else:
                logging.info("No new members found.")

            logging.info("Sleeping %s seconds...", scan_interval)
            time.sleep(scan_interval)

    threading.Thread(target=main_loop, daemon=False).start()

    # 4. Keep main thread alive
    while True:
        time.sleep(1)
