The error is happening because you are still pasting the conversational text into your `main.py` file. Python cannot read English sentences; it can only read code.

**Please follow these exact steps to fix it:**

1.  Open your `main.py` file in your code editor.
2.  **Delete everything** currently in the file.
3.  Copy **only** the code inside the black box below.
4.  Paste it into `main.py`.
5.  Save the file and push it to Render again.

```python
import logging, os, datetime, time, json, threading, requests, tls_client, pickle, sys, queue, tempfile
from concurrent.futures import ThreadPoolExecutor

# ---------- Global defaults ----------
DEFAULT_SCAN_INTERVAL = 60          # not used for persistent, kept for fallback
DEFAULT_BATCH_SIZE = 20
DEFAULT_INDIVIDUAL_THRESHOLD = 5
DEFAULT_JOIN_WINDOW = 2 * 24 * 60 * 60
DEFAULT_BLACKLISTED_ROLES = []
DEFAULT_BLACKLISTED_USERS = []

# ---------- Logging setup ----------
logging.basicConfig(
    level=logging.INFO,
    format="\x1b[38;5;9m[\x1b[0m%(asctime)s\x1b[38;5;9m]\x1b[0m [%(profile)s] %(message)s\x1b[0m",
    datefmt="%H:%M:%S"
)
class ProfileLogger(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        return msg, kwargs

# ---------- WebSocket imports ----------
try:
    import websocket
except ImportError:
    os.system("pip install websocket-client")
    import websocket

# ---------- Helper: Atomic Cache Saving ----------
def save_cache_atomic(filepath, data):
    """Saves data to a pickle file atomically to prevent corruption on crash."""
    dir_name = os.path.dirname(filepath) or '.'
    try:
        with tempfile.NamedTemporaryFile(dir=dir_name, delete=False, mode='wb') as f:
            pickle.dump(data, f)
            temp_name = f.name
        os.replace(temp_name, filepath)
    except Exception as e:
        logging.error(f"Failed to save cache {filepath}: {e}")

# ---------- Helper: Discord session ----------
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

# ---------- Helper: fetch joined_at ----------
def fetch_member_joined_at(user_id, token, guild_id):
    try:
        sess = session(token)
        resp = sess.get(f'https://discord.com/api/v9/guilds/{guild_id}/members/{user_id}')
        if resp.status_code == 200:
            return resp.json().get('joined_at')
        else:
            logging.warning(f"API fetch for {user_id} returned {resp.status_code}")
            return None
    except Exception as e:
        logging.error(f"Error fetching member {user_id}: {e}")
        return None

# ---------- Webhook sending ----------
def send_single_webhook(member_id, tag, join_time, webhook_url, token, guild_id, max_retries=3):
    attempt = 0
    wait_time = 2
    while attempt <= max_retries:
        try:
            sess = session(token)
            guild_resp = sess.get(f'https://discord.com/api/v9/guilds/{guild_id}')
            guild_name = guild_resp.json().get('name', 'Unknown')
            if tag.startswith('@'):
                clean_username = tag[1:]
            elif '#' in tag:
                clean_username = tag.split('#')[0]
            else:
                clean_username = tag
            join_str = join_time.strftime("%m-%d-%Y on %I:%M %p")
            payload = {
                "content": f"@here New User Joined {guild_id}",
                "embeds": [{
                    "color": 161791,
                    "author": {"name": "Snitched Successful"},
                    "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                    "fields": [
                        {"name": "Username", "value": f"[{clean_username}](https://discord.com/users/{member_id})", "inline": True},
                        {"name": "Full Tag (copy)", "value": f"`{tag}`", "inline": True},
                        {"name": "User ID", "value": member_id, "inline": True},
                        {"name": "Joined Server", "value": join_str, "inline": False},
                        {"name": "Mention", "value": f"<@{member_id}>", "inline": True},
                        {"name": "Guild", "value": guild_name, "inline": True}
                    ]
                }]
            }
            response = requests.post(webhook_url, json=payload)
            if response.status_code == 204:
                logging.info(f"✅ Webhook sent for {member_id}")
                return
            elif response.status_code == 429:
                data = response.json()
                retry_after = data.get('retry_after', wait_time)
                wait_time = max(wait_time, retry_after)
                logging.warning(f"Rate limited for {member_id}, waiting {wait_time}s...")
                time.sleep(wait_time)
                attempt += 1
                wait_time *= 2
            else:
                logging.error(f"Webhook failed with status {response.status_code}: {response.text[:200]}")
                return
        except Exception as e:
            logging.error(f"Webhook exception: {e}")
            attempt += 1
            time.sleep(2)

def send_batch_webhook(batch, webhook_url, token, guild_id, max_retries=3):
    if not batch:
        return
    attempt = 0
    wait_time = 2
    while attempt <= max_retries:
        try:
            sess = session(token)
            guild_resp = sess.get(f'https://discord.com/api/v9/guilds/{guild_id}')
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
                    "value": (
                        f"**Full Tag (copy):** `{tag}`\n"
                        f"**Profile:** [{clean_username}](https://discord.com/users/{member_id})\n"
                        f"**ID:** `{member_id}`\n"
                        f"**Joined:** {join_str}"
                    ),
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
            response = requests.post(webhook_url, json=payload)
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
            else:
                logging.error(f"Batch webhook failed with status {response.status_code}: {response.text[:200]}")
                return
        except Exception as e:
            logging.error(f"Batch webhook exception: {e}")
            attempt += 1
            time.sleep(2)

# ---------- Processing new members (used for initial baseline) ----------
def process_new_members(new_members_dict, guild_id, webhook_url, token,
                        notified_set, join_window_seconds, batch_size, individual_threshold,
                        logger):
    if not new_members_dict:
        return

    now = datetime.datetime.now(datetime.timezone.utc)
    pending = []

    for member_id, (tag, joined_at) in new_members_dict.items():
        if not joined_at:
            logger.info(f"Missing joined_at for {member_id}, fetching via API...")
            joined_at = fetch_member_joined_at(member_id, token, guild_id)
            if not joined_at:
                logger.warning(f"Could not fetch joined_at for {member_id}, skipping.")
                continue

        try:
            join_time = datetime.datetime.fromisoformat(joined_at.replace('Z', '+00:00'))
            age = (now - join_time).total_seconds()
            if age <= join_window_seconds:
                if member_id in notified_set:
                    continue
                pending.append({
                    'member_id': member_id,
                    'tag': tag,
                    'join_time': join_time
                })
                notified_set.add(member_id)
            else:
                logger.debug(f"Member {member_id} joined {age/3600:.1f} hours ago, skipping.")
        except Exception as e:
            logger.warning(f"Error processing {member_id}: {e}")

    if not pending:
        logger.info("No new members within window.")
        return

    if len(pending) <= individual_threshold:
        logger.info(f"📨 Sending {len(pending)} members individually.")
        for item in pending:
            send_single_webhook(item['member_id'], item['tag'], item['join_time'],
                                webhook_url, token, guild_id)
            time.sleep(2)
    else:
        logger.info(f"📦 Sending {len(pending)} members in batches of {batch_size}.")
        for i in range(0, len(pending), batch_size):
            batch = pending[i:i+batch_size]
            send_batch_webhook(batch, webhook_url, token, guild_id)
            if i + batch_size < len(pending):
                time.sleep(2)

    # Save cache atomically
    save_cache_atomic(f"notified_{guild_id}.pkl", notified_set)
    logger.info("✅ Finished processing initial members.")

# ---------- Initial full scan (uses existing autoSnitch logic) ----------
def autoSnitch(token, guild_id, channel_ids, blacklisted_roles, blacklisted_users, max_retries=3):
    all_members = {}
    for ch_id in channel_ids:
        for attempt in range(max_retries):
            try:
                logging.info(f"Scanning channel {ch_id} (attempt {attempt+1}/{max_retries}) ...")
                sb = DiscordSocket(token, guild_id, ch_id, blacklisted_roles, blacklisted_users)
                result = sb.run(timeout=45)
                if result:
                    logging.info(f"Channel {ch_id} returned {len(result)} members.")
                    all_members.update(result)
                    break
                else:
                    if sb.auth_failed:
                        raise Exception("Authentication failed")
                    if sb.rate_limited:
                        logging.warning(f"Rate limited on channel {ch_id}. Waiting 120s.")
                        time.sleep(120)
                    else:
                        logging.warning(f"Channel {ch_id} returned 0 members. Retrying...")
                        time.sleep(2 * (attempt + 1))
            except Exception as e:
                logging.error(f"Error scanning channel {ch_id}: {e}")
                if "Authentication failed" in str(e):
                    raise
                time.sleep(2 * (attempt + 1))
        else:
            logging.error(f"Failed to get members from channel {ch_id} after {max_retries} attempts.")
    return all_members

# ---------- DiscordSocket (for initial scan only) ----------
class DiscordSocket(websocket.WebSocketApp):
    def __init__(self, token, guild_id, channel_id, blacklisted_roles, blacklisted_users):
        self.token = token
        self.guild_id = guild_id
        self.channel_id = channel_id
        self.blacklisted_roles = [str(r) for r in blacklisted_roles]
        self.blacklisted_users = [str(u) for u in blacklisted_users]
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
        self.lastRange = 0
        self.packets_recv = 0
        self.rate_limited = False
        self.auth_failed = False

    def run(self, timeout=60):
        timer = threading.Timer(timeout, self.close)
        timer.daemon = True
        timer.start()
        self.run_forever()
        timer.cancel()
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
                    "system_locale": "en-US",
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
                threading.Thread(target=self.heartbeatThread, args=(decoded["d"]["heartbeat_interval"] / 1000,), daemon=True).start()

            if t == "READY":
                for guild in decoded.get("d", {}).get("guilds", []):
                    self.guilds[guild["id"]] = {"member_count": guild.get("member_count", 0)}

            if t == "READY_SUPPLEMENTAL":
                member_count = self.guilds.get(self.guild_id, {}).get("member_count", 0)
                if member_count == 0:
                    logging.warning(f"⚠️ Member count is 0 for channel {self.channel_id}. Closing socket.")
                    self.close()
                    return
                self.ranges = [[0, 99]]
                self.lastRange = 0
                self.scrapeUsers()

            elif t == "GUILD_MEMBER_LIST_UPDATE":
                parsed = self.parseGuildMemberListUpdate(decoded)
                if parsed['guild_id'] != self.guild_id:
                    return

                for elem, index in enumerate(parsed["types"]):
                    updates = parsed["updates"][elem]
                    if isinstance(updates, dict):
                        updates = [updates]
                    elif not isinstance(updates, list):
                        updates = []

                    if index == "SYNC":
                        if len(updates) == 0:
                            self.endScraping = True
                            break
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

                    elif index == "UPDATE":
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
                        self.lastRange += 1
                        self.ranges = [[self.lastRange*100, self.lastRange*100+99]]
                        self.scrapeUsers()

                if self.endScraping:
                    self.close()

        except Exception as e:
            logging.error(f"Error in sock_message: {e}")

    def sock_close(self, ws, close_code, close_msg):
        if close_msg and "Authentication failed" in close_msg:
            self.auth_failed = True
            logging.error("Authentication failed – token invalid.")
        elif close_msg and "Rate limited" in close_msg:
            self.rate_limited = True
            logging.warning(f"Rate limit detected on channel {self.channel_id}.")

    @staticmethod
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

# ---------- Persistent Listener ----------
class PersistentSnitch(websocket.WebSocketApp):
    def __init__(self, token, guild_id, channel_id, webhook_url,
                 blacklisted_roles, blacklisted_users,
                 notified_set, join_window_seconds, batch_size, individual_threshold,
                 logger):
        self.token = token
        self.guild_id = guild_id
        self.channel_id = channel_id
        self.webhook_url = webhook_url
        self.blacklisted_roles = [str(r) for r in blacklisted_roles]
        self.blacklisted_users = [str(u) for u in blacklisted_users]
        self.notified_set = notified_set
        self.join_window_seconds = join_window_seconds
        self.batch_size = batch_size
        self.individual_threshold = individual_threshold
        self.logger = logger

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
            on_close=lambda ws, close_code, close_msg: self.sock_close(ws, close_code, close_msg),
            on_error=lambda ws, err: self.sock_error(ws, err)
        )
        self.guilds = {}
        self.packets_recv = 0
        self.auth_failed = False
        self.running = True
        
        # Webhook queue to prevent blocking the WebSocket thread
        self.webhook_queue = queue.Queue()
        
        # Start background webhook sender
        threading.Thread(target=self._webhook_sender_loop, daemon=True).start()

    def _webhook_sender_loop(self):
        """Background thread to process webhooks without blocking the WebSocket."""
        while self.running:
            try:
                item = self.webhook_queue.get(timeout=1)
                if item is None:
                    break
                member_id, tag, join_time, webhook_url, token, guild_id = item
                send_single_webhook(member_id, tag, join_time, webhook_url, token, guild_id)
            except queue.Empty:
                continue
            except Exception as e:
                self.logger.error(f"Webhook sender error: {e}")

    def run_forever(self):
        while self.running:
            try:
                self.logger.info(f"Starting persistent listener for guild {self.guild_id} (channel {self.channel_id})...")
                super().run_forever()
                if self.auth_failed:
                    self.logger.error("Authentication failed. Stopping listener.")
                    break
                self.logger.warning("WebSocket closed. Reconnecting in 10s...")
                time.sleep(10)
            except Exception as e:
                self.logger.error(f"Listener error: {e}. Reconnecting in 10s...")
                time.sleep(10)

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
                    "system_locale": "en-US",
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

    def heartbeat_thread(self, interval):
        while self.running:
            try:
                time.sleep(interval)
                self.send('{"op":1,"d":' + str(self.packets_recv) + '}')
            except Exception:
                break

    def resubscribe(self):
        """Re-subscribes to the member list if an INVALIDATE op is received."""
        payload = {
            "op": 14,
            "d": {
                "guild_id": self.guild_id,
                "typing": True,
                "activities": True,
                "threads": True,
                "channels": {self.channel_id: [[0, 99]]}
            }
        }
        self.send(json.dumps(payload))

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
                threading.Thread(target=self.heartbeat_thread, args=(interval,), daemon=True).start()

            if t == "READY":
                for guild in decoded.get("d", {}).get("guilds", []):
                    self.guilds[guild["id"]] = {"member_count": guild.get("member_count", 0)}

            if t == "READY_SUPPLEMENTAL":
                # Subscribe to member list updates for this channel
                payload = {
                    "op": 14,
                    "d": {
                        "guild_id": self.guild_id,
                        "typing": True,
                        "activities": True,
                        "threads": True,
                        "channels": {self.channel_id: [[0, 99]]}  # small initial sync
                    }
                }
                self.send(json.dumps(payload))

            elif t == "GUILD_MEMBER_LIST_UPDATE":
                parsed = self.parse_member_update(decoded)
                if parsed['guild_id'] != self.guild_id:
                    return

                for elem, op_type in enumerate(parsed["types"]):
                    updates = parsed["updates"][elem]
                    if isinstance(updates, dict):
                        updates = [updates]
                    elif not isinstance(updates, list):
                        updates = []

                    # Handle INVALIDATE op (Discord telling us to re-subscribe)
                    if op_type == "INVALIDATE":
                        self.logger.warning("Received INVALIDATE. Re-subscribing to member list...")
                        self.resubscribe()
                        continue

                    # Only process INSERT ops (new members)
                    if op_type != "INSERT":
                        continue

                    for item in updates:
                        if "member" not in item:
                            continue
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

                        # Process this new member
                        self.process_single_new_member(user_id, tag, joined_at)

        except Exception as e:
            self.logger.error(f"Error in sock_message: {e}")

    def process_single_new_member(self, member_id, tag, joined_at):
        now = datetime.datetime.now(datetime.timezone.utc)

        if not joined_at:
            self.logger.info(f"Missing joined_at for {member_id}, fetching via API...")
            joined_at = fetch_member_joined_at(member_id, self.token, self.guild_id)
            if not joined_at:
                # Fallback: use current time if API fails, so we still snitch them
                self.logger.warning(f"Could not fetch joined_at for {member_id}. Using current time.")
                joined_at = now.isoformat()

        try:
            join_time = datetime.datetime.fromisoformat(joined_at.replace('Z', '+00:00'))
            age = (now - join_time).total_seconds()
            if age > self.join_window_seconds:
                self.logger.debug(f"Member {member_id} joined {age/3600:.1f} hours ago, skipping.")
                return
            if member_id in self.notified_set:
                return

            self.notified_set.add(member_id)
            
            # Queue the webhook instead of sending it synchronously
            self.webhook_queue.put((
                member_id, tag, join_time,
                self.webhook_url, self.token, self.guild_id
            ))
            
            # Save cache atomically
            save_cache_atomic(f"notified_{self.guild_id}.pkl", self.notified_set)

        except Exception as e:
            self.logger.warning(f"Error processing new member {member_id}: {e}")

    def sock_close(self, ws, close_code, close_msg):
        if close_msg and "Authentication failed" in close_msg:
            self.auth_failed = True
            self.logger.error("Authentication failed.")
        elif close_msg and "Rate limited" in close_msg:
            self.logger.warning("Rate limited. Will reconnect.")
        else:
            self.logger.warning(f"Closed: {close_code} - {close_msg}")

    def sock_error(self, ws, error):
        self.logger.error(f"WebSocket error: {error}")

    @staticmethod
    def parse_member_update(response):
        memberdata = {
            "guild_id": response["d"]["guild_id"],
            "types": [],
            "updates": []
        }
        for chunk in response['d']['ops']:
            memberdata['types'].append(chunk['op'])
            if chunk['op'] in ('SYNC', 'INVALIDATE'):
                if chunk['op'] == 'SYNC':
                    memberdata['updates'].append(chunk['items'])
                else:
                    memberdata['updates'].append([])
            elif chunk['op'] in ('INSERT', 'UPDATE', 'DELETE'):
                if chunk['op'] == 'DELETE':
                    memberdata['updates'].append([])
                else:
                    memberdata['updates'].append(chunk['item'])
        return memberdata

# ---------- Profile runner ----------
def run_profile(profile):
    profile_name = profile.get('profile_name', 'default')
    token = profile.get('token')
    if not token:
        logging.error(f"Profile {profile_name}: missing token, skipping.")
        return

    # Override defaults
    scan_interval = profile.get('scan_interval', DEFAULT_SCAN_INTERVAL)  # not used, kept for compatibility
    batch_size = profile.get('batch_size', DEFAULT_BATCH_SIZE)
    individual_threshold = profile.get('individual_threshold', DEFAULT_INDIVIDUAL_THRESHOLD)
    join_window_seconds = profile.get('join_window_seconds', DEFAULT_JOIN_WINDOW)
    blacklisted_roles = profile.get('blacklistedRoles', DEFAULT_BLACKLISTED_ROLES)
    blacklisted_users = profile.get('blacklistedUsers', DEFAULT_BLACKLISTED_USERS)

    guilds = profile.get('guilds', [])
    if not guilds:
        logging.error(f"Profile {profile_name}: no guilds defined, skipping.")
        return

    logger = ProfileLogger(logging.getLogger(), {'profile': profile_name})

    # We'll start a persistent listener for each guild
    listeners = []

    for g in guilds:
        guild_id = g['guildId']
        channel_ids = g['channelIds']
        webhook_url = g['webhook']
        if not channel_ids:
            logger.error(f"Guild {guild_id}: no channel IDs, skipping.")
            continue

        # Load notified cache per guild
        cache_file = f"notified_{guild_id}.pkl"
        if os.path.exists(cache_file):
            with open(cache_file, 'rb') as f:
                notified_set = pickle.load(f)
        else:
            notified_set = set()

        # --- STEP 1: Initial full scan for this guild ---
        logger.info(f"Performing initial baseline scan for guild {guild_id}...")
        try:
            members = autoSnitch(token, guild_id, channel_ids, blacklisted_roles, blacklisted_users)
            logger.info(f"Guild {guild_id}: {len(members)} members visible.")
            # Process recent joins from baseline
            process_new_members(members, guild_id, webhook_url, token,
                                notified_set, join_window_seconds, batch_size, individual_threshold,
                                logger)
        except Exception as e:
            logger.error(f"Baseline scan failed for guild {guild_id}: {e}")
            if "Authentication failed" in str(e):
                logger.error("Token invalid – profile stopped.")
                return
            # If baseline fails, we skip this guild
            continue

        # --- STEP 2: Start persistent listener on the first channel (member list is global) ---
        channel_id = channel_ids[0]  # only need one channel to get member updates
        logger.info(f"Starting persistent listener for guild {guild_id} on channel {channel_id}...")
        listener = PersistentSnitch(
            token, guild_id, channel_id, webhook_url,
            blacklisted_roles, blacklisted_users,
            notified_set, join_window_seconds, batch_size, individual_threshold,
            logger
        )
        t = threading.Thread(target=listener.run_forever, daemon=True)
        t.start()
        listeners.append(listener)

    # Keep the profile alive (just sleep; listeners run in daemon threads)
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        pass

# ---------- Config loading ----------
def load_config():
    if 'DISCORD_TOKEN' in os.environ:
        token = os.environ.get('DISCORD_TOKEN').strip()
        raw_guild = os.environ.get('DISCORD_GUILD_ID', '')
        if ',' in raw_guild:
            guildId = raw_guild.split(',')[0].strip()
        else:
            guildId = raw_guild
        channel_id_env = os.environ.get('DISCORD_CHANNEL_ID', '')
        if ',' in channel_id_env:
            channelIds = [ch.strip() for ch in channel_id_env.split(',') if ch.strip()]
        else:
            channelIds = [channel_id_env] if channel_id_env else []
        webhook = os.environ.get('DISCORD_WEBHOOK')
        if not webhook:
            raise ValueError("DISCORD_WEBHOOK not set in environment.")
        profile = {
            "profile_name": "env_profile",
            "token": token,
            "scan_interval": int(os.environ.get('SCAN_INTERVAL', DEFAULT_SCAN_INTERVAL)),
            "batch_size": int(os.environ.get('BATCH_SIZE', DEFAULT_BATCH_SIZE)),
            "individual_threshold": int(os.environ.get('INDIVIDUAL_THRESHOLD', DEFAULT_INDIVIDUAL_THRESHOLD)),
            "blacklistedRoles": json.loads(os.environ.get('DISCORD_BLACKLISTED_ROLES', '[]')),
            "blacklistedUsers": json.loads(os.environ.get('DISCORD_BLACKLISTED_USERS', '[]')),
            "guilds": [{
                "guildId": guildId,
                "channelIds": channelIds,
                "webhook": webhook
            }]
        }
        return [profile]

    try:
        with open('config.json', 'r') as f:
            config = json.load(f)
        if isinstance(config, list):
            return config
        else:
            return [{
                "profile_name": "default",
                "token": config.get('token'),
                "scan_interval": config.get('scan_interval', DEFAULT_SCAN_INTERVAL),
                "batch_size": config.get('batch_size', DEFAULT_BATCH_SIZE),
                "individual_threshold": config.get('individual_threshold', DEFAULT_INDIVIDUAL_THRESHOLD),
                "blacklistedRoles": config.get('blacklistedRoles', []),
                "blacklistedUsers": config.get('blacklistedUsers', []),
                "guilds": [{
                    "guildId": config.get('guildID'),
                    "channelIds": config.get('channelIds') or [config.get('channelId')],
                    "webhook": config.get('webhook')
                }]
            }]
    except FileNotFoundError:
        raise SystemExit("No config.json found and no DISCORD_TOKEN in environment.")

# ---------- Health check server ----------
def start_health_server():
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
        port = int(os.environ.get('PORT', 10000))
        server = HTTPServer(('0.0.0.0', port), HealthCheckHandler)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        logging.info(f"HTTP health check server started on port {port}")
    except Exception as e:
        logging.warning(f"Could not start HTTP server: {e}")

# ---------- Main ----------
if __name__ == '__main__':
    start_health_server()
    profiles = load_config()
    logging.info(f"Loaded {len(profiles)} profile(s).")

    threads = []
    for p in profiles:
        t = threading.Thread(target=run_profile, args=(p,), daemon=True)
        t.start()
        threads.append(t)

    try:
        while True:
            time.sleep(10)
    except KeyboardInterrupt:
        logging.info("Shutting down...")
        sys.exit(0)
```
