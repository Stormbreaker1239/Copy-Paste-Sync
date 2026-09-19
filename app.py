import asyncio
import json
import uuid
import sys
import os
import base64
import struct
import socket
import time
import logging
from logging.handlers import RotatingFileHandler
import webbrowser
import threading
from datetime import datetime
from typing import Dict, List, Optional

import gradio as gr
from PySide6.QtWidgets import QApplication, QSystemTrayIcon, QMenu, QStyle
from PySide6.QtGui import QClipboard, QImage
from PySide6.QtCore import QByteArray, QBuffer, QIODevice, QUrl, QMimeData, QObject, Signal
from qasync import QEventLoop
from zeroconf.asyncio import AsyncZeroconf
from zeroconf import ServiceInfo
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

import discovery

# --- LOGGING SETUP ---
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)
logger = logging.getLogger("ClipSync")
logger.setLevel(logging.INFO)

formatter = logging.Formatter('[%(asctime)s] [%(levelname)s] [%(name)s]: %(message)s')
file_handler = RotatingFileHandler(os.path.join(LOG_DIR, "clipsync.log"), maxBytes=5*1024*1024, backupCount=3)
file_handler.setFormatter(formatter)
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(console_handler)

# --- CONFIG & PATHS ---
CONFIG_FILE = "sync_config.json"
DEFAULT_PORT = 5555
DOWNLOAD_DIR = os.path.abspath("ClipSync_Downloads")
os.makedirs(DOWNLOAD_DIR, exist_ok=True)
STATIC_SALT = b'clipboard_sync_v1_production_salt'
PROTOCOL_MAGIC = b'CS'  # ClipSync Magic Header

def load_config() -> dict:
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, "r") as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Failed to load config: {e}")
    return {"join_code": "1234", "relay_url": "wss://relay.clipsync.io"}

def save_config(join_code: str):
    try:
        with open(CONFIG_FILE, "w") as f:
            json.dump({"join_code": join_code}, f)
    except Exception as e:
        logger.error(f"Failed to save config: {e}")

def find_free_port(start_port=7860, max_attempts=100) -> int:
    for port in range(start_port, start_port + max_attempts):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                s.bind(("127.0.0.1", port))
                return port
            except socket.error:
                continue
    return start_port

# --- CRYPTOGRAPHY ---
def generate_key(join_code: str) -> bytes:
    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=32,
        salt=STATIC_SALT,
        iterations=100000,
    )
    return base64.urlsafe_b64encode(kdf.derive(join_code.encode()))

class CryptoManager:
    def __init__(self, join_code: str):
        self.fernet = Fernet(generate_key(join_code))

    def encrypt(self, payload: bytes) -> str:
        return self.fernet.encrypt(payload).decode('utf-8')

    def decrypt(self, token: str) -> bytes:
        try:
            return self.fernet.decrypt(token.encode('utf-8'))
        except Exception as e:
            logger.error(f"Decryption error: {e}")
            return b""

# --- BINARY PROTOCOL LAYER ---
async def send_msg(writer: asyncio.StreamWriter, data_dict: dict):
    """Sends framed message: [Magic 2B][Length 4B][JSON Payload]"""
    payload = json.dumps(data_dict).encode('utf-8')
    header = PROTOCOL_MAGIC + struct.pack('!I', len(payload))
    writer.write(header + payload)
    await writer.drain()

async def recv_msg(reader: asyncio.StreamReader) -> Optional[dict]:
    """Reads framed message safely, checking Magic Header."""
    try:
        magic = await reader.readexactly(2)
        if magic != PROTOCOL_MAGIC:
            logger.warning("Received invalid protocol header magic bytes.")
            return None
        header = await reader.readexactly(4)
        length = struct.unpack('!I', header)[0]
        data = await reader.readexactly(length)
        return json.loads(data.decode('utf-8'))
    except asyncio.IncompleteReadError:
        return None
    except Exception as e:
        logger.error(f"Protocol receive error: {e}")
        return None

# --- STATE MANAGEMENT ---
class AppState:
    def __init__(self):
        conf = load_config()
        self.mode = "IDLE"  # "IDLE", "HOST", "CLIENT"
        self.join_code = conf.get("join_code", "1234")
        self.relay_url = conf.get("relay_url", None)
        self.status = "System Standby"
        
        # Room Meta State
        self.is_connected = False
        self.is_host = False
        self.room_key = self.join_code
        self.room_name = ""
        self.username = ""
        self.host_name = ""
        self.discovery_mode = "LAN (mDNS)"
        self.visibility_modes = ["LAN (mDNS)"]
        
        # Audio/Copy Records
        self.copy_logs: List[List] = []  # [[Sr No, User, Content, Time/Date]]
        self.connected_clients: Dict[str, Dict] = {}  # {client_id: {name, join_time, ip, writer}}
        self.blocked_clients: List[str] = []
        
        # Operational Handles
        self.hub_manager = None
        self.active_task = None
        self.crypto = CryptoManager(self.join_code)
        self.last_sync_id = None
        self.processing_remote = False
        self.last_synced_time = "Never"
        self.ui_port = 7860
        self.aiozc = None

state = AppState()

# --- QT THREAD-SAFE CLIPBOARD INTERACTION ---
class SafeClipboard(QObject):
    update_signal = Signal(dict)

    def __init__(self, app: QApplication):
        super().__init__()
        self.app = app
        self.clipboard = app.clipboard()
        self.update_signal.connect(self._apply_clipboard)

    def dispatch_update(self, payload: dict):
        self.update_signal.emit(payload)

    def _apply_clipboard(self, payload: dict):
        c_type = payload.get("type")
        data = payload.get("data")
        
        if c_type == "text":
            self.clipboard.setText(data)
        elif c_type == "image":
            image = QImage.fromData(data)
            if not image.isNull():
                self.clipboard.setImage(image, QClipboard.Clipboard)
        elif c_type == "file":
            target_path = payload.get("path")
            url = QUrl.fromLocalFile(target_path)
            mime = QMimeData()
            mime.setUrls([url])
            self.clipboard.setMimeData(mime)

# --- CONNECTION MANAGER (HOST ENGINE) ---
class ConnectionManager:
    def __init__(self):
        self.clients: Dict[asyncio.StreamWriter, str] = {}
        self.client_meta: Dict[str, dict] = {}

    async def register(self, writer: asyncio.StreamWriter, client_name: str, client_id: str) -> str:
        peer_ip = writer.get_extra_info('peername')[0]
        
        # Block enforcement check
        if client_id in state.blocked_clients or peer_ip in state.blocked_clients:
            await send_msg(writer, {"type": "blocked", "reason": "Blocked by room host."})
            writer.close()
            await writer.wait_closed()
            return ""

        self.clients[writer] = client_id
        join_time = datetime.now().strftime("%H:%M:%S")
        self.client_meta[client_id] = {
            "name": client_name,
            "ip": peer_ip,
            "join_time": join_time,
            "writer": writer
        }
        state.connected_clients[client_id] = self.client_meta[client_id]
        logger.info(f"Peer registered: {client_name} ({client_id}) from {peer_ip}")
        return client_id

    async def unregister(self, writer: asyncio.StreamWriter):
        if writer in self.clients:
            client_id = self.clients[writer]
            logger.info(f"Peer disconnected: {client_id}")
            del self.clients[writer]
            if client_id in self.client_meta:
                del self.client_meta[client_id]
            if client_id in state.connected_clients:
                del state.connected_clients[client_id]
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass

    async def kick_client(self, client_id: str) -> bool:
        if client_id in self.client_meta:
            writer = self.client_meta[client_id]["writer"]
            try:
                await send_msg(writer, {"type": "kicked", "reason": "Host removed you from the room."})
            except Exception:
                pass
            await self.unregister(writer)
            return True
        return False

    async def broadcast(self, msg_dict: dict, sender_writer: Optional[asyncio.StreamWriter] = None):
        tasks = [self._safe_send(w, msg_dict) for w in list(self.clients.keys()) if w != sender_writer]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _safe_send(self, writer: asyncio.StreamWriter, msg_dict: dict):
        try:
            await send_msg(writer, msg_dict)
        except Exception as e:
            logger.warning(f"Broadcast failure to peer, removing: {e}")
            await self.unregister(writer)

state.hub_manager = ConnectionManager()

# --- WATCHER & LISTENER PIPELINES ---
async def clipboard_watcher(safe_cb: SafeClipboard, writer: asyncio.StreamWriter):
    clipboard = safe_cb.clipboard
    last_text = ""
    last_img_hash = 0
    last_urls = []

    while state.mode in ["CLIENT", "HOST"]:
        if state.processing_remote:
            await asyncio.sleep(0.2)
            continue

        try:
            mime = clipboard.mimeData()

            # 1. FILE TRANSMISSION (Max 150MB safeguard)
            if mime.hasUrls():
                current_urls = [url.toLocalFile() for url in mime.urls() if url.isLocalFile()]
                if current_urls and current_urls != last_urls:
                    last_urls = current_urls
                    target_file = current_urls[0]

                    if os.path.exists(target_file) and os.path.isfile(target_file):
                        file_size = os.path.getsize(target_file)
                        if file_size <= 150 * 1024 * 1024:
                            filename = os.path.basename(target_file)
                            state.status = f"Streaming file: {filename}..."
                            
                            with open(target_file, "rb") as f:
                                encrypted_payload = state.crypto.encrypt(f.read())

                            state.last_sync_id = str(uuid.uuid4())
                            ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            
                            msg_payload = {
                                "type": "clip", "id": state.last_sync_id,
                                "content_type": "file", "filename": filename,
                                "content": encrypted_payload, "sender": state.username
                            }
                            
                            if state.mode == "HOST":
                                await state.hub_manager.broadcast(msg_payload)
                            else:
                                await send_msg(writer, msg_payload)
                                
                            state.copy_logs.insert(0, [len(state.copy_logs) + 1, state.username, f"📁 File: {filename}", ts])
                            state.status = "Sync Active"

            # 2. TEXT TRANSMISSION
            elif mime.hasText() and mime.text() != last_text:
                last_text = mime.text()
                state.last_sync_id = str(uuid.uuid4())
                ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                encrypted_payload = state.crypto.encrypt(last_text.encode('utf-8'))
                
                msg_payload = {
                    "type": "clip", "id": state.last_sync_id,
                    "content_type": "text", "content": encrypted_payload, "sender": state.username
                }

                if state.mode == "HOST":
                    await state.hub_manager.broadcast(msg_payload)
                else:
                    await send_msg(writer, msg_payload)
                    
                preview = (last_text[:35] + '...') if len(last_text) > 35 else last_text
                state.copy_logs.insert(0, [len(state.copy_logs) + 1, state.username, preview, ts])

            # 3. IMAGE TRANSMISSION
            elif mime.hasImage():
                img = clipboard.image()
                current_hash = hash(img.cacheKey())

                if not img.isNull() and current_hash != last_img_hash:
                    last_img_hash = current_hash
                    ba = QByteArray()
                    buffer = QBuffer(ba)
                    buffer.open(QIODevice.WriteOnly)
                    img.save(buffer, "PNG")

                    encrypted_payload = state.crypto.encrypt(ba.data().data())
                    state.last_sync_id = str(uuid.uuid4())
                    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                    msg_payload = {
                        "type": "clip", "id": state.last_sync_id,
                        "content_type": "image", "content": encrypted_payload, "sender": state.username
                    }

                    if state.mode == "HOST":
                        await state.hub_manager.broadcast(msg_payload)
                    else:
                        await send_msg(writer, msg_payload)
                        
                    state.copy_logs.insert(0, [len(state.copy_logs) + 1, state.username, "🖼️ Image", ts])

        except Exception as e:
            logger.error(f"Watcher Loop Error: {e}", exc_info=True)

        await asyncio.sleep(0.5)

async def clipboard_listener(reader: asyncio.StreamReader, safe_cb: SafeClipboard, tray: QSystemTrayIcon):
    while state.mode in ["CLIENT", "HOST"]:
        msg = await recv_msg(reader)
        if not msg:
            continue

        msg_type = msg.get("type")

        if msg_type == "kicked" or msg_type == "room_deleted":
            reason = "Host has ended the room." if msg_type == "room_deleted" else "Kicked by host."
            state.status = f"Terminated: {reason}"
            state.mode = "IDLE"
            state.is_connected = False
            tray.showMessage("ClipSync Alert", reason, QSystemTrayIcon.Warning, 3000)
            break

        # Anti-Echo Dropper
        if msg.get("id") == state.last_sync_id and state.last_sync_id is not None:
            continue

        if msg_type == "clip":
            raw_bytes = state.crypto.decrypt(msg.get("content", ""))
            if not raw_bytes:
                logger.warning("Failed to decrypt incoming packet.")
                continue

            state.processing_remote = True
            c_type = msg.get("content_type", "text")
            sender = msg.get("sender", "Peer")
            ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            state.last_synced_time = ts
            state.last_sync_id = msg.get("id")

            log_entry_content = ""
            if c_type == "text":
                decoded_text = raw_bytes.decode('utf-8')
                safe_cb.dispatch_update({"type": "text", "data": decoded_text})
                log_entry_content = (decoded_text[:35] + '...') if len(decoded_text) > 35 else decoded_text
            elif c_type == "image":
                safe_cb.dispatch_update({"type": "image", "data": raw_bytes})
                log_entry_content = "🖼️ Image"
            elif c_type == "file":
                filename = msg.get("filename", "synced_file")
                target_path = os.path.join(DOWNLOAD_DIR, filename)
                with open(target_path, "wb") as f:
                    f.write(raw_bytes)
                safe_cb.dispatch_update({"type": "file", "path": target_path})
                log_entry_content = f"📁 File: {filename}"

            state.copy_logs.insert(0, [len(state.copy_logs) + 1, sender, log_entry_content, ts])
            tray.showMessage("ClipSync", f"Synced {c_type.upper()} from {sender}", QSystemTrayIcon.Information, 1500)
            
            # If we are Host, re-broadcast to other clients
            if state.mode == "HOST":
                await state.hub_manager.broadcast(msg, reader)

            await asyncio.sleep(0.8)
            state.processing_remote = False

# --- ENGINE EXECUTORS ---
async def run_as_host(safe_cb: SafeClipboard, tray: QSystemTrayIcon):
    local_ip = socket.gethostbyname(socket.gethostname())
    state.status = f"Hosting Room: {local_ip}:{DEFAULT_PORT}"
    state.aiozc = AsyncZeroconf()

    unique_server_name = f"hub-{uuid.uuid4().hex[:4]}.local."
    info = ServiceInfo("_clip-sync._tcp.local.", f"Hub-{state.room_key}._clip-sync._tcp.local.",
                        addresses=[socket.inet_aton(local_ip)], port=DEFAULT_PORT, server=unique_server_name)
    await state.aiozc.async_register_service(info)

    async def handle_peer(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        client_id = None
        try:
            auth = await recv_msg(reader)
            if auth and auth.get("code") == state.room_key:
                c_name = auth.get("username", "Unknown Peer")
                c_id = auth.get("client_id", str(uuid.uuid4())[:8])
                
                client_id = await state.hub_manager.register(writer, c_name, c_id)
                if client_id:
                    await send_msg(writer, {"type": "auth_ok", "room_name": state.room_name, "host_name": state.host_name})
                    await clipboard_listener(reader, safe_cb, tray)
            else:
                await send_msg(writer, {"type": "auth_fail", "reason": "Invalid Security Key."})
                writer.close()
                await writer.wait_closed()
        except Exception as e:
            logger.error(f"Host Connection Handling Error: {e}")
        finally:
            await state.hub_manager.unregister(writer)

    server = await asyncio.start_server(handle_peer, '0.0.0.0', DEFAULT_PORT)
    
    # Run clipboard watcher locally on Host
    watcher_task = asyncio.create_task(clipboard_watcher(safe_cb, None))

    try:
        async with server:
            await server.serve_forever()
    except asyncio.CancelledError:
        pass
    finally:
        watcher_task.cancel()
        await state.hub_manager.broadcast({"type": "room_deleted"})
        if state.aiozc:
            await state.aiozc.async_unregister_all_services()
            await state.aiozc.close()

async def run_as_client(safe_cb: SafeClipboard, tray: QSystemTrayIcon):
    state.status = "Searching for Room..."
    state.aiozc = AsyncZeroconf()
    
    try:
        res = await discovery.discover(state.room_key, state.aiozc, relay_url=state.relay_url)
        if res and res.get("endpoint"):
            host, port = res["endpoint"]
            state.status = f"Connecting to {host}:{port}..."
            reader, writer = await asyncio.open_connection(host, port)
            
            client_id = str(uuid.uuid4())[:8]
            await send_msg(writer, {
                "type": "join", 
                "code": state.room_key, 
                "username": state.username, 
                "client_id": client_id
            })
            resp = await recv_msg(reader)

            if resp and resp.get("type") == "auth_ok":
                state.status = "Sync Active"
                state.is_connected = True
                await asyncio.gather(
                    clipboard_watcher(safe_cb, writer),
                    clipboard_listener(reader, safe_cb, tray)
                )
            else:
                reason = resp.get("reason", "Authentication failed.") if resp else "Server refused connection."
                state.status = f"Connection Failed: {reason}"
                state.mode = "IDLE"
                state.is_connected = False
        else:
            state.status = "ROOM_NOT_FOUND"
            state.mode = "IDLE"
            state.is_connected = False
    except Exception as e:
        logger.warning(f"Client connection error: {e}")
        state.status = f"Network failure: {str(e)}"
        state.mode = "IDLE"
        state.is_connected = False
    finally:
        if state.aiozc:
            await state.aiozc.async_close()

# --- DIAGNOSTIC HELPERS & GRADIO EVENT HANDLERS ---
def handle_join_room(room_key: str, username: str, discovery_mode: str):
    if not room_key.strip():
        gr.Warning("Room Key cannot be empty.")
        return [gr.update()] * 6
    if not username.strip():
        gr.Warning("Please enter a username before joining.")
        return [gr.update()] * 6

    state.room_key = room_key.strip()
    state.username = username.strip()
    state.discovery_mode = discovery_mode
    state.crypto = CryptoManager(state.room_key)
    save_config(state.room_key)
    
    state.mode = "CLIENT"
    
    # Probe connection status up to 4s
    start_time = time.time()
    while time.time() - start_time < 4.0:
        if state.is_connected:
            break
        if state.status == "ROOM_NOT_FOUND" or "Failed" in state.status or state.mode == "IDLE":
            break
        time.sleep(0.2)

    if not state.is_connected:
        err_msg = state.status
        state.mode = "IDLE"
        if err_msg == "ROOM_NOT_FOUND":
            gr.Warning(f"Room with Key '{state.room_key}' does not exist! Please double check the key or host a room.")
        else:
            gr.Error(f"Connection Failed: {err_msg}")
        return [gr.update()] * 6

    gr.Info("Successfully connected to the room!")
    
    return [
        gr.update(visible=False),                     # landing_container
        gr.update(visible=True),                      # dashboard_container
        f"### Room Key: `{state.room_key}`",          # ui_room_name
        f"🔑 Security Key: **{state.room_key}**",     # ui_room_key_display
        f"Host: {state.host_name or 'Remote Host'}",   # ui_host_name
        gr.update(visible=False),                     # host_management_panel
    ]

def handle_host_room(room_key: str, room_name: str, host_name: str, visibility: List[str]):
    if not room_key.strip() or not room_name.strip() or not host_name.strip():
        gr.Warning("Please complete all host parameters (Key, Room Name, Host Name).")
        return [gr.update()] * 6

    state.room_key = room_key.strip()
    state.room_name = room_name.strip()
    state.username = host_name.strip()
    state.host_name = f"{state.username} (You)"
    state.visibility_modes = visibility
    state.crypto = CryptoManager(state.room_key)
    save_config(state.room_key)
    
    state.is_host = True
    state.is_connected = True
    state.mode = "HOST"

    gr.Info(f"Room '{state.room_name}' created and broadcasting!")

    return [
        gr.update(visible=False),                     # landing_container
        gr.update(visible=True),                      # dashboard_container
        f"### Room: {state.room_name}",              # ui_room_name
        f"🔑 Security Key: **{state.room_key}**",     # ui_room_key_display
        f"Host: {state.host_name}",                   # ui_host_name
        gr.update(visible=True),                      # host_management_panel
    ]

def handle_leave_or_delete_room():
    msg = "Room deleted gracefully. Disconnected all clients." if state.is_host else "Left room."
    state.mode = "IDLE"
    state.is_connected = False
    state.is_host = False
    state.copy_logs = []
    state.connected_clients = {}
    
    gr.Info(msg)

    return [
        gr.update(visible=True),                      # landing_container
        gr.update(visible=False),                     # dashboard_container
    ]

# FIXED: Async Kick Handler prevents runtime thread errors
async def kick_client(client_id: str):
    cid = client_id.strip() if client_id else ""
    if cid and state.hub_manager:
        success = await state.hub_manager.kick_client(cid)
        if success:
            gr.Info(f"Kicked client ID '{cid}'.")
        else:
            gr.Warning(f"Client ID '{cid}' not found.")
    else:
        gr.Warning("Please enter a valid Client ID.")
    return get_clients_table_data()

# FIXED: Async Block Handler
async def block_client(client_id: str):
    cid = client_id.strip() if client_id else ""
    if cid and state.hub_manager:
        if cid in state.connected_clients:
            ip = state.connected_clients[cid]["ip"]
            state.blocked_clients.append(ip)
        state.blocked_clients.append(cid)
        await state.hub_manager.kick_client(cid)
        gr.Warning(f"Blocked client ID/IP '{cid}'.")
    else:
        gr.Warning("Please enter a valid Client ID.")
    return get_clients_table_data()

def get_clients_table_data():
    rows = []
    for cid, info in state.connected_clients.items():
        rows.append([cid, info["name"], info["ip"], info["join_time"]])
    return rows

# --- CUSTOM CSS FOR POLISHED LANDING UI ---
CUSTOM_CSS = """
.clipsync-card {
    max-width: 650px;
    margin: 30px auto;
    padding: 24px;
    border-radius: 12px;
    background: var(--background-fill-primary);
    box-shadow: 0 4px 20px rgba(0, 0, 0, 0.08);
}
.header-title {
    text-align: center;
    margin-bottom: 8px;
}
.sub-title {
    text-align: center;
    color: var(--body-text-color-subdued);
    font-size: 0.9em;
    margin-bottom: 20px;
}
"""

# --- GRADIO UI CONSTRUCTION ---
def build_dashboard():
    with gr.Blocks(title="ClipSync Engine", theme=gr.themes.Soft(), css=CUSTOM_CSS) as demo:
        
        # -------------------------------------------------------------
        # SECTION 1: LANDING MENU (POLISHED TABS + VISIBLE KEYS)
        # -------------------------------------------------------------
        with gr.Column(visible=True, elem_classes=["clipsync-card"]) as landing_container:
            gr.Markdown("# 📋 ClipSync", elem_classes=["header-title"])
            gr.Markdown("Seamless Real-time Cross-Device Clipboard Synchronization", elem_classes=["sub-title"])
            
            with gr.Tabs():
                # TAB 1: JOIN ROOM
                with gr.TabItem("🔗 Join Room"):
                    with gr.Group():
                        join_key = gr.Textbox(
                            label="Room Key", 
                            value=state.join_code, 
                            type="text",  # PLAIN TEXT VISIBLE INPUT
                            placeholder="Enter 4-digit or custom key...",
                            interactive=True
                        )
                        join_username = gr.Textbox(
                            label="Your Display Name", 
                            placeholder="e.g. Siddhant-Laptop"
                        )
                        
                        with gr.Accordion("Advanced Discovery Options", open=False):
                            join_discovery_mode = gr.Radio(
                                choices=["LAN (mDNS)", "WAN (Relay)", "Manual Direct IP"],
                                value="LAN (mDNS)",
                                label="Protocol Mode"
                            )
                        
                        btn_join = gr.Button("Connect to Room", variant="primary", size="lg")

                # TAB 2: HOST ROOM
                with gr.TabItem("🚀 Host Room"):
                    with gr.Group():
                        host_key = gr.Textbox(
                            label="Room Key", 
                            value=state.join_code, 
                            type="text",  # PLAIN TEXT VISIBLE INPUT
                            placeholder="Set security key for clients...",
                            interactive=True
                        )
                        host_room_name = gr.Textbox(
                            label="Room Name", 
                            placeholder="e.g. Workstation Sync"
                        )
                        host_name = gr.Textbox(
                            label="Host Display Name", 
                            placeholder="e.g. Siddhant-PC"
                        )
                        
                        with gr.Accordion("Advanced Broadcast Options", open=False):
                            host_visibility = gr.CheckboxGroup(
                                choices=["LAN (mDNS)", "WAN (Relay)"],
                                value=["LAN (mDNS)"],
                                label="Room Broadcast Visibility"
                            )
                        
                        btn_host = gr.Button("Create & Broadcast Room", variant="primary", size="lg")

        # -------------------------------------------------------------
        # SECTION 2: ACTIVE ROOM DASHBOARD
        # -------------------------------------------------------------
        with gr.Column(visible=False) as dashboard_container:
            
            # Top Header Bar
            with gr.Row(equal_height=True):
                ui_room_name = gr.Markdown("### Room Name")
                
                with gr.Accordion("Click to reveal Security Key", open=False):
                    ui_room_key_display = gr.Markdown("🔑 Security Key: ----")

            ui_host_name = gr.Markdown("Host: --")
            ui_room_status = gr.Markdown("🟢 **Status:** Connected and Syncing Active")

            gr.Markdown("---")
            
            # Clipboard Logs Component (5 Entries Display Target)
            gr.Markdown("### 📜 Copy Log History")
            copy_log_table = gr.Dataframe(
                headers=["Sr No", "User", "Copied Content", "Time / Date"],
                value=[],
                row_count=(5, "fixed"),
                col_count=(4, "fixed"),
                interactive=False
            )

            # Host Exclusive Control Panel
            with gr.Group(visible=False) as host_management_panel:
                gr.Markdown("---")
                gr.Markdown("### 🛡️ Host Management Console")
                
                clients_table = gr.Dataframe(
                    headers=["Client ID", "Username", "IP Address", "Joined At"],
                    value=[],
                    interactive=False,
                    label="Connected Clients"
                )
                
                with gr.Row():
                    target_client_id = gr.Textbox(label="Target Client ID", placeholder="Enter Client ID from table above...")
                    btn_kick = gr.Button("Kick Client", variant="stop")
                    btn_block = gr.Button("Block Client (Temporary)", variant="stop")
                
                gr.Markdown("---")
                btn_delete_room = gr.Button("🗑️ Delete Room & Disconnect All", variant="stop")

            # Non-Host Leave Button
            btn_leave_room = gr.Button("Leave Room", variant="secondary", visible=True)

        # -------------------------------------------------------------
        # PERIODIC UI REFRESH TIMER
        # -------------------------------------------------------------
        timer = gr.Timer(2)
        timer.tick(
            lambda: (
                state.copy_logs[:5],
                get_clients_table_data(),
                f"🟢 **Status:** {state.status}"
            ),
            outputs=[copy_log_table, clients_table, ui_room_status]
        )

        # -------------------------------------------------------------
        # EVENT BINDINGS
        # -------------------------------------------------------------
        
        # Join Action
        btn_join.click(
            fn=handle_join_room,
            inputs=[join_key, join_username, join_discovery_mode],
            outputs=[
                landing_container,
                dashboard_container,
                ui_room_name,
                ui_room_key_display,
                ui_host_name,
                host_management_panel
            ]
        )

        # Host Action
        btn_host.click(
            fn=handle_host_room,
            inputs=[host_key, host_room_name, host_name, host_visibility],
            outputs=[
                landing_container,
                dashboard_container,
                ui_room_name,
                ui_room_key_display,
                ui_host_name,
                host_management_panel
            ]
        )

        # Room Exit Handlers
        btn_delete_room.click(
            fn=handle_leave_or_delete_room,
            outputs=[landing_container, dashboard_container]
        )

        btn_leave_room.click(
            fn=handle_leave_or_delete_room,
            outputs=[landing_container, dashboard_container]
        )

        # Moderation Actions (Now async-bound)
        btn_kick.click(fn=kick_client, inputs=[target_client_id], outputs=[clients_table])
        btn_block.click(fn=block_client, inputs=[target_client_id], outputs=[clients_table])

    threading.Thread(
        target=demo.launch,
        kwargs={"server_port": state.ui_port, "prevent_thread_lock": True},
        daemon=True
    ).start()

# --- MAIN ENGINE ASYNC LOOP ---
async def main_engine(safe_cb: SafeClipboard, tray: QSystemTrayIcon):
    last_mode = "IDLE"
    while True:
        if state.mode != last_mode:
            logger.info(f"Transitioning mode: {last_mode} -> {state.mode}")
            if state.active_task:
                state.active_task.cancel()
                try: 
                    await state.active_task
                except asyncio.CancelledError: 
                    pass
                state.active_task = None

            if state.mode == "HOST":
                state.active_task = asyncio.create_task(run_as_host(safe_cb, tray))
            elif state.mode == "CLIENT":
                state.active_task = asyncio.create_task(run_as_client(safe_cb, tray))
            elif state.mode == "IDLE":
                state.status = "System Standby"
                tray.showMessage("ClipSync", "All active network pipelines stopped.", QSystemTrayIcon.Information, 1000)

            last_mode = state.mode
        await asyncio.sleep(0.5)

# --- ENTRY POINT ---
def main():
    q_app = QApplication(sys.argv)
    loop = QEventLoop(q_app)
    asyncio.set_event_loop(loop)

    safe_cb = SafeClipboard(q_app)
    state.ui_port = find_free_port(7860)

    icon = q_app.style().standardIcon(QStyle.StandardPixmap.SP_DriveNetIcon)
    tray = QSystemTrayIcon(icon, q_app)
    menu = QMenu()
    menu.addAction("Open Dashboard").triggered.connect(lambda: webbrowser.open(f"http://localhost:{state.ui_port}"))
    menu.addSeparator()
    menu.addAction("Exit").triggered.connect(q_app.quit)
    tray.setContextMenu(menu)
    tray.show()

    build_dashboard()

    with loop:
        loop.create_task(main_engine(safe_cb, tray))
        loop.run_forever()

if __name__ == "__main__":
    main()