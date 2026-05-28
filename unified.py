import asyncio, json, uuid, sys, os, base64, struct, threading, socket, time
import gradio as gr
from PySide6.QtWidgets import QApplication, QSystemTrayIcon, QMenu, QStyle
from PySide6.QtGui import QClipboard, QImage, QIcon
from PySide6.QtCore import QByteArray, QBuffer, QIODevice, Qt, QUrl, QMimeData
from qasync import QEventLoop
from zeroconf.asyncio import AsyncZeroconf
from zeroconf import ServiceInfo
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
import webbrowser
import discovery

# --- CONFIG PERSISTENCE ---
CONFIG_FILE = "sync_config.json"
my_port = 5555
DOWNLOAD_DIR = os.path.abspath("ClipSync_Downloads")

if not os.path.exists(DOWNLOAD_DIR):
    os.makedirs(DOWNLOAD_DIR)

def load_config():
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, "r") as f:
                return json.load(f)
        except: return {"join_code": "1234"}
    return {"join_code": "1234"}

def save_config(join_code):
    with open(CONFIG_FILE, "w") as f:
        json.dump({"join_code": join_code}, f)

def find_free_port(start_port=7860, max_attempts=100):
    for port in range(start_port, start_port + max_attempts):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                s.bind(("localhost", port))
                return port
            except socket.error:
                continue
    return start_port 

# --- CRYPTO ---
STATIC_SALT = b'clipboard_sync_v1_salt' 

def generate_key(join_code: str):
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

    def encrypt(self, text: str) -> str:
        return self.fernet.encrypt(text.encode()).decode()

    def decrypt(self, encrypted_text: str) -> str:
        try: return self.fernet.decrypt(encrypted_text.encode()).decode()
        except: return "[Error: Decryption Failed]"

# --- UNIFIED NETWORK PROTOCOL ---
async def send_msg(writer, data_dict):
    payload = json.dumps(data_dict).encode()
    header = struct.pack('!I', len(payload))
    writer.write(header + payload)
    await writer.drain()

async def recv_msg(reader):
    try:
        header = await reader.readexactly(4)
        length = struct.unpack('!I', header)[0]
        data = await reader.readexactly(length)
        return json.loads(data.decode())
    except: return None

class ConnectionManager:
    def __init__(self):
        self.clients = {} 

    async def register(self, writer):
        peer_name = f"{writer.get_extra_info('peername')[0]}:{writer.get_extra_info('peername')[1]}"
        self.clients[writer] = peer_name
        return peer_name

    async def unregister(self, writer):
        if writer in self.clients:
            del self.clients[writer]
        try:
            writer.close()
            await writer.wait_closed()
        except: pass

    async def kick_peer_by_name(self, peer_name):
        target_writer = None
        for w, name in self.clients.items():
            if name == peer_name:
                target_writer = w
                break
        if target_writer:
            try:
                await send_msg(target_writer, {"type": "kicked"})
            except: pass
            await self.unregister(target_writer)

    async def broadcast(self, msg_dict, sender_writer):
        payload = json.dumps(msg_dict).encode()
        header = struct.pack('!I', len(payload))
        packet = header + payload
        tasks = [self._safe_send(c, packet) for c in list(self.clients.keys()) if c != sender_writer]
        if tasks: await asyncio.gather(*tasks)

    async def _safe_send(self, writer, packet):
        try:
            writer.write(packet)
            await writer.drain()
        except: await self.unregister(writer)

    def get_connected_list(self):
        return list(self.clients.values())

# --- SYSTEM STATE ---
class AppState:
    def __init__(self):
        conf = load_config()
        self.mode = "IDLE"
        self.join_code = conf.get("join_code", "1234")
        self.status = "System Standby"
        self.history = []
        self.hub_manager = ConnectionManager()
        self.active_task = None
        self.crypto = CryptoManager(self.join_code)
        self.last_sync_id = None 
        self.processing_remote = False
        self.last_synced_time = "Never"
        self.ui_port = 7860
        self.aiozc = None

state = AppState()

# --- CLIENT SYNC ENGINE ---
async def clipboard_watcher(app, writer):
    clipboard = app.clipboard()
    last_text = ""
    last_img_hash = 0
    last_urls = []
    
    while state.mode == "CLIENT":
        if state.processing_remote:
            await asyncio.sleep(0.2)
            continue

        mime = clipboard.mimeData()
        
        # 1. FILE SYNC INTEGRATION
        if mime.hasUrls():
            current_urls = [url.toLocalFile() for url in mime.urls() if url.isLocalFile()]
            if current_urls and current_urls != last_urls:
                last_urls = current_urls
                target_file = current_urls[0]
                
                if os.path.exists(target_file) and os.path.isfile(target_file):
                    filename = os.path.basename(target_file)
                    file_size = os.path.getsize(target_file)
                    
                    if file_size < 150 * 1024 * 1024:
                        state.status = f"Streaming file: {filename}..."
                        try:
                            with open(target_file, "rb") as f:
                                raw_bytes = f.read()
                            b64_file = base64.b64encode(raw_bytes).decode()
                            
                            state.last_sync_id = str(uuid.uuid4())
                            ts = time.strftime("%H:%M:%S")
                            await send_msg(writer, {
                                "type": "clip", "id": state.last_sync_id,
                                "content_type": "file", "filename": filename,
                                "content": state.crypto.encrypt(b64_file)
                            })
                            state.history.append([ts, f"Sent File: {filename}"])
                            state.status = "Sync Active"
                        except Exception as e:
                            state.status = f"File Read Failed: {str(e)}"
                await asyncio.sleep(1.0)

        # 2. TEXT SYNC
        elif mime.hasText() and mime.text() != last_text:
            last_text = mime.text()
            state.last_sync_id = str(uuid.uuid4()) 
            ts = time.strftime("%H:%M:%S")
            await send_msg(writer, {
                "type": "clip", "id": state.last_sync_id, 
                "content_type": "text", "content": state.crypto.encrypt(last_text)
            })
            state.history.append([ts, "Sent Text"])

        # 3. IMAGE SYNC
        elif mime.hasImage():
            img = clipboard.image()
            current_hash = hash(img.cacheKey())
            
            if not img.isNull() and current_hash != last_img_hash:
                last_img_hash = current_hash 
                state.status = "Syncing Image..." 
                ba = QByteArray()
                buffer = QBuffer(ba)
                buffer.open(QIODevice.WriteOnly)
                img.save(buffer, "PNG")
                
                b64_data = base64.b64encode(ba.data()).decode()
                state.last_sync_id = str(uuid.uuid4())
                ts = time.strftime("%H:%M:%S")
                
                await send_msg(writer, {
                    "type": "clip", "id": state.last_sync_id, 
                    "content_type": "image", "content": state.crypto.encrypt(b64_data)
                })
                
                state.history.append([ts, "Sent Image"])
                state.status = "Sync Active"
                await asyncio.sleep(1.0) 
        
        await asyncio.sleep(0.5)

async def clipboard_listener(reader, app, tray):
    clipboard = app.clipboard()
    while state.mode == "CLIENT":
        msg = await recv_msg(reader)
        if not msg: continue
        
        if msg.get("type") == "kicked":
            state.status = "You were kicked by the host."
            state.mode = "IDLE"
            tray.showMessage("ClipSync Alert", "Kicked from the room by the host.", QSystemTrayIcon.Warning, 2000)
            break

        # HARD REJECTION: If packet identifier matches our own last broadcasted message, drop it instantly
        if msg.get("id") == state.last_sync_id and state.last_sync_id is not None: 
            continue 

        decrypted = state.crypto.decrypt(msg.get("content"))
        if "[Error:" in decrypted: continue

        # Raise processing flag to let watcher back off entirely
        state.processing_remote = True
        c_type = msg.get("content_type", "text")
        ts = time.strftime("%H:%M:%S")
        state.last_synced_time = ts
        
        # Save payload ID as our active sync tracking state to ensure watcher ignores the update loop
        state.last_sync_id = msg.get("id")

        if c_type == "text":
            clipboard.setText(decrypted)
        elif c_type == "image":
            img_bytes = base64.b64decode(decrypted)
            image = QImage.fromData(img_bytes)
            if not image.isNull(): 
                clipboard.setImage(image, QClipboard.Clipboard)
        elif c_type == "file":
            filename = msg.get("filename", "synced_file")
            file_bytes = base64.b64decode(decrypted)
            target_path = os.path.join(DOWNLOAD_DIR, filename)
            with open(target_path, "wb") as f:
                f.write(file_bytes)
            
            url = QUrl.fromLocalFile(target_path)
            new_mime = QMimeData()
            new_mime.setUrls([url])
            clipboard.setMimeData(new_mime)

        state.history.append([ts, f"Received {c_type.upper()}"])
        tray.showMessage("ClipSync Success", f"Synced {c_type} [At {ts}]", QSystemTrayIcon.Information, 1500)
        
        # Give the operating system clipboard framework ample breathing room to settle down
        await asyncio.sleep(0.8)
        state.processing_remote = False

# --- ROLE EXECUTORS ---
async def run_as_host():
    local_ip = socket.gethostbyname(socket.gethostname())
    state.status = f"Hosting Hub: {local_ip}"
    state.aiozc = AsyncZeroconf()
    
    unique_server_name = f"hub-{uuid.uuid4().hex[:4]}.local."
    info = ServiceInfo("_clip-sync._tcp.local.", f"Hub-{state.join_code}._clip-sync._tcp.local.",
                       addresses=[socket.inet_aton(local_ip)], port=my_port, server=unique_server_name)
    await state.aiozc.async_register_service(info) 
    
    async def handle_peer(reader, writer):
        try:
            auth = await recv_msg(reader)
            if auth and auth.get("code") == state.join_code: 
                await send_msg(writer, {"type": "auth_ok"})
                await state.hub_manager.register(writer)
                while state.mode == "HOST":
                    data = await recv_msg(reader)
                    if not data: break
                    await state.hub_manager.broadcast(data, writer)
        except: pass
        finally:
            await state.hub_manager.unregister(writer)

    server = await asyncio.start_server(handle_peer, '0.0.0.0', my_port)
    
    try:
        async with server: 
            await server.serve_forever()
    except asyncio.CancelledError:
        pass
    finally:
        if state.aiozc:
            await state.aiozc.async_unregister_all_services()
            await state.aiozc.close()
            state.aiozc = None

async def run_as_client(app, tray):
    state.status = "Initializing Discovery..."
    state.aiozc = AsyncZeroconf()
    
    try:
        while state.mode == "CLIENT":
            hub_info = await discovery.discover(state.join_code, state.aiozc)
            if hub_info:
                try:
                    state.status = "Connecting to Hub..."
                    reader, writer = await asyncio.open_connection(*hub_info)
                    await send_msg(writer, {"type": "join", "code": state.join_code})
                    resp = await recv_msg(reader)
                    
                    if resp and resp.get("type") == "auth_ok":
                        state.status = "Sync Active"
                        await asyncio.gather(
                            clipboard_watcher(app, writer), 
                            clipboard_listener(reader, app, tray)
                        )
                except Exception:
                    state.status = "Connection lost. Retrying..."
                    await asyncio.sleep(2)
            else:
                state.status = "Still searching for Hub..."
                await asyncio.sleep(1)
    finally:
        if state.aiozc:
            await state.aiozc.async_close()
            state.aiozc = None

# --- UI & RUNNER ---
def build_dashboard():
    with gr.Blocks(theme="soft") as demo:
        gr.Markdown("# 🛰️ ClipSync Unified Terminal")
        
        with gr.Row():
            room_code = gr.Textbox(label="Room Code", value=state.join_code)
            current_mode = gr.Label(label="Current Role", value=state.mode)
            last_sync_disp = gr.Label(label="Last Synced Time", value=state.last_synced_time)
        
        with gr.Row():
            host_btn = gr.Button("🚀 Host Room")
            join_btn = gr.Button("🔗 Join Room")
            stop_btn = gr.Button("🛑 Go Idle / Disconnect")
        
        status_disp = gr.Textbox(label="System Status", interactive=False)
        
        with gr.Row():
            with gr.Column(scale=2):
                gr.Markdown("### 📜 Activity Log")
                history_table = gr.Dataframe(headers=["Timestamp", "Event Data"], value=state.history)
                clear_hist_btn = gr.Button("🧹 Clear History Log")
            
            with gr.Column(scale=1):
                gr.Markdown("### 🥾 Room Management")
                peer_list = gr.Dropdown(label="Connected Users (Peers)", choices=[])
                refresh_peers_btn = gr.Button("🔄 Refresh Peer List")
                kick_btn = gr.Button("💥 Kick Selected Peer", variant="stop")

        def start_mode(mode, code):
            state.join_code = code
            save_config(code)
            state.crypto = CryptoManager(code)
            state.mode = mode
            return f"Transitioning into {mode}...", mode

        def go_idle():
            state.mode = "IDLE"
            return "Stopping services...", "IDLE"

        def clear_logs():
            state.history = []
            return []

        def get_peers():
            return gr.Dropdown(choices=state.hub_manager.get_connected_list())

        async def kick_peer_logic(peer_name):
            if peer_name:
                await state.hub_manager.kick_peer_by_name(peer_name)
                return f"Evicted {peer_name} successfully", gr.Dropdown(choices=state.hub_manager.get_connected_list(), value=None)
            return "No peer selected", gr.Dropdown(choices=state.hub_manager.get_connected_list())

        host_btn.click(start_mode, [gr.State("HOST"), room_code], [status_disp, current_mode])
        join_btn.click(start_mode, [gr.State("CLIENT"), room_code], [status_disp, current_mode])
        stop_btn.click(go_idle, None, [status_disp, current_mode])
        clear_logs_btn = clear_hist_btn.click(clear_logs, None, history_table)
        
        refresh_peers_btn.click(get_peers, None, peer_list)
        kick_btn.click(kick_peer_logic, peer_list, [status_disp, peer_list])

        timer = gr.Timer(2)
        timer.tick(
            lambda: (state.status, state.mode, state.history[-12:], state.last_synced_time), 
            outputs=[status_disp, current_mode, history_table, last_sync_disp]
        )

    threading.Thread(
        target=demo.launch, 
        kwargs={"server_port": state.ui_port, "prevent_thread_lock": True}, 
        daemon=True
    ).start()

async def main_engine(app, tray):
    last_processed_mode = "IDLE"
    
    while True:
        if state.mode != last_processed_mode:
            state.status = f"Transitioning to {state.mode}..."
            
            if state.active_task:
                state.active_task.cancel()
                try:
                    await state.active_task
                except asyncio.CancelledError:
                    pass
                state.active_task = None
                
            if last_processed_mode == "HOST" and state.aiozc:
                try:
                    await state.aiozc.async_unregister_all_services()
                    await state.aiozc.close()
                    state.aiozc = None
                except Exception as e:
                    print(f"Teardown Error: {e}")

            if state.mode == "HOST":
                state.active_task = asyncio.create_task(run_as_host())
                state.status = "Initializing Hub..."
            elif state.mode == "CLIENT":
                state.active_task = asyncio.create_task(run_as_client(app, tray))
                state.status = "Searching for Hub..."
            elif state.mode == "IDLE":
                state.status = "System Standby (Idle)"
                tray.setToolTip(f"ClipSync [Idle] - Last Synced: {state.last_synced_time}")
                tray.showMessage("ClipSync Status", "All active processing pipelines stopped.", QSystemTrayIcon.Information, 1000)

            last_processed_mode = state.mode

        if state.mode != "IDLE":
            tray.setToolTip(f"ClipSync [{state.mode}] - Last Synced: {state.last_synced_time}")

        await asyncio.sleep(1) 

def setup_tray(app):
    icon = app.style().standardIcon(QStyle.StandardPixmap.SP_DriveNetIcon)
    tray = QSystemTrayIcon(icon, app)
    menu = QMenu()
    
    dash_action = menu.addAction("Open Dashboard")
    dash_action.triggered.connect(lambda: webbrowser.open(f"http://localhost:{state.ui_port}"))
    
    menu.addSeparator()
    exit_action = menu.addAction("Quit App")
    exit_action.triggered.connect(app.quit)
    
    tray.setContextMenu(menu)
    tray.show()
    return tray

def main():
    q_app = QApplication(sys.argv)
    loop = QEventLoop(q_app)
    asyncio.set_event_loop(loop)
    
    state.ui_port = find_free_port(7860)
    q_app.setQuitOnLastWindowClosed(False)
    
    build_dashboard()
    tray = setup_tray(q_app)
    
    with loop:
        loop.create_task(main_engine(q_app, tray))
        loop.run_forever()

if __name__ == "__main__":
    main()