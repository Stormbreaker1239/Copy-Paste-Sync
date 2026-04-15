# TODO: Add timestamps to history log, kicking people out of the room, and a "clear history" button on the dashboard. Also add a "last synced" timestamp on the tray notifications.
import asyncio, json, uuid, sys, os, base64, struct, threading, socket, time
import gradio as gr
from PySide6.QtWidgets import QApplication, QSystemTrayIcon, QMenu
from PySide6.QtGui import QClipboard, QImage, QIcon
from PySide6.QtCore import QByteArray, QBuffer, QIODevice, Qt
from PySide6.QtWidgets import QApplication, QSystemTrayIcon, QMenu, QStyle
from qasync import QEventLoop
from zeroconf.asyncio import AsyncZeroconf
from zeroconf import ServiceInfo
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
import webbrowser
import discovery

# --- CONFIG PERSISTENCE [cite: 19, 20] ---
CONFIG_FILE = "sync_config.json"
my_port = 5555

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
    """Checks for the first available port starting from start_port."""
    for port in range(start_port, start_port + max_attempts):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                s.bind(("localhost", port))
                return port
            except socket.error:
                continue
    return start_port # Fallback
# --- CRYPTO [cite: 41, 42] ---
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

# --- UNIFIED NETWORK PROTOCOL [cite: 3, 39, 40] ---
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
        self.clients = set()

    async def register(self, writer):
        self.clients.add(writer)

    async def unregister(self, writer):
        if writer in self.clients:
            self.clients.remove(writer)
        try:
            writer.close()
            await writer.wait_closed()
        except: pass

    async def broadcast(self, msg_dict, sender_writer):
        payload = json.dumps(msg_dict).encode()
        header = struct.pack('!I', len(payload))
        packet = header + payload
        tasks = [self._safe_send(c, packet) for c in list(self.clients) if c != sender_writer]
        if tasks: await asyncio.gather(*tasks)

    async def _safe_send(self, writer, packet):
        try:
            writer.write(packet)
            await writer.drain()
        except: await self.unregister(writer)

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
        self.last_sync_id = None # Echo Protection [cite: 21, 29]
        self.processing_remote = False

state = AppState()
state.ui_port = 7860
# --- CLIENT SYNC ENGINE [cite: 22-26, 30-32] ---
async def clipboard_watcher(app, writer):
    clipboard = app.clipboard()
    last_text = ""
    last_img_hash = 0
    
    while state.mode == "CLIENT":
        if state.processing_remote:
            await asyncio.sleep(0.5)
            continue

        mime = clipboard.mimeData()
        # TEXT SYNC [cite: 22]
        if mime.hasText() and mime.text() != last_text:
            last_text = mime.text()
            state.last_sync_id = str(uuid.uuid4()) # 
            await send_msg(writer, {
                "type": "clip", "id": state.last_sync_id, 
                "content_type": "text", "content": state.crypto.encrypt(last_text)
            })
            state.history.append([time.strftime("%H:%M:%S"), "Sent Text"])

        # IMAGE SYNC [cite: 23-26]
        elif mime.hasImage():
            img = clipboard.image()
            current_hash = hash(img.cacheKey())
            
            if not img.isNull() and current_hash != last_img_hash:
                # IMPORTANT: Lock immediately to prevent re-entry
                last_img_hash = current_hash 
                
                state.status = "Syncing Image..." # Feedback
                ba = QByteArray()
                buffer = QBuffer(ba); buffer.open(QIODevice.WriteOnly)
                img.save(buffer, "PNG")
                
                b64_data = base64.b64encode(ba.data()).decode()
                state.last_sync_id = str(uuid.uuid4())
                
                await send_msg(writer, {
                    "type": "clip", "id": state.last_sync_id, 
                    "content_type": "image", "content": state.crypto.encrypt(b64_data)
                })
                
                state.history.append([time.strftime("%H:%M:%S"), "Sent Image"])
                state.status = "Sync Active"
                # Give the system a breath after a heavy image
                await asyncio.sleep(1.0) 
        
        await asyncio.sleep(1.0)

async def clipboard_listener(reader, app, tray):
    clipboard = app.clipboard()
    while state.mode == "CLIENT":
        msg = await recv_msg(reader)
        # ECHO PROTECTION: If ID matches our last sent, ignore it 
        if not msg or msg.get("id") == state.last_sync_id: 
            continue 

        decrypted = state.crypto.decrypt(msg.get("content"))
        if "[Error:" in decrypted: continue

        state.processing_remote = True
        c_type = msg.get("content_type", "text")
        
        if c_type == "text":
            clipboard.setText(decrypted)
        elif c_type == "image":
            img_bytes = base64.b64decode(decrypted)
            image = QImage.fromData(img_bytes)
            if not image.isNull(): 
                clipboard.setImage(image,QClipboard.Clipboard)

        state.history.append([time.strftime("%H:%M:%S"), f"Received {c_type}"])
        # TRAY NOTIFICATION 
        tray.showMessage("ClipSync", f"Synced {c_type} from LAN", QSystemTrayIcon.Information, 1500)
        await asyncio.sleep(0.5)
        state.processing_remote = False

# --- ROLE EXECUTORS ---
async def run_as_host():
    local_ip = socket.gethostbyname(socket.gethostname())
    state.status = f"Hosting Hub: {local_ip}"
    state.aiozc = AsyncZeroconf()
    info = ServiceInfo("_clip-sync._tcp.local.", f"Hub-{state.join_code}._clip-sync._tcp.local.",
                       addresses=[socket.inet_aton(local_ip)], port=my_port, server="hub.local.")
    await state.aiozc.async_register_service(info) 
    
    async def handle_peer(reader, writer):
        auth = await recv_msg(reader)
        if auth and auth.get("code") == state.join_code: 
            await send_msg(writer, {"type": "auth_ok"})
            await state.hub_manager.register(writer)
            while True:
                data = await recv_msg(reader)
                if not data: break
                await state.hub_manager.broadcast(data, writer)
        writer.close()

    server = await asyncio.start_server(handle_peer, '0.0.0.0', my_port)
    async with server: await server.serve_forever()

async def run_as_client(app, tray):
    state.status = "Initializing Discovery..."
    
    # Create the instance ONCE here
    state.aiozc = AsyncZeroconf()
    
    try:
        while state.mode == "CLIENT":
            # Pass the join_code and the instance to the discover function
            hub_info = await discovery.discover(state.join_code, state.aiozc)
            
            if hub_info:
                try:
                    state.status = "Connecting to Hub..."
                    reader, writer = await asyncio.open_connection(*hub_info)
                    
                    # Handshake and Sync...
                    await send_msg(writer, {"type": "join", "code": state.join_code})
                    resp = await recv_msg(reader)
                    
                    if resp and resp.get("type") == "auth_ok":
                        state.status = "Sync Active"
                        await asyncio.gather(
                            clipboard_watcher(app, writer), 
                            clipboard_listener(reader, app, tray)
                        )
                except Exception as e:
                    state.status = "Connection lost. Retrying..."
                    await asyncio.sleep(2)
            else:
                state.status = "Still searching for Hub..."
                await asyncio.sleep(1)
                
    finally:
        # Cleanup the instance when the task is canceled or the loop ends
        if state.aiozc:
            await state.aiozc.async_close()
            state.aiozc = None

# --- UI & RUNNER ---
def build_dashboard():
    # 1. Removed theme from here
    with gr.Blocks() as demo:
        gr.Markdown("# 🛰️ ClipSync Unified")
        with gr.Row():
            room_code = gr.Textbox(label="Room Code", value=state.join_code)
            current_mode = gr.Label(label="Role", value=state.mode)
        
        host_btn = gr.Button("🚀 Host")
        join_btn = gr.Button("🔗 Join")
        status_disp = gr.Textbox(label="Status", interactive=False)
        history_table = gr.Dataframe(headers=["Time", "Event"])

        def start_mode(mode, code):
            state.join_code = code
            save_config(code)
            state.crypto = CryptoManager(code)
            state.mode = mode
            if state.active_task: 
                state.active_task.cancel()
            state.active_task = None
            return f"Starting {mode}...", mode

        host_btn.click(start_mode, [gr.State("HOST"), room_code], [status_disp, current_mode])
        join_btn.click(start_mode, [gr.State("CLIENT"), room_code], [status_disp, current_mode])

        # 2. Use Timer instead of .load(every=)
        timer = gr.Timer(2)
        timer.tick(
            lambda: (state.status, state.mode, state.history[-10:]), 
            outputs=[status_disp, current_mode, history_table]
        )

    # 3. Moved theme to launch
    threading.Thread(
    target=demo.launch, 
    kwargs={
      "server_port": state.ui_port, 
      "prevent_thread_lock": True, 
      "theme": "soft" # Theme goes here now
    }, 
    daemon=True
  ).start()

    print(f"--- Dashboard logic initialized on port {state.ui_port} ---")

async def main_engine(app, tray):
    # We keep track of the current mode being executed to detect changes
    last_processed_mode = "IDLE"
    
    while True:
        # Check if the user changed the mode via the Gradio Dashboard
        if state.mode != last_processed_mode:
            state.status = f"Transitioning to {state.mode}..."
            
            # 1. KILL EXISTING TASK: If something is running, shut it down
            if state.active_task:
                state.active_task.cancel()
                try:
                    await state.active_task
                except asyncio.CancelledError:
                    pass
                state.active_task = None
                
            # 2. CLEANUP: If we were hosting, we need to unregister Zeroconf
            # This prevents "Service Name Already in Use" errors on restart
            if last_processed_mode == "HOST":
                try:
                    # Logic to unregister Zeroconf if you stored the object in state
                    if hasattr(state, 'aiozc') and state.aiozc:
                        await state.aiozc.async_unregister_all_services()
                        await state.aiozc.close()
                        state.aiozc = None
                except Exception as e:
                    print(f"Cleanup error: {e}")

            # 3. START NEW TASK
            if state.mode == "HOST":
                state.active_task = asyncio.create_task(run_as_host())
                state.status = "Initializing Hub..."
            elif state.mode == "CLIENT":
                state.active_task = asyncio.create_task(run_as_client(app, tray))
                state.status = "Searching for Hub..."
            elif state.mode == "IDLE":
                state.status = "System Standby (Idle)"
                tray.showMessage("ClipSync", "All sync tasks stopped.", QSystemTrayIcon.Information, 1000)

            last_processed_mode = state.mode

        await asyncio.sleep(1) # Polling interval for state changes

def setup_tray(app):
    # Using SP_DriveNetIcon as you had it, looks more "sync-like"
    icon = app.style().standardIcon(QStyle.StandardPixmap.SP_DriveNetIcon)
    tray = QSystemTrayIcon(icon, app)
    menu = QMenu()
    
    dash_action = menu.addAction("Open Dashboard")
    # Better way to open URLs
    dash_action.triggered.connect(lambda: webbrowser.open("http://localhost:" + str(state.ui_port)))
    
    menu.addSeparator()
    exit_action = menu.addAction("Exit")
    exit_action.triggered.connect(app.quit)
    
    tray.setContextMenu(menu)
    tray.show()
    return tray

def main():
    q_app = QApplication(sys.argv)
    
    # 1. Setup the Event Loop
    loop = QEventLoop(q_app)
    asyncio.set_event_loop(loop)
    # --- NEW STEP: Determine Port First ---
    state.ui_port = find_free_port(7860)
    # 2. Prevent the app from closing when the browser tab is shut
    q_app.setQuitOnLastWindowClosed(False)
    
    # 3. Use the helper function to build the tray (it already has the Exit button!)
    
    # 4. Start the Gradio Web Server
    build_dashboard()
    tray = setup_tray(q_app)
    
    # 5. Run the background engine
    with loop:
        loop.create_task(main_engine(q_app, tray))
        loop.run_forever()

if __name__ == "__main__":
    main()