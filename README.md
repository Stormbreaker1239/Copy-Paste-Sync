# ClipSync

**ClipSync** is a lightweight, production-grade cross-device clipboard synchronization application built in Python. It enables real-time, encrypted text clipboard sharing across local networks and over the internet via a custom relay protocol, featuring a system tray interface and a Gradio dashboard.

---

## Features

* **Real-Time Synchronization**: Instantly sync text clipboards across devices on local networks (LAN) or over the Internet.
* **End-to-End Encryption**: Encrypted network engine ensuring secure clipboard transfer between synced nodes.
* **System Tray Integration**: Runs quietly in the background with quick access controls from the system tray.
* **Gradio Dashboard**: Interactive webui to monitor connected peers, view synchronization status, and manage configuration settings.
* **Global Search & Note Tools**: Integrated clipboard search, lightweight spellchecking, dynamic tab management, and checklist support.
* **Robust Architecture**: Built with thread-safe queue handling, binary protocol framing, structured log rotation, and full error recovery.

---

## Tech Stack

* **Language**: Python 3.10+
* **GUI Framework**: Gradio (Web Interface) / PyQt
* **Networking**: Custom TCP/UDP binary protocol & encrypted socket relay
* **Security**: Cryptography (Symmetric Encryption)

---

## Repository Structure

```text
clipsync/
├── app.py              # Main application entry point & Tray UI
├── core/
│   ├── sync_engine.py  # Binary protocol framing & network engine
│   ├── crypto.py       # Encryption and decryption routines
│   └── logger.py       # Rotational file logging system
├── ui/
│   └── dashboard.py    # Gradio monitoring & control panel
├── config.json         # Local settings and relay configurations
├── requirements.txt    # Python dependencies
└── README.md           # Project documentation

```

---

## Getting Started

### Prerequisites

* Python 3.10 or higher
* `pip` package manager

### Installation

1. **Clone the repository**:
```bash
git clone https://github.com/your-username/clipsync.git
cd clipsync

```


2. **Create and activate a virtual environment**:
```bash
python -m venv venv
# On Windows:
venv\Scripts\activate
# On macOS/Linux:
source venv/bin/activate

```


3. **Install dependencies**:
```bash
pip install -r requirements.txt

```



---

## Usage

1. **Start ClipSync**:
```bash
python app.py

```


2. **Access Dashboard**:
Open your browser and navigate to `[http://127.0.0.1:7860](http://127.0.0.1:7860)` to view active sync connections and clipboard activity logs.
3. **Background Operation**:
ClipSync automatically minimizes to your system tray. Right-click the tray icon to pause sync, switch relay channels, or exit.

---

