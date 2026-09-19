import asyncio
import json
import logging
import websockets

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ClipSyncRelay")

# Map of room_code -> hub connection info
ROOMS = {}

async def handler(websocket, path):
    try:
        parts = path.strip("/").split("/")
        if len(parts) < 2:
            return

        action, room_code = parts[0], parts[1]

        # 1. Hub registers itself on the public WAN map
        if action == "register":
            ROOMS[room_code] = websocket.remote_address
            logger.info(f"Registered Room '{room_code}' from {websocket.remote_address}")
            await websocket.send(json.dumps({"status": "registered"}))
            await websocket.wait_closed()

        # 2. Client queries for Hub endpoint
        elif action == "lookup":
            endpoint = ROOMS.get(room_code)
            if endpoint:
                await websocket.send(json.dumps({"status": "success", "endpoint": endpoint}))
            else:
                await websocket.send(json.dumps({"status": "not_found"}))

    except Exception as e:
        logger.error(f"Relay error: {e}")
    finally:
        # Cleanup disconnected host rooms
        for room, addr in list(ROOMS.items()):
            if addr == websocket.remote_address:
                del ROOMS[room]

async def main():
    async with websockets.serve(handler, "0.0.0.0", 8765):
        logger.info("Relay signaling server running on port 8765...")
        await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())