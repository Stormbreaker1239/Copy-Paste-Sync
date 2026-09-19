import socket
import asyncio
import logging
import json
import websockets
from zeroconf.asyncio import AsyncZeroconf, AsyncServiceBrowser

logger = logging.getLogger("ClipSync.Discovery")

class DiscoveryListener:
    def __init__(self, target_code: str):
        self.found_hub = asyncio.Event()
        self.hub_address = None
        self.target_code = target_code

    def update_service(self, zc, type_, name):
        pass

    def remove_service(self, zc, type_, name):
        logger.debug(f"Service removed: {name}")

    def add_service(self, zc, type_, name):
        asyncio.create_task(self.async_add_service(zc, type_, name))

    async def async_add_service(self, zc, type_, name):
        try:
            info = await zc.async_get_service_info(type_, name)
            if info and f"Hub-{self.target_code}" in name:
                address = socket.inet_ntoa(info.addresses[0])
                self.hub_address = (address, info.port)
                logger.info(f"Local mDNS discovery successful: {self.hub_address}")
                self.found_hub.set()
        except Exception as e:
            logger.error(f"Error resolving mDNS service {name}: {e}")

async def discover_lan(room_code: str, aiozc: AsyncZeroconf, timeout: float = 3.0):
    """Searches local network via mDNS."""
    listener = DiscoveryListener(room_code)
    browser = AsyncServiceBrowser(aiozc.zeroconf, "_clip-sync._tcp.local.", listener)
    try:
        await asyncio.wait_for(listener.found_hub.wait(), timeout=timeout)
        return listener.hub_address
    except asyncio.TimeoutError:
        return None
    finally:
        await browser.async_cancel()

async def discover_wan(room_code: str, relay_server_url: str, timeout: float = 4.0):
    """Fallback discovery over Internet via a Signaling/Relay Server."""
    try:
        async with websockets.connect(f"{relay_server_url}/lookup/{room_code}", timeout=timeout) as ws:
            response = await ws.recv()
            data = json.loads(response)
            if data.get("status") == "success":
                logger.info(f"Internet WAN discovery successful: {data['endpoint']}")
                return tuple(data['endpoint']) # returns (host, port) or relay session ID
    except Exception as e:
        logger.debug(f"WAN discovery fallback skipped or failed: {e}")
    return None

async def discover(room_code: str, aiozc: AsyncZeroconf, relay_url: str = None):
    """Hybrid Local/WAN Discovery pipeline."""
    # Step 1: Rapid LAN Check (3 Seconds)
    lan_result = await discover_lan(room_code, aiozc, timeout=3.0)
    if lan_result:
        return {"mode": "DIRECT", "endpoint": lan_result}

    # Step 2: WAN Relay Fallback (If configured)
    if relay_url:
        wan_result = await discover_wan(room_code, relay_url)
        if wan_result:
            return {"mode": "RELAY", "endpoint": wan_result}

    return None