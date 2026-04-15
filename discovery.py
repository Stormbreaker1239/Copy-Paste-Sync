import socket
import asyncio
from zeroconf.asyncio import AsyncZeroconf, AsyncServiceBrowser

class DiscoveryListener:
    def __init__(self, target_code):
        self.found_hub = asyncio.Event()
        self.hub_address = None
        self.target_code = target_code

    def update_service(self, zc, type_, name):
        pass

    def remove_service(self, zc, type_, name):
        print(f"Service {name} removed")

    def add_service(self, zc, type_, name):
        # We use asyncio.ensure_future because add_service is called from a thread
        asyncio.ensure_future(self.async_add_service(zc, type_, name))

    async def async_add_service(self, zc, type_, name):
        info = await zc.async_get_service_info(type_, name)
        if info:
            # Now we use the target_code passed during initialization
            if f"Hub-{self.target_code}" in name: 
                address = socket.inet_ntoa(info.addresses[0])
                self.hub_address = (address, info.port)
                self.found_hub.set()

async def discover(room_code, aiozc):
    listener = DiscoveryListener(room_code)
    
    print(f"Searching for Hub-{room_code}...")
    # We use the aiozc instance passed from the caller instead of creating a new one
    browser = AsyncServiceBrowser(aiozc.zeroconf, "_clip-sync._tcp.local.", listener)
    
    try:
        await asyncio.wait_for(listener.found_hub.wait(), timeout=10.0)
        return listener.hub_address
    except asyncio.TimeoutError:
        return None
    finally:
        # We DON'T close aiozc here anymore because it's managed by the caller
        await browser.async_cancel()

if __name__ == "__main__":
    try:
        hub_info = asyncio.run(discover())
        if hub_info:
            print(f"Discovery successful. Ready to connect to {hub_info}")
    except KeyboardInterrupt:
        pass