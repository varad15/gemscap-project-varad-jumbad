import asyncio
import json
import logging
import websockets
from datetime import datetime
from sqlalchemy.orm import Session
from src.database import SessionLocal, Tick, init_db
from src.utils import setup_logger

logger = setup_logger("Ingestion")

BRIDGE_PORT = 8765


class BridgeServer:
    def __init__(self):
        self.buffer = []
        self.batch_size = 50  # Flush to DB every 50 ticks
        self.flush_interval = 0.5  # Or every 0.5 seconds
        self.last_flush = datetime.now()

    def _flush_buffer(self):
        """
        Writes buffered ticks to SQLite in a single transaction.
        """
        if not self.buffer:
            return

        session: Session = SessionLocal()
        try:
            session.bulk_save_objects(self.buffer)
            session.commit()
            self.buffer.clear()
            self.last_flush = datetime.now()
        except Exception as e:
            logger.error(f"Database write error: {e}")
            session.rollback()
        finally:
            session.close()

    async def handle_browser_connection(self, websocket):
        """
        Handles the WebSocket connection from the HTML file.
        Receives JSON trade data and saves to DB.
        """
        logger.info(f"🟢 Browser Bridge Connected on Port {BRIDGE_PORT}")

        try:
            async for message in websocket:
                try:
                    data = json.loads(message)

                    # Expected Payload from Binance Futures via HTML:
                    # {"e":"trade", "E": 123456789, "s": "BTCUSDT", "p": "50000.50", "q": "0.001", ...}

                    if 'e' in data and data['e'] == 'trade':
                        tick = Tick(
                            symbol=data['s'],
                            price=float(data['p']),
                            quantity=float(data['q']),
                            timestamp=datetime.fromtimestamp(data['E'] / 1000.0)
                        )
                        self.buffer.append(tick)

                        # Buffer Flush Logic
                        time_diff = (datetime.now() - self.last_flush).total_seconds()
                        if len(self.buffer) >= self.batch_size or time_diff >= self.flush_interval:
                            self._flush_buffer()

                except json.JSONDecodeError:
                    logger.warning("Received invalid JSON from bridge")
                except Exception as e:
                    logger.error(f"Error processing message: {e}")

        except websockets.exceptions.ConnectionClosed:
            logger.warning("🔴 Browser Bridge Disconnected.")
        finally:
            self._flush_buffer()  # Ensure remaining data is saved

    async def start_server(self):
        """
        Starts the asyncio WebSocket Server.
        """
        init_db()  # Ensure DB exists
        logger.info(f"🚀 Bridge Server waiting for connection at ws://localhost:{BRIDGE_PORT}")
        logger.info("👉 Please open 'index.html' and click START to begin data feed.")

        # Start server
        async with websockets.serve(self.handle_browser_connection, "localhost", BRIDGE_PORT):
            await asyncio.Future()  # Run forever


# Helper function to run ingestor in a separate thread for Streamlit
def run_ingestor_sync():
    server = BridgeServer()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(server.start_server())
    except KeyboardInterrupt:
        pass
    finally:
        loop.close()