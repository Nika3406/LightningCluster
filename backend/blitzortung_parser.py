import asyncio
import json
import zlib
import struct
from datetime import datetime
from playwright.async_api import async_playwright


def blitzortung_lzw_decode(encoded: str) -> str:
    if not encoded:
        return ""
    dictionary = {}
    chars = list(encoded)
    prev = chars[0]
    out = [prev]
    next_code = 256
    head = prev

    for ch in chars[1:]:
        code = ord(ch)
        if code < 256:
            entry = ch
        else:
            entry = dictionary.get(code, prev + head)
        out.append(entry)
        head = entry[0]
        dictionary[next_code] = prev + head
        next_code += 1
        prev = entry
    return "".join(out)


def parse_lightning_message(message_str: str) -> dict:
    try:
        raw_data = json.loads(message_str)
        return {"success": True, "raw": raw_data, "encoding": "plain_json"}
    except json.JSONDecodeError:
        try:
            decoded_str = blitzortung_lzw_decode(message_str)
            raw_data = json.loads(decoded_str)
            return {"success": True, "raw": raw_data, "encoding": "blitzortung_lzw"}
        except Exception:
            return {"success": False, "error": "Decode failed"}


class BlitzortungRawCollector:
    def __init__(self, json_filename="lightning_data.json"):
        self.json_filename = json_filename
        self.message_count = 0
        with open(self.json_filename, "w", encoding="utf-8") as f:
            f.write("[\n")

    def save_message(self, message):
        try:
            if isinstance(message, dict) and message.get("opcode") == 1:
                message_str = str(message.get("data", ""))
                parsed_result = parse_lightning_message(message_str)

                if parsed_result["success"]:
                    entry = {
                        "index": self.message_count,
                        "timestamp": datetime.now().isoformat(),
                        "data": parsed_result["raw"]
                    }

                    with open(self.json_filename, "a", encoding="utf-8") as f:
                        if self.message_count > 0:
                            f.write(",\n")
                        json.dump(entry, f)

                    self.message_count += 1
                    if self.message_count % 10 == 0:
                        print(f"Saved {self.message_count} messages")

        except Exception as e:
            print(f"Save error: {e}")

    async def collect_from_browser(self, duration_seconds=120):
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()

            async def on_websocket(ws):
                async def on_frame(frame):
                    self.save_message(frame)

                ws.on("framereceived", lambda frame: asyncio.create_task(on_frame(frame)))

            page.on("websocket", on_websocket)
            await page.goto("https://www.blitzortung.org/en/live_lightning_maps.php", timeout=60000)

            await asyncio.sleep(duration_seconds)
            await browser.close()