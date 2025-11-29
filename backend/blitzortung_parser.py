import asyncio
import json
import zlib
import struct
from datetime import datetime
from playwright.async_api import async_playwright


# ---------------------------
# ZLIB DECOMPRESSION FUNCTION
# ---------------------------
def decompress_ws(data: bytes) -> str:
    try:
        return zlib.decompress(data, -zlib.MAX_WBITS).decode("utf-8", errors="replace")
    except Exception as e:
        print("[DECOMPRESS ERROR]", e)
        return None


# -------------------------------------------
# BLITZORTUNG CUSTOM LZW-STYLE STRING DECODER
# -------------------------------------------
def blitzortung_lzw_decode(encoded: str) -> str:
    if not encoded:
        return ""

    # Dictionary: int code -> string
    dictionary: dict[int, str] = {}

    chars = list(encoded)

    # First char is literal; seed output & previous value
    prev = chars[0]
    out = [prev]

    next_code = 256
    head = prev  # first char of previous decoded string

    for ch in chars[1:]:
        code = ord(ch)

        # Literal characters
        if code < 256:
            entry = ch
        else:
            # LZW "special case": if code not yet defined, use prev + head
            entry = dictionary.get(code, prev + head)

        out.append(entry)

        # First character of current entry
        head = entry[0]

        # Add new entry to dictionary: previous_string + first_char_of_current
        dictionary[next_code] = prev + head
        next_code += 1

        prev = entry

    return "".join(out)


# ---------------------------
# ENHANCED DECODING FUNCTIONS
# ---------------------------
def decode_binary_value(value):
    if not isinstance(value, str):
        return value

    # Check if string contains high-byte characters (indicating binary encoding)
    if not any(ord(c) > 127 for c in value):
        return value

    try:
        # Interpret as bytes 1:1
        byte_data = value.encode("latin-1")  # Preserve byte values exactly

        # Attempt to decode as various numeric types
        if len(byte_data) == 2:
            return struct.unpack(">H", byte_data)[0]  # 16-bit unsigned
        elif len(byte_data) == 4:
            return struct.unpack(">I", byte_data)[0]  # 32-bit unsigned

        # If we can't decode, return as hex string for inspection
        return byte_data.hex()
    except Exception:
        return value


def clean_lightning_data(data):
    if isinstance(data, dict):
        cleaned = {}
        for key, value in data.items():
            cleaned[key] = clean_lightning_data(value)
        return cleaned

    elif isinstance(data, list):
        return [clean_lightning_data(item) for item in data]

    elif isinstance(data, str):
        return decode_binary_value(data)

    else:
        return data


# -------------------------------
# MAIN PARSER / ENCODING DETECTOR
# -------------------------------
def parse_lightning_message(message_str: str) -> dict:
    # 1) First attempt: plain JSON (no obfuscation)
    try:
        raw_data = json.loads(message_str)
        cleaned_data = clean_lightning_data(raw_data)
        return {
            "success": True,
            "raw": raw_data,
            "decoded": cleaned_data,
            "encoding": "plain_json",
        }
    except json.JSONDecodeError as first_error:
        # 2) Second attempt: assume Blitzortung LZW-style obfuscation
        try:
            decoded_str = blitzortung_lzw_decode(message_str)
            raw_data = json.loads(decoded_str)
            cleaned_data = clean_lightning_data(raw_data)
            return {
                "success": True,
                "raw": raw_data,
                "decoded": cleaned_data,
                # This is the "what kind of encryption is this?" label:
                "encoding": "blitzortung_lzw_string_obfuscation",
            }
        except Exception as second_error:
            # Could not decode even after LZW attempt
            return {
                "success": False,
                "error": (
                    "JSON Parse Error. Direct parse failed with: "
                    f"{first_error}; LZW-style decode failed with: {second_error}"
                ),
                "raw_message": message_str[:200],
            }

    except Exception as e:
        # Any other unexpected error
        return {
            "success": False,
            "error": f"Unknown Error: {e}",
            "raw_message": message_str[:200],
        }


# ---------------
# COLLECTOR CLASS
# ---------------
class BlitzortungRawCollector:
    def __init__(self, json_filename="lightning_messages_decoded.json"):
        self.json_filename = json_filename
        self.message_count = 0
        self.decode_stats = {
            "total": 0,
            "decoded_success": 0,
            "decode_failed": 0,
        }

        with open(self.json_filename, "w", encoding="utf-8") as f:
            f.write("[\n")

        print(f"Created output file: {self.json_filename}\n")

    def save_message(self, message):
        """Save and decode WebSocket message"""
        try:
            raw_bytes = None
            message_str = ""

            # Extract bytes/text from Playwright's WS frame structure
            if isinstance(message, dict):
                # Binary frame (opcode 2)
                if "opcode" in message and message["opcode"] == 2:
                    raw_bytes = message.get("data", None)

                # Text frame (opcode 1)
                if "opcode" in message and message["opcode"] == 1:
                    msg_text = message.get("data", "")
                    message_str = str(msg_text)

                # Alternative field seen in some runtimes
                if "payloadData" in message:
                    possible = message["payloadData"]
                    if isinstance(possible, bytes):
                        raw_bytes = possible
                    elif isinstance(possible, str):
                        message_str = possible

            # Decompress if we have bytes (zlib / permessage-deflate)
            if raw_bytes is not None:
                message_str = decompress_ws(raw_bytes)
                if message_str is None:
                    return

            elif isinstance(message, str):
                message_str = message
            else:
                # Fallback string conversion
                message_str = str(message)

            # Parse and decode the message
            self.decode_stats["total"] += 1
            parsed_result = parse_lightning_message(message_str)

            if parsed_result["success"]:
                self.decode_stats["decoded_success"] += 1
            else:
                self.decode_stats["decode_failed"] += 1

            # Create entry with both raw and decoded data
            entry = {
                "index": self.message_count,
                "timestamp": datetime.now().isoformat(),
                "raw_message": message_str,
                "decoded": parsed_result,
            }

            # Write to file
            with open(self.json_filename, "a", encoding="utf-8") as f:
                if self.message_count > 0:
                    f.write(",\n")
                json.dump(entry, f, ensure_ascii=False, indent=2)

            self.message_count += 1

            # Progress reporting
            if self.message_count % 10 == 0 or self.message_count <= 5:
                enc = parsed_result.get("encoding", "n/a") if parsed_result.get("success") else "failed"
                print(
                    f"Saved message #{self.message_count} | "
                    f"Decoded: {self.decode_stats['decoded_success']} | "
                    f"Failed: {self.decode_stats['decode_failed']} | "
                    f"Encoding: {enc}"
                )

        except Exception as e:
            print(f"[ERROR] Failed to save message: {e}")

    def finalize_file(self):
        """Finalize the output JSON file"""
        try:
            with open(self.json_filename, "a", encoding="utf-8") as f:
                f.write("\n]")

            print(f"\n{'=' * 70}")
            print(f"Finalized file with {self.message_count} entries.")
            print("Decode Statistics:")
            print(f"  Total messages: {self.decode_stats['total']}")
            print(f"  Successfully decoded: {self.decode_stats['decoded_success']}")
            print(f"  Decode failures: {self.decode_stats['decode_failed']}")
            print(f"{'=' * 70}\n")
        except Exception as e:
            print(f"[ERROR] Failed to finalize file: {e}")

    async def collect_from_browser(self, duration_seconds=120):
        """Collect lightning data from Blitzortung website"""
        print("=" * 70)
        print("Blitzortung Lightning Data Collector with Decoder")
        print("=" * 70)

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=False)
            context = await browser.new_context()
            page = await context.new_page()

            start_time = asyncio.get_event_loop().time()
            ws_connected = False

            async def on_websocket(ws):
                nonlocal ws_connected
                ws_connected = True
                print(f"\n[WS CONNECTED] {ws.url}\n")

                async def on_frame(frame):
                    self.save_message(frame)

                ws.on("framereceived", lambda frame: asyncio.create_task(on_frame(frame)))

            page.on("websocket", on_websocket)

            print("Opening Blitzortung...")
            await page.goto(
                "https://www.blitzortung.org/en/live_lightning_maps.php",
                wait_until="domcontentloaded",
                timeout=60000,
            )

            print("Waiting 8 seconds for WebSocket to appear...")
            await asyncio.sleep(8)

            if not ws_connected:
                print("WARNING: No WebSocket detected!")
            else:
                print("WebSocket active. Starting collection...\n")

            # Main collection loop
            try:
                while True:
                    await asyncio.sleep(1)
                    elapsed = asyncio.get_event_loop().time() - start_time
                    if elapsed >= duration_seconds:
                        break
            except KeyboardInterrupt:
                print("\nCollection stopped by user.\n")

            self.finalize_file()
            await browser.close()

        print("\nCOLLECTION COMPLETE\n")


# ----------------
# MAIN ENTRY POINT
# ----------------
async def main():
    collector = BlitzortungRawCollector(
        json_filename="lightning_messages_decoded.json"
    )
    await collector.collect_from_browser(duration_seconds=120)


if __name__ == "__main__":
    asyncio.run(main())
