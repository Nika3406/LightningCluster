import os
import sys
import json
import math
import logging
import threading
import subprocess
import time
import signal
import shutil
from pathlib import Path
from flask import Flask, jsonify, send_from_directory

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("lightning-orchestrator")

REPO_ROOT = Path(__file__).resolve().parent.parent
FRONTEND_DIR = REPO_ROOT / "frontend"
FRONTEND_BUILD = FRONTEND_DIR / "build"
FRONTEND_PUBLIC = FRONTEND_DIR / "public"
COLLECTOR_SCRIPT = Path(__file__).resolve().parent / "blitzortung_parser.py"
COLLECTOR_JSON = REPO_ROOT / "backend" / "lightning_messages_decoded.json"
# The collector writes lightning_messages_decoded.json in its cwd; ensure the path used matches the collector's file
# The collector's default is "lightning_messages_decoded.json" in its working directory; we'll run it with cwd=backend/

# Supervisor control
shutdown_event = threading.Event()
collector_proc_lock = threading.Lock()
collector_proc = None  # subprocess.Popen

app = Flask(
    __name__,
    static_folder=str(FRONTEND_BUILD / "static"),
    static_url_path="/static",
)


# ---------------------------
# Algorithms (same as before)
# ---------------------------
class CMPSC463Algorithms:
    def haversine_distance(self, lat1, lon1, lat2, lon2):
        R = 6371.0  # Earth radius in km
        dlat = math.radians(lat2 - lat1)
        dlon = math.radians(lon2 - lon1)
        a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
        return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    def bfs_connected_components(self, strikes, max_distance_km=50):
        if not strikes:
            return []
        visited = set()
        components = []
        for i in range(len(strikes)):
            if i not in visited:
                component = []
                queue = [i]
                visited.add(i)
                while queue:
                    current_idx = queue.pop(0)
                    component.append(strikes[current_idx])
                    current = strikes[current_idx]
                    for j in range(len(strikes)):
                        if j not in visited:
                            dist = self.haversine_distance(current['lat'], current['lon'], strikes[j]['lat'], strikes[j]['lon'])
                            if dist <= max_distance_km:
                                visited.add(j)
                                queue.append(j)
                if len(component) > 1:
                    components.append(component)
        return components

    def _calculate_local_density(self, strike, all_strikes, radius_km=50):
        count = 0
        for other in all_strikes:
            if self.haversine_distance(strike['lat'], strike['lon'], other['lat'], other['lon']) <= radius_km:
                count += 1
        return count

    def greedy_hotspot_selection(self, strikes, k=10):
        if len(strikes) <= k:
            return strikes
        weighted = []
        for s in strikes:
            w = self._calculate_local_density(s, strikes)
            weighted.append((w, s))
        weighted.sort(reverse=True, key=lambda x: x[0])
        return [s for _, s in weighted[:k]]

    def prim_mst_clusters(self, strikes, max_edge_km=100):
        if len(strikes) <= 1:
            return []
        n = len(strikes)
        dist_matrix = [[0.0] * n for _ in range(n)]
        for i in range(n):
            for j in range(i + 1, n):
                d = self.haversine_distance(strikes[i]['lat'], strikes[i]['lon'], strikes[j]['lat'], strikes[j]['lon'])
                dist_matrix[i][j] = dist_matrix[j][i] = d
        visited = set([0])
        mst_edges = []
        while len(visited) < n:
            min_edge = (float('inf'), -1, -1)
            for u in list(visited):
                for v in range(n):
                    if v not in visited and dist_matrix[u][v] > 0 and dist_matrix[u][v] < min_edge[0]:
                        min_edge = (dist_matrix[u][v], u, v)
            if min_edge[1] != -1:
                mst_edges.append(min_edge)
                visited.add(min_edge[2])
            else:
                break
        # remove long edges
        short_edges = [e for e in mst_edges if e[0] <= max_edge_km]
        # adjacency
        adj = {i: set() for i in range(n)}
        for dist, u, v in short_edges:
            adj[u].add(v)
            adj[v].add(u)
        clusters = []
        seen = set()
        for i in range(n):
            if i in seen:
                continue
            stack = [i]
            comp = []
            seen.add(i)
            while stack:
                node = stack.pop()
                comp.append(strikes[node])
                for nb in adj.get(node, ()):
                    if nb not in seen:
                        seen.add(nb)
                        stack.append(nb)
            if len(comp) > 1:
                avg_lat = sum(s['lat'] for s in comp) / len(comp)
                avg_lon = sum(s['lon'] for s in comp) / len(comp)
                clusters.append({'center': {'lat': avg_lat, 'lon': avg_lon}, 'count': len(comp), 'strikes': comp})
        return clusters


# ---------------------------
# Collector supervisor
# ---------------------------
def start_collector_supervisor():
    """
    Starts a background thread that ensures the collector subprocess is running.
    If it exits unexpectedly, the supervisor restarts it after a short delay.
    """
    def supervisor():
        global collector_proc
        backend_dir = Path(__file__).resolve().parent
        cmd = [sys.executable, str(COLLECTOR_SCRIPT.name)]
        env = os.environ.copy()
        # Pass headless env variable into collector
        env_headless = os.getenv("PLAYWRIGHT_HEADLESS", "1")
        env["PLAYWRIGHT_HEADLESS"] = env_headless

        while not shutdown_event.is_set():
            with collector_proc_lock:
                if collector_proc is None or collector_proc.poll() is not None:
                    logger.info("Starting collector subprocess...")
                    try:
                        # Run collector with cwd=backend_dir so its output file will be created there
                        collector_proc = subprocess.Popen(
                            cmd,
                            cwd=str(backend_dir),
                            env=env,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT,
                            text=True,
                        )
                        logger.info(f"Collector started (pid={collector_proc.pid})")
                    except Exception as e:
                        logger.exception("Failed to start collector:")
                        collector_proc = None

            # tail the collector stdout while it's running
            if collector_proc:
                try:
                    while collector_proc.poll() is None and not shutdown_event.is_set():
                        # read a line with timeout-like behavior
                        line = collector_proc.stdout.readline()
                        if line:
                            print(f"[collector] {line.rstrip()}")
                        else:
                            # Avoid busy loop if no output
                            time.sleep(0.2)
                    # if we get here, collector exited or shutdown_event set
                    rc = collector_proc.poll()
                    logger.warning(f"Collector process exited (rc={rc})")
                except Exception:
                    logger.exception("Error while monitoring collector stdout")
            # if not shutting down - restart after brief sleep
            if not shutdown_event.is_set():
                logger.info("Restarting collector in 2s...")
                time.sleep(2)

        # Shutdown requested: ensure collector is terminated
        with collector_proc_lock:
            if collector_proc and collector_proc.poll() is None:
                logger.info("Terminating collector subprocess...")
                collector_proc.terminate()
                try:
                    collector_proc.wait(timeout=5)
                except Exception:
                    try:
                        collector_proc.kill()
                    except Exception:
                        pass
        logger.info("Collector supervisor exiting.")

    t = threading.Thread(target=supervisor, daemon=True)
    t.start()
    return t


# ---------------------------
# Frontend build helper
# ---------------------------
def ensure_frontend_built():
    """
    Ensure FRONTEND_BUILD exists. If not, attempt to run `npm ci && npm run build` inside frontend/.
    SKIP_FRONTEND_BUILD=1 can be set to skip this step (if you run build manually).
    """
    skip = os.getenv("SKIP_FRONTEND_BUILD", "0").lower() in ("1", "true", "yes")
    if FRONTEND_BUILD.exists() and FRONTEND_BUILD.is_dir():
        logger.info("Frontend build directory exists; skipping build.")
        return True

    if skip:
        logger.warning("SKIP_FRONTEND_BUILD set but build directory not found. The frontend may not serve correctly.")
        return False

    # Check for npm
    npm = shutil.which("npm")
    if not npm:
        logger.error("npm not found in PATH. Install Node/npm or run frontend build manually.")
        return False

    logger.info("Building frontend (this may take a minute)...")
    try:
        # Run npm ci (use ci if package-lock exists) else npm install
        if (FRONTEND_DIR / "package-lock.json").exists():
            run_cmd = [npm, "ci"]
        else:
            run_cmd = [npm, "install"]
        subprocess.run(run_cmd, cwd=str(FRONTEND_DIR), check=True)

        # Run npm run build
        subprocess.run([npm, "run", "build"], cwd=str(FRONTEND_DIR), check=True)
        logger.info("Frontend build complete.")
        return True
    except subprocess.CalledProcessError as e:
        logger.exception("Frontend build failed.")
        return False


# ---------------------------
# Utilities: read strikes
# ---------------------------
def read_strikes_from_collector(filename=COLLECTOR_JSON, limit=500):
    """
    Robust parser for the collector JSON produced by blitzortung_parser.BlitzortungRawCollector.
    Returns list of {'lat': float, 'lon': float, 'intensity': float}
    """
    if not filename.exists():
        return []
    try:
        raw = filename.read_text(encoding="utf-8").strip()
        if not raw:
            return []
        # tolerate missing closing bracket
        if raw.startswith("[") and not raw.endswith("]"):
            raw = raw + "]"
        data = json.loads(raw)
    except Exception:
        logger.exception("Failed to parse collector JSON")
        return []

    strikes = []
    for entry in data[-limit:]:
        parsed = entry.get("decoded") or {}
        candidate = None
        if parsed.get("success"):
            rawp = parsed.get("raw") or {}
            decp = parsed.get("decoded") or {}
            for container in (rawp, decp):
                if isinstance(container, dict):
                    if "data" in container and isinstance(container["data"], dict):
                        maybe = container["data"]
                        if "lat" in maybe and "lon" in maybe:
                            candidate = maybe
                            break
                    if "lat" in container and "lon" in container:
                        candidate = container
                        break
        if candidate is None:
            rm = entry.get("raw_message", "")
            try:
                pm = json.loads(rm)
                if isinstance(pm, dict):
                    if "data" in pm and isinstance(pm["data"], dict) and "lat" in pm["data"]:
                        candidate = pm["data"]
                    elif "lat" in pm and "lon" in pm:
                        candidate = pm
            except Exception:
                pass
        if candidate and "lat" in candidate and "lon" in candidate:
            try:
                strikes.append({
                    "lat": float(candidate["lat"]),
                    "lon": float(candidate["lon"]),
                    "intensity": float(candidate.get("mcg", candidate.get("intensity", 1)))
                })
            except Exception:
                continue
    return strikes


# ---------------------------
# Flask endpoints
# ---------------------------
@app.route("/api/lightning")
def api_lightning():
    strikes = read_strikes_from_collector()
    alg = CMPSC463Algorithms()
    bfs_clusters = alg.bfs_connected_components(strikes)
    hotspots = alg.greedy_hotspot_selection(strikes)
    mst_clusters = alg.prim_mst_clusters(strikes)

    return jsonify({
        "strikes": strikes,
        "clusters": mst_clusters,
        "hotspots": hotspots,
        "stats": {
            "total_strikes": len(strikes),
            "bfs_clusters": len(bfs_clusters),
            "mst_clusters": len(mst_clusters)
        }
    })


@app.route("/", defaults={"path": ""})
@app.route("/<path:path>")
def serve_frontend(path):
    # If build directory exists, serve files from build (create-react-app output)
    if FRONTEND_BUILD.exists():
        if path and (FRONTEND_BUILD / path).exists():
            return send_from_directory(str(FRONTEND_BUILD), path)
        # serve index.html for SPA
        if (FRONTEND_BUILD / "index.html").exists():
            return send_from_directory(str(FRONTEND_BUILD), "index.html")
    # Fallback: try public (dev/static files)
    if FRONTEND_PUBLIC.exists():
        if path and (FRONTEND_PUBLIC / path).exists():
            return send_from_directory(str(FRONTEND_PUBLIC), path)
        if (FRONTEND_PUBLIC / "index.html").exists():
            return send_from_directory(str(FRONTEND_PUBLIC), "index.html")
    return "Frontend not built. Run npm install && npm run build in frontend/ or set SKIP_FRONTEND_BUILD=1", 500


# ---------------------------
# Shutdown helpers
# ---------------------------
def shutdown_handler(signum, frame):
    logger.info(f"Received signal {signum}; shutting down...")
    shutdown_event.set()
    # Terminate collector subprocess if running
    with collector_proc_lock:
        global collector_proc
        if collector_proc and collector_proc.poll() is None:
            try:
                collector_proc.terminate()
            except Exception:
                pass


signal.signal(signal.SIGINT, shutdown_handler)
signal.signal(signal.SIGTERM, shutdown_handler)


# ---------------------------
# Main orchestration
# ---------------------------
def main():
    # 1) Ensure frontend build exists (unless SKIP_FRONTEND_BUILD=1)
    built = ensure_frontend_built()
    if not built:
        logger.warning("Frontend build not available. The server may return an error page.")

    # 2) Start collector supervisor thread
    sup_thread = start_collector_supervisor()

    # 3) Start Flask
    host = os.getenv("LIGHTNING_HOST", "0.0.0.0")
    port = int(os.getenv("LIGHTNING_PORT", "8080"))
    debug = os.getenv("FLASK_DEBUG", "0").lower() in ("1", "true", "yes")
    logger.info(f"Starting Flask server on {host}:{port} (debug={debug})")
    try:
        # Use Werkzeug's reloader disabled to avoid double-launching subprocesses
        app.run(host=host, port=port, debug=debug, use_reloader=False)
    finally:
        # shutdown_event ensures supervisor will exit and terminate collector
        shutdown_event.set()
        logger.info("Waiting for supervisor thread to finish...")
        sup_thread.join(timeout=5)
        logger.info("Exiting orchestrator.")


if __name__ == "__main__":
    main()