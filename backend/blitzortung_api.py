import asyncio
import json
import aiohttp
from aiohttp import web
import os
import math
from blitzortung_parser import BlitzortungRawCollector
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CMPSC463Algorithms:
    def __init__(self):
        pass

    def haversine_distance(self, lat1, lon1, lat2, lon2):
        """Calculate distance between two points on Earth (for BFS/DFS)"""
        R = 6371  # Earth radius in km
        dlat = math.radians(lat2 - lat1)
        dlon = math.radians(lon2 - lon1)
        a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(
            dlon / 2) ** 2
        return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    def bfs_connected_components(self, strikes, max_distance_km=50):
        """BFS for finding connected lightning regions (from page 7)"""
        if not strikes:
            return []

        visited = set()
        components = []

        for i in range(len(strikes)):
            if i not in visited:
                # Start BFS from this strike
                component = []
                queue = [i]
                visited.add(i)

                while queue:
                    current_idx = queue.pop(0)
                    component.append(strikes[current_idx])
                    current_strike = strikes[current_idx]

                    # Find neighbors within max_distance
                    for j in range(len(strikes)):
                        if j not in visited:
                            dist = self.haversine_distance(
                                current_strike['lat'], current_strike['lon'],
                                strikes[j]['lat'], strikes[j]['lon']
                            )
                            if dist <= max_distance_km:
                                visited.add(j)
                                queue.append(j)

                if len(component) > 1:  # Only keep meaningful clusters
                    components.append(component)

        return components

    def greedy_hotspot_selection(self, strikes, k=10):
        """Greedy algorithm for selecting top hotspots (from page 17-18)"""
        if len(strikes) <= k:
            return strikes

        # Calculate "weight" as local density
        weighted_strikes = []
        for strike in strikes:
            density = self._calculate_local_density(strike, strikes)
            weighted_strikes.append((density, strike))

        # Greedy selection: sort by density and take top k
        weighted_strikes.sort(reverse=True)
        return [strike for _, strike in weighted_strikes[:k]]

    def _calculate_local_density(self, strike, all_strikes, radius_km=50):
        """Calculate how many strikes are nearby (local density)"""
        count = 0
        for other in all_strikes:
            dist = self.haversine_distance(
                strike['lat'], strike['lon'],
                other['lat'], other['lon']
            )
            if dist <= radius_km:
                count += 1
        return count

    def prim_mst_clusters(self, strikes, max_edge_km=100):
        """Prim's MST algorithm for clustering (from page 20-21)"""
        if len(strikes) <= 1:
            return []

        # Build distance matrix
        n = len(strikes)
        dist_matrix = [[0] * n for _ in range(n)]
        for i in range(n):
            for j in range(i + 1, n):
                dist = self.haversine_distance(
                    strikes[i]['lat'], strikes[i]['lon'],
                    strikes[j]['lat'], strikes[j]['lon']
                )
                dist_matrix[i][j] = dist_matrix[j][i] = dist

        # Prim's MST algorithm
        visited = set()
        mst_edges = []
        visited.add(0)

        while len(visited) < n:
            min_edge = (float('inf'), -1, -1)
            for i in visited:
                for j in range(n):
                    if j not in visited and dist_matrix[i][j] > 0:
                        if dist_matrix[i][j] < min_edge[0]:
                            min_edge = (dist_matrix[i][j], i, j)

            if min_edge[1] != -1:
                mst_edges.append(min_edge)
                visited.add(min_edge[2])

        # Remove long edges to form clusters
        clusters = self._form_clusters_from_mst(strikes, mst_edges, max_edge_km)
        return clusters

    def _form_clusters_from_mst(self, strikes, mst_edges, max_edge_km):
        """Form clusters by removing long MST edges"""
        # Sort edges by distance and remove long ones
        mst_edges.sort()
        short_edges = [edge for edge in mst_edges if edge[0] <= max_edge_km]

        # Find connected components
        clusters = []
        visited = set()

        for i in range(len(strikes)):
            if i not in visited:
                cluster = []
                stack = [i]
                visited.add(i)

                while stack:
                    node = stack.pop()
                    cluster.append(strikes[node])

                    # Find neighbors in short edges
                    for dist, u, v in short_edges:
                        if u == node and v not in visited:
                            visited.add(v)
                            stack.append(v)
                        elif v == node and u not in visited:
                            visited.add(u)
                            stack.append(u)

                if len(cluster) > 1:
                    clusters.append({
                        'center': self._calculate_center(cluster),
                        'count': len(cluster),
                        'strikes': cluster
                    })

        return clusters

    def _calculate_center(self, strikes):
        """Calculate center point of a cluster"""
        avg_lat = sum(s['lat'] for s in strikes) / len(strikes)
        avg_lon = sum(s['lon'] for s in strikes) / len(strikes)
        return {'lat': avg_lat, 'lon': avg_lon}


class LightningAPI:
    def __init__(self):
        self.collector = BlitzortungRawCollector()
        self.algorithms = CMPSC463Algorithms()

    def get_strikes(self):
        try:
            if not os.path.exists("lightning_messages_decoded.json"):
                return []

            with open("lightning_messages_decoded.json", 'r', encoding='utf-8') as f:
                content = f.read().strip()
                if content.startswith('[') and not content.endswith(']'):
                    content += ']'
                data = json.loads(content)

            strikes = []
            for entry in data[-500:]:
                if entry.get('data') and 'lat' in entry['data'] and 'lon' in entry['data']:
                    strike = entry['data']
                    strikes.append({
                        'lat': strike['lat'],
                        'lon': strike['lon'],
                        'intensity': strike.get('mcg', 1)
                    })
            return strikes
        except Exception as e:
            logger.exception("Error reading strikes:")
            return []

    async def get_lightning_data(self, request):
        strikes = self.get_strikes()

        # Use CMPSC 463 algorithms
        bfs_clusters = self.algorithms.bfs_connected_components(strikes)
        hotspots = self.algorithms.greedy_hotspot_selection(strikes)
        mst_clusters = self.algorithms.prim_mst_clusters(strikes)

        return web.json_response({
            'strikes': strikes,
            'clusters': mst_clusters,  # Using MST clusters as main heatmap
            'hotspots': hotspots,
            'stats': {
                'total_strikes': len(strikes),
                'bfs_clusters': len(bfs_clusters),
                'mst_clusters': len(mst_clusters)
            }
        })


async def start_server():
    api = LightningAPI()

    # Run collector in background and log exceptions
    async def run_collector():
        try:
            await api.collector.collect_from_browser(duration_seconds=3600)
        except Exception as e:
            logger.exception("Collector failed:")

    asyncio.create_task(run_collector())

    app = web.Application()

    # Add API routes first
    app.router.add_get('/api/lightning', api.get_lightning_data)

    # CORS middleware (use aiohttp's middleware decorator)
    @web.middleware
    async def cors_middleware(request, handler):
        # Handle preflight
        if request.method == 'OPTIONS':
            resp = web.Response(status=200)
            resp.headers['Access-Control-Allow-Origin'] = '*'
            resp.headers['Access-Control-Allow-Methods'] = 'GET,POST,OPTIONS'
            resp.headers['Access-Control-Allow-Headers'] = 'Content-Type,Authorization'
            return resp

        response = await handler(request)
        response.headers['Access-Control-Allow-Origin'] = '*'
        response.headers['Access-Control-Allow-Methods'] = 'GET,POST,OPTIONS'
        response.headers['Access-Control-Allow-Headers'] = 'Content-Type,Authorization'
        return response

    app.middlewares.append(cors_middleware)

    # Serve frontend static files. Point to the public folder where index.html lives.
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'frontend', 'public'))
    if os.path.isdir(base_dir):
        logger.info(f"Serving frontend static files from {base_dir}")
        # serve static files for everything under '/'
        app.router.add_static('/', path=base_dir, show_index=True)
    else:
        logger.warning(f"Frontend public folder not found at {base_dir}. Static files won't be served.")

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, 'localhost', 8080)
    await site.start()
    print("Server running: http://localhost:8080")
    await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(start_server())