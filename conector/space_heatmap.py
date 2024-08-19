import requests
from requests.auth import HTTPBasicAuth
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import cv2
import numpy as np
from datetime import datetime, timedelta
from io import BytesIO
import logging

logger = logging.getLogger(__name__)

# URLs and credentials
stores = {
    "OML01-Omnia GuimarãesShopping": ["http://93.108.96.96:21001/"],
    "OML02-Omnia Fórum Almada": ["http://188.37.190.134:2201/"],
    "OML03-Omnia Norteshopping": ["http://188.37.124.33:21002/"],
    "ONL01-Only UBBO Amadora": ["http://93.108.245.76:21002/", "http://93.108.245.76:21003/"]
}

username = "admin"
password = "grnl.2024"

def generate_heatmap(ip, start_time, end_time, sub_type):
    try:
        snapshot_url = f"{ip}snapshot.cgi"
        heatmap_url = f"{ip}dataloader.cgi?dw=spaceheatmap&sub_type={sub_type}&time_start={start_time}&time_end={end_time}"

        logger.info(f"Fetching heatmap data from URL: {heatmap_url}")

        response = requests.get(heatmap_url, auth=HTTPBasicAuth(username, password), timeout=30)
        if response.status_code != 200:
            raise ValueError(f"Failed to fetch heatmap data. Status code: {response.status_code}")

        data = response.json()
        if not data or 'data' not in data:
            raise ValueError("No data available for the selected period or 'data' key is missing")

        df = pd.DataFrame(data['data'])
        map_w, map_h = data.get('map_w', 192), data.get('map_h', 192)  # Default values if not present

        snapshot_img = capture_snapshot(snapshot_url)
        if snapshot_img:
            overlayed_image = overlay_heatmap_on_snapshot(snapshot_img, df, map_w, map_h)
            return overlayed_image
        else:
            raise ValueError("Failed to capture snapshot")

    except Exception as e:
        logger.error(f"Error in generate_heatmap: {e}")
        return None

def capture_snapshot(snapshot_url):
    try:
        response = requests.get(snapshot_url, auth=HTTPBasicAuth(username, password), stream=True, timeout=30)
        if response.status_code == 200:
            img = BytesIO()
            for chunk in response.iter_content(chunk_size=8192):
                img.write(chunk)
            img.seek(0)
            logger.info("Snapshot captured successfully.")
            return img
        else:
            logger.error(f"Failed to capture the snapshot. Status code: {response.status_code}")
            return None
    except requests.exceptions.RequestException as e:
        logger.error(f"Error trying to capture the snapshot: {e}")
        return None

def overlay_heatmap_on_snapshot(snapshot_img, heatmap_data, map_w, map_h, x_shift=350, y_shift=-100):
    try:
        snapshot_img = cv2.imdecode(np.frombuffer(snapshot_img.read(), np.uint8), cv2.IMREAD_COLOR)
        snapshot_img_rgb = cv2.cvtColor(snapshot_img, cv2.COLOR_BGR2RGB)
        img_height, img_width, _ = snapshot_img.shape

        if heatmap_data is not None and not heatmap_data.empty:
            heatmap_pivot = heatmap_data.pivot_table(index='y', columns='x', values='value', fill_value=0)
            heatmap_fixed = np.zeros((map_h, map_w))
            for (x, y), value in np.ndenumerate(heatmap_pivot.values):
                if x < map_h and y < map_w:
                    heatmap_fixed[x, y] = value

            heatmap_resized = cv2.resize(heatmap_fixed, (img_width, img_height), interpolation=cv2.INTER_CUBIC)
            img_center_x, img_center_y = img_width // 2, img_height // 2
            heatmap_center_x, heatmap_center_y = heatmap_resized.shape[1] // 2, heatmap_resized.shape[0] // 2

            fig, ax = plt.subplots(figsize=(10, 10))
            cmap = create_transparent_to_jet_cmap()
            ax.imshow(snapshot_img_rgb, extent=[0, img_width, 0, img_height])
            norm = plt.Normalize(vmin=heatmap_resized.min(), vmax=heatmap_resized.max())
            heatmap = ax.imshow(heatmap_resized, cmap=cmap, norm=norm, interpolation='nearest',
                                extent=[img_center_x - heatmap_center_x + x_shift, img_center_x + heatmap_center_x + x_shift,
                                        img_center_y - heatmap_center_y + y_shift, img_center_y + heatmap_center_y + y_shift],
                                alpha=0.7)

            cbar_ax = fig.add_axes([0.125, 0.05, 0.75, 0.02])
            cbar = fig.colorbar(heatmap, cax=cbar_ax, orientation='horizontal')
            cbar.ax.tick_params(labelsize=0, colors='none')
        else:
            fig, ax = plt.subplots(figsize=(10, 10))
            ax.imshow(snapshot_img_rgb, extent=[0, img_width, 0, img_height])
            ax.axis('off')
            fig.text(0.5, 0.1, "Sem dados para o período escolhido", ha='center', fontsize=12, color='red')

        buf = BytesIO()
        plt.savefig(buf, format='jpeg', bbox_inches='tight', pad_inches=0, dpi=300)
        buf.seek(0)
        logger.info("Overlayed image created successfully.")
        return buf

    except Exception as e:
        logger.error(f"Error during overlay process: {e}")
        return None

def create_transparent_to_jet_cmap():
    jet = plt.cm.jet(np.arange(256))
    jet[:64, -1] = np.linspace(0, 1, 64)
    transparent_to_jet = mcolors.ListedColormap(jet)
    return transparent_to_jet
