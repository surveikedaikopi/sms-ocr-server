# SMS-OCR-SERVER

## Directory Structure

### Explanation:
- **config/**: Contains configuration files.
- **controllers/**: Contains the main logic files for different functionalities (e.g., `bubble.py`, `scto.py`, `sms.py`, `whatsapp.py`).
- **data/**: Contains data files related to location (e.g., `location.cpg`, `location.dbf`, `location.prj`, `location.shp`, `location.shx`).
- **utils/**: Contains utility functions (e.g., `utils.py`).
- **Dockerfile**: Docker configuration file.
- **main.py**: Main application file.
- **README.txt**: Documentation file.
- **requirements.txt**: Python dependencies file.

## Docker Commands

### Build and Run Docker Container

```bash
sudo docker build -t webhook .
sudo docker run -d -p 8008:8008 --env-file .env webhook
```

### View Docker Logs

```bash
sudo docker logs <container_id>
```