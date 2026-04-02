#!/bin/bash
LOG_FILE="/var/log/airflow-startup.log"
PROJECT_DIR="/home/ec2-user/IS459_Crypto_BigData_Pipeline"
REPO_URL="https://github.com/KeJun11/IS459_Crypto_BigData_Pipeline.git"
BRANCH="main"

log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - $1" | tee -a "$LOG_FILE"
}

log "Starting Airflow startup script..."

# 1. Wait for network ready
log "Waiting for network..."
until ping -c1 google.com > /dev/null 2>&1; do
    sleep 2
done
log "Network is ready."

# 2. Run Docker service/daemon
log "Ensuring Docker is running..."
sudo systemctl start docker
sudo systemctl enable docker

# 3. Pull latest Github repository
if [ ! -d "$PROJECT_DIR" ]; then
    log "Cloning repository for first time..."
    cd /home/ec2-user
    git clone "$REPO_URL" "$PROJECT_DIR"
    cd "$PROJECT_DIR"
else
    cd "$PROJECT_DIR"
    log "Pulling latest Git repository..."
    git pull origin "$BRANCH"
fi

# 4. Docker Compose up & restart
log "Starting Docker Compose..."
docker-compose down || true  # Ignore error if not running
docker-compose up -d --force-recreate

log "Airflow startup completed successfully."