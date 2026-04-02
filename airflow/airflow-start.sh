#!/bin/bash
# Should be under "/usr/local/bin" in EC2 instance & set as executable (sudo chmod +x airflow-start.sh)

LOG_FILE="/var/log/airflow-start.log"
PROJECT_DIR="/home/ec2-user/IS459_Crypto_BigData_Pipeline"
REPO_URL="https://github.com/KeJun11/IS459_Crypto_BigData_Pipeline.git"
BRANCH="main"

log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - $1" | tee -a "$LOG_FILE"
}

log_success() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - SUCCESS: $1" | tee -a "$LOG_FILE"
}

log_failure() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - FAILED: $1" | tee -a "$LOG_FILE"
}

log "Starting Airflow start script..."

# 1. Wait for network ready
log "Waiting for network..."
until ping -c1 google.com > /dev/null 2>&1; do
    sleep 2
done
log_success "Network is ready."

# 2. Run Docker service/daemon
log "Ensuring Docker is running..."
if sudo systemctl start docker 2>&1 | tee -a "$LOG_FILE"; then
    log_success "Docker service started successfully."
else
    log_failure "Failed to start Docker service."
    exit 1
fi
sudo systemctl enable docker

# 3. Pull latest Github repository
if [ ! -d "$PROJECT_DIR" ]; then
    log "Cloning repository for first time..."
    cd /home/ec2-user

    if sudo git clone "$REPO_URL" "$PROJECT_DIR" 2>&1 | tee -a "$LOG_FILE"; then
        log_success "Repository cloned successfully."
    else
        log_failure "Failed to clone repository."
        exit 1
    fi
    cd "$PROJECT_DIR"
else
    cd "$PROJECT_DIR"
    log "Pulling latest Git repository..."

    if sudo git pull origin "$BRANCH" 2>&1 | tee -a "$LOG_FILE"; then
        log_success "Repository updated successfully."
    else
        log_failure "Failed to update repository."
        exit 1
    fi
fi

# 4. Docker Compose up & restart
log "Starting Docker Compose..."
docker-compose down || true  # Ignore error if not running
if docker-compose up -d --force-recreate 2>&1 | tee -a "$LOG_FILE"; then
    log_success "Docker Compose started successfully."
else
    log_failure "Failed to start Docker Compose."
    exit 1
fi

log_success "Airflow startup completed successfully."