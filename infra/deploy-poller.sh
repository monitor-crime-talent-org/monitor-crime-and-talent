#!/bin/sh
# 1. Install required tools (Fixed typo: docker-cli)
apk add --no-cache git docker-cli docker-cli-compose

# 2. Configure Git to trust the local directory
git config --global --add safe.directory /app

# 3. Start Polling Loop
while true; do
    echo "$(date): Checking for updates on 'main'..."
    git fetch origin main
    
    LOCAL=$(git rev-parse HEAD)
    REMOTE=$(git rev-parse origin/main)

    if [ "$LOCAL" != "$REMOTE" ]; then
        echo "New changes detected! ($LOCAL -> $REMOTE)"
        git pull origin main
        
        echo "Redeploying services..."
        # We run this on the host via the mounted socket
        docker compose up -d --build
        echo "Redeploy complete."
    else
        echo "Already up to date."
    fi

    sleep 60
done
