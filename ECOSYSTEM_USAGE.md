# Ecosystem Configuration Files

This directory contains PM2 ecosystem configuration files for different deployment modes.

## Available Configurations

### 1. `ecosystem.priority.config.json`
Runs priority queue bots + API:
- extract-queue-bot-priority (priority=1)
- scrape-queue-bot-priority (priority=1)  
- data-etl-api

### 2. `ecosystem.normal.config.json`
Runs normal queue bots + API:
- extract-queue-bot-normal (priority=0)
- scrape-queue-bot-normal (priority=0)
- data-etl-api

## Usage Commands

```bash
# Start priority mode (recommended for production)
pm2 start ecosystem.priority.config.json

# Start normal mode (for development/testing)
pm2 start ecosystem.normal.config.json

# Stop all services
pm2 stop all

# Restart all services
pm2 restart all

# View logs
pm2 logs

# View specific app logs
pm2 logs extract-queue-bot-priority
pm2 logs extract-queue-bot-normal
pm2 logs data-etl-api

# Check status
pm2 status
```

## Log Files

Each configuration uses separate log files to avoid conflicts:

### Priority Mode Logs:
- `./logs/extract-queue-bot-priority-*.log`
- `./logs/scrape-queue-bot-priority-*.log`
- `./logs/data-etl-api-*.log`

### Normal Mode Logs:
- `./logs/extract-queue-bot-normal-*.log`
- `./logs/scrape-queue-bot-normal-*.log`
- `./logs/data-etl-api-*.log`

## Key Differences

| Config   | Extract Priority | Extract Normal | Scrape Priority | Scrape Normal | API |
|----------|------------------|----------------|-----------------|---------------|-----|
| priority | ✅               | ❌             | ✅              |           ❌  |  ✅ |
| normal   | ❌               | ✅             | ❌              | ✅            |  ✅ |

## Environment Variables

All configurations use the same environment variables:
- `PYTHONPATH`: Module search paths
- `PYTHONUNBUFFERED`: Immediate stdout/stderr output
- `NODE_ENV`: Environment mode (production)

## Deployment Recommendations

- **Production**: Use `ecosystem.priority.config.json`
- **Development/Testing**: Use `ecosystem.normal.config.json`
