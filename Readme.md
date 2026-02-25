
# Weather_Pull

A Python-based weather data extraction and alerting system that fetches weather information and sends notifications via Telegram.

## Overview

Weather_Pull is a utility project designed to automatically pull weather data and send alerts through Telegram. The system integrates weather data retrieval with a Telegram messaging interface to provide timely weather notifications.

## Project Structure

```
Weather_Pull/
├── weather_loader.py          # Main weather data fetching and processing module
├── send_telegram_alert.py     # Telegram notification handler
├── chatid.py                  # Telegram chat ID configuration
├── last_run_summary.txt       # Log of last execution summary
```

## Files Description

### `weather_loader.py`
The core module responsible for:
- Fetching weather data from weather APIs
- Processing and parsing weather information
- Managing data retrieval logic
- Handling data transformations

### `send_telegram_alert.py`
Handles all Telegram messaging functionality:
- Sending weather alerts to configured Telegram chats
- Managing message formatting
- Handling Telegram bot API interactions
- Error handling for notification failures

### `chatid.py`
Configuration module for:
- Storing Telegram chat IDs
- Managing notification recipients
- Chat ID setup and validation

### `last_run_summary.txt`
Runtime log containing:
- Last execution timestamp
- Summary of operations performed
- Data processing results
- Any errors or warnings encountered

## Requirements

This project requires Python 3.x and the following dependencies:
- `requests` (for API calls)
- `python-telegram-bot` (for Telegram integration)

## Installation

1. Clone the repository:
```bash
git clone https://github.com/UEM-UGA/Weather_Pull.git
cd Weather_Pull
```

2. Install required dependencies:
```bash
pip install requests python-telegram-bot
```

3. Configure your Telegram credentials:
   - Update `chatid.py` with your Telegram bot token and chat IDs

## Usage

Run the weather pull and alert system:
```bash
python weather_loader.py
```

This will:
1. Fetch current weather data
2. Process the information
3. Send alerts to configured Telegram chats via `send_telegram_alert.py`
4. Log the execution summary to `last_run_summary.txt`

## Configuration

Before running the script, configure:
- **Telegram Bot Token**: Set in `send_telegram_alert.py`
- **Chat IDs**: Add target Telegram chat IDs to `chatid.py`
- **Weather Data Source**: Configure API endpoints in `weather_loader.py`

## Automation

To run this automatically on a schedule, use a task scheduler:


### Windows (Task Scheduler)
Create a scheduled task pointing to `weather_loader.py`

## Logging

Execution logs are stored in `last_run_summary.txt`, which includes:
- Timestamp of last run
- Operations completed
- Number of alerts sent
- Any errors encountered

## Development

### Language
- **Python** (100%)

### Future Enhancements
- Multiple weather data sources
- Configurable alert thresholds
- Weather history tracking
- Web dashboard for monitoring
- Database integration for historical data

## Troubleshooting

If alerts are not being sent:
1. Verify Telegram bot token in `send_telegram_alert.py`
2. Check chat IDs in `chatid.py`
3. Ensure network connectivity for API calls
4. Review `last_run_summary.txt` for error messages

## Author

UEM-UGA
