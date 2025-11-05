# Excel Allocation System

A web-based application for processing Excel allocation and data files with role-based access for Admin and Agent users.

## Features

### Admin Role

- Upload allocation Excel files
- Upload data Excel files
- Process and analyze uploaded files
- Download processed results
- Reset application state

### Agent Role (Coming Soon)

- Upload status files
- View processing status

## Installation

1. Clone or download the repository
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Usage

1. Start the application:

   ```bash
   python app.py
   ```

2. Open your browser and go to: `http://localhost:5003`

3. Switch between Admin and Agent roles using the role selector

4. For Admin role:
   - Upload allocation file (Excel format)
   - Upload data file (Excel format)
   - Click "Process Files" to analyze the data
   - Download the processed results

## File Requirements

- **Allocation File**: Excel file (.xlsx or .xls) containing allocation data
- **Data File**: Excel file (.xlsx or .xls) containing related data for processing

## Deployment

The application is configured for deployment on platforms like Heroku/Railway with:

- `Procfile` for web process definition
- `runtime.txt` for Python version specification
- `requirements.txt` for dependency management

### Timezone Configuration for Railway/Production

Railway servers run in **UTC timezone**. The cleanup cron job is timezone-aware and can be configured via environment variables:

**Environment Variables:**

- `CLEANUP_TIMEZONE`: Timezone for cleanup schedule (default: `Asia/Kolkata` for IST)
- `CLEANUP_HOUR`: Hour in 24-hour format (default: `18` for 6 PM)
- `CLEANUP_MINUTE`: Minute (default: `0`)

**Example for Railway:**
To run cleanup at 6 PM IST (which is 12:30 PM UTC):

- Set `CLEANUP_TIMEZONE=Asia/Kolkata`
- Set `CLEANUP_HOUR=18`
- Set `CLEANUP_MINUTE=0`

The scheduler will automatically convert IST 6 PM to UTC 12:30 PM.

**For UTC timezone:**

- Set `CLEANUP_TIMEZONE=UTC`
- Set `CLEANUP_HOUR=18` (for 6 PM UTC)

### Reminder Email System

The system automatically sends reminder emails to agents every 2 hours from their shift start time.

**How it works:**

1. After processing allocation files, the system extracts each agent's shift start time
2. Every 2 hours, the system checks if it's time to send reminders
3. Reminders are sent at 2-hour intervals from shift start (e.g., if shift starts at 8 AM, reminders at 8 AM, 10 AM, 12 PM, 2 PM, etc.)
4. Each reminder includes the agent's allocation details and insurance companies

**Timezone Configuration:**

- `REMINDER_TIMEZONE`: Timezone for shift times (default: `Asia/Kolkata` for IST)
- Shift times are stored in the specified timezone
- The system automatically converts times correctly on Railway (UTC servers)

**Example:**

- Agent shift starts at 8:00 AM IST
- Reminders sent at: 8:00 AM, 10:00 AM, 12:00 PM, 2:00 PM, 4:00 PM, 6:00 PM IST
- On Railway (UTC), these automatically convert to the correct UTC times

## Technology Stack

- **Backend**: Flask (Python web framework)
- **Data Processing**: Pandas, OpenPyXL
- **Frontend**: HTML5, CSS3, JavaScript
- **Styling**: Modern CSS with gradients and responsive design

## Development

The application follows the same structure as other Excel automation tools in the parent directory:

- Single-file Flask application with embedded HTML template
- Global state management for file uploads
- File processing with pandas
- Download functionality for processed results

## Future Enhancements

- Agent role functionality for status file uploads
- Advanced Excel processing algorithms
- User authentication and session management
- Database integration for persistent storage
- API endpoints for external integrations
