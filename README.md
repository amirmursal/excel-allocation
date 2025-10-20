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

The application is configured for deployment on platforms like Heroku with:

- `Procfile` for web process definition
- `runtime.txt` for Python version specification
- `requirements.txt` for dependency management

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
