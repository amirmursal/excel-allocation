# Database Setup Guide

## Overview

The Excel Allocation System now uses a database for persistent storage of:

- User authentication and employee records
- Session management
- File upload and processing data
- Allocation results

## Database Support

- **Local Development**: SQLite (default)
- **Production (Railway/Heroku)**: PostgreSQL

## Setup Instructions

### 1. Local Development

```bash
# Install dependencies
pip install -r requirements.txt

# Initialize database
python migrate.py

# Run application
python app.py
```

### 2. Railway Deployment

1. Add PostgreSQL service to your Railway project
2. Set environment variables:
   ```
   DATABASE_URL=postgresql://username:password@host:port/database
   SECRET_KEY=your-secret-key
   ```
3. Deploy the application

### 3. Environment Variables

Create a `.env` file with:

```env
SECRET_KEY=your-secret-key-change-in-production
DATABASE_URL=postgresql://username:password@host:port/database
MAIL_USERNAME=your-email@gmail.com
MAIL_PASSWORD=your-app-password
```

## Database Models

### Users Table

- `id`: Primary key
- `username`: Unique username
- `email`: User email
- `password_hash`: Hashed password
- `role`: 'admin' or 'agent'
- `name`: Display name
- `is_active`: Account status
- `created_at`: Account creation date
- `last_login`: Last login timestamp

### UserSessions Table

- `id`: Session ID (UUID)
- `user_id`: Foreign key to users
- `session_data`: JSON session data
- `created_at`: Session creation
- `expires_at`: Session expiration
- `is_active`: Session status

### Allocations Table

- `id`: Primary key
- `user_id`: Foreign key to users
- `allocation_filename`: Uploaded allocation file name
- `data_filename`: Uploaded data file name
- `allocation_data`: JSON allocation data
- `data_file_data`: JSON data file content
- `processing_result`: Processing results
- `agent_allocations_data`: JSON agent allocations
- `created_at`: Record creation
- `updated_at`: Last update

## Default Users

After initialization, these users are created:

- **admin** / admin123 (Admin role)
- **agent1** / agent123 (Agent role)
- **agent2** / agent456 (Agent role)

## Features

- ✅ Persistent user authentication
- ✅ Database-backed session management
- ✅ File upload data persistence
- ✅ Automatic session cleanup
- ✅ Role-based access control
- ✅ Password hashing
- ✅ Railway/Heroku deployment ready
