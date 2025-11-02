#!/usr/bin/env python3
"""
Excel Allocation System - Web Application
Admin can upload allocation and data files, Agent can upload status files
"""

from flask import Flask, render_template_string, request, jsonify, send_file, redirect, session, url_for, flash
from flask_mail import Mail, Message
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate
import pandas as pd
import os
import re
from datetime import datetime, timedelta
from werkzeug.utils import secure_filename
from werkzeug.security import generate_password_hash, check_password_hash
import tempfile
import io
import uuid
import json
from functools import wraps
from dotenv import load_dotenv
# Google OAuth imports
from google.auth.transport import requests
from google.oauth2 import id_token
import requests as req
# Scheduler for reminder emails
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger

# Load environment variables
load_dotenv()

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max file size
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'your-secret-key-change-in-production')

# Database configuration
DATABASE_URL = os.environ.get('DATABASE_URL')
if DATABASE_URL:
    # For Railway/Heroku deployment
    if DATABASE_URL.startswith('postgres://'):
        DATABASE_URL = DATABASE_URL.replace('postgres://', 'postgresql://', 1)
    app.config['SQLALCHEMY_DATABASE_URI'] = DATABASE_URL
else:
    # For local development
    app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///excel_allocation.db'

app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

# Initialize database
db = SQLAlchemy(app)
migrate = Migrate(app, db)

# Email configuration
app.config['MAIL_SERVER'] = 'smtp.gmail.com'
app.config['MAIL_PORT'] = 587
app.config['MAIL_USE_TLS'] = True
app.config['MAIL_USERNAME'] = 'amirmursal@gmail.com'
app.config['MAIL_PASSWORD'] = 'wgps wdsn ycly rnqt'    
app.config['MAIL_DEFAULT_SENDER'] = 'amirmursal@gmail.com'

# Initialize Flask-Mail
mail = Mail(app)

# Global variable to store agent allocations data for reminders
agent_allocations_for_reminders = None

# Google OAuth Configuration
GOOGLE_CLIENT_ID = os.environ.get('GOOGLE_CLIENT_ID')
GOOGLE_CLIENT_SECRET = os.environ.get('GOOGLE_CLIENT_SECRET')
GOOGLE_DISCOVERY_URL = "https://accounts.google.com/.well-known/openid_configuration"

# Database Models
class User(db.Model):
    """User model for authentication and employee management"""
    __tablename__ = 'users'
    
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), unique=True, nullable=True)  # Made nullable for OAuth users
    email = db.Column(db.String(120), unique=True, nullable=False)
    password_hash = db.Column(db.String(255), nullable=True)  # Made nullable for OAuth users
    role = db.Column(db.String(20), nullable=False, default='agent')  # admin, agent
    name = db.Column(db.String(100), nullable=False)
    is_active = db.Column(db.Boolean, default=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    last_login = db.Column(db.DateTime)
    # Google OAuth fields
    google_id = db.Column(db.String(100), unique=True, nullable=True)
    auth_provider = db.Column(db.String(20), default='local')  # local, google
    
    # Relationships
    sessions = db.relationship('UserSession', backref='user', lazy=True, cascade='all, delete-orphan')
    allocations = db.relationship('Allocation', backref='user', lazy=True)
    
    def set_password(self, password):
        """Hash and set password"""
        self.password_hash = generate_password_hash(password)
    
    def check_password(self, password):
        """Check if provided password matches hash"""
        return check_password_hash(self.password_hash, password)
    
    def to_dict(self):
        """Convert user to dictionary"""
        return {
            'id': self.id,
            'username': self.username,
            'email': self.email,
            'role': self.role,
            'name': self.name,
            'is_active': self.is_active,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'last_login': self.last_login.isoformat() if self.last_login else None
        }

class UserSession(db.Model):
    """User session model for database-based session management"""
    __tablename__ = 'user_sessions'
    
    id = db.Column(db.String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    user_id = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=False)
    session_data = db.Column(db.Text)  # JSON string
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    expires_at = db.Column(db.DateTime, nullable=False)
    is_active = db.Column(db.Boolean, default=True)
    
    def set_data(self, data):
        """Set session data as JSON string"""
        self.session_data = json.dumps(data)
    
    def get_data(self):
        """Get session data from JSON string"""
        if self.session_data:
            return json.loads(self.session_data)
        return {}
    
    def is_expired(self):
        """Check if session is expired"""
        return datetime.utcnow() > self.expires_at

class Allocation(db.Model):
    """Allocation model for storing file processing data"""
    __tablename__ = 'allocations'
    
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=False)
    allocation_filename = db.Column(db.String(255))
    data_filename = db.Column(db.String(255))
    allocation_data = db.Column(db.Text)  # JSON string
    data_file_data = db.Column(db.Text)  # JSON string
    processing_result = db.Column(db.Text)
    agent_allocations_data = db.Column(db.Text)  # JSON string
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    def set_allocation_data(self, data):
        """Set allocation data as JSON string"""
        if data is not None:
            # Convert pandas DataFrames to JSON-serializable format
            if isinstance(data, dict):
                serializable_data = {}
                for key, value in data.items():
                    if isinstance(value, pd.DataFrame):
                        # Convert DataFrame to records and handle Timestamps
                        df_records = value.to_dict('records')
                        # Convert any Timestamp objects to strings
                        for record in df_records:
                            for k, v in record.items():
                                if hasattr(v, 'isoformat'):  # Check if it's a Timestamp
                                    record[k] = v.isoformat()
                        serializable_data[key] = df_records
                    else:
                        serializable_data[key] = value
                self.allocation_data = json.dumps(serializable_data)
            elif isinstance(data, pd.DataFrame):
                # Convert DataFrame to records and handle Timestamps
                df_records = data.to_dict('records')
                # Convert any Timestamp objects to strings
                for record in df_records:
                    for k, v in record.items():
                        if hasattr(v, 'isoformat'):  # Check if it's a Timestamp
                            record[k] = v.isoformat()
                self.allocation_data = json.dumps(df_records)
            else:
                self.allocation_data = json.dumps(data)
        else:
            self.allocation_data = None
    
    def get_allocation_data(self):
        """Get allocation data from JSON string"""
        if self.allocation_data:
            data = json.loads(self.allocation_data)
            # Convert back to pandas DataFrames if needed
            if isinstance(data, dict):
                converted_data = {}
                for key, value in data.items():
                    if isinstance(value, list) and len(value) > 0 and isinstance(value[0], dict):
                        # This looks like a DataFrame converted to records
                        converted_data[key] = pd.DataFrame(value)
                    else:
                        converted_data[key] = value
                return converted_data
            elif isinstance(data, list) and len(data) > 0 and isinstance(data[0], dict):
                # This looks like a single DataFrame converted to records
                return pd.DataFrame(data)
            else:
                return data
        return None
    
    def set_data_file_data(self, data):
        """Set data file data as JSON string"""
        if data is not None:
            # Convert pandas DataFrames to JSON-serializable format
            if isinstance(data, dict):
                serializable_data = {}
                for key, value in data.items():
                    if isinstance(value, pd.DataFrame):
                        # Convert DataFrame to records and handle Timestamps
                        df_records = value.to_dict('records')
                        # Convert any Timestamp objects to strings
                        for record in df_records:
                            for k, v in record.items():
                                if hasattr(v, 'isoformat'):  # Check if it's a Timestamp
                                    record[k] = v.isoformat()
                        serializable_data[key] = df_records
                    else:
                        serializable_data[key] = value
                self.data_file_data = json.dumps(serializable_data)
            elif isinstance(data, pd.DataFrame):
                # Convert DataFrame to records and handle Timestamps
                df_records = data.to_dict('records')
                # Convert any Timestamp objects to strings
                for record in df_records:
                    for k, v in record.items():
                        if hasattr(v, 'isoformat'):  # Check if it's a Timestamp
                            record[k] = v.isoformat()
                self.data_file_data = json.dumps(df_records)
            else:
                self.data_file_data = json.dumps(data)
        else:
            self.data_file_data = None
    
    def get_data_file_data(self):
        """Get data file data from JSON string"""
        if self.data_file_data:
            data = json.loads(self.data_file_data)
            # Convert back to pandas DataFrames if needed
            if isinstance(data, dict):
                converted_data = {}
                for key, value in data.items():
                    if isinstance(value, list) and len(value) > 0 and isinstance(value[0], dict):
                        # This looks like a DataFrame converted to records
                        converted_data[key] = pd.DataFrame(value)
                    else:
                        converted_data[key] = value
                return converted_data
            elif isinstance(data, list) and len(data) > 0 and isinstance(data[0], dict):
                # This looks like a single DataFrame converted to records
                return pd.DataFrame(data)
            else:
                return data
        return None
    
    def set_agent_allocations_data(self, data):
        """Set agent allocations data as JSON string"""
        if data is not None:
            # Convert pandas DataFrames to JSON-serializable format
            if isinstance(data, dict):
                serializable_data = {}
                for key, value in data.items():
                    if isinstance(value, pd.DataFrame):
                        # Convert DataFrame to records and handle Timestamps
                        df_records = value.to_dict('records')
                        # Convert any Timestamp objects to strings
                        for record in df_records:
                            for k, v in record.items():
                                if hasattr(v, 'isoformat'):  # Check if it's a Timestamp
                                    record[k] = v.isoformat()
                        serializable_data[key] = df_records
                    else:
                        serializable_data[key] = value
                self.agent_allocations_data = json.dumps(serializable_data)
            elif isinstance(data, pd.DataFrame):
                # Convert DataFrame to records and handle Timestamps
                df_records = data.to_dict('records')
                # Convert any Timestamp objects to strings
                for record in df_records:
                    for k, v in record.items():
                        if hasattr(v, 'isoformat'):  # Check if it's a Timestamp
                            record[k] = v.isoformat()
                self.agent_allocations_data = json.dumps(df_records)
            else:
                self.agent_allocations_data = json.dumps(data)
        else:
            self.agent_allocations_data = None
    
    def get_agent_allocations_data(self):
        """Get agent allocations data from JSON string"""
        if self.agent_allocations_data:
            data = json.loads(self.agent_allocations_data)
            # Convert back to pandas DataFrames if needed
            if isinstance(data, dict):
                converted_data = {}
                for key, value in data.items():
                    if isinstance(value, list) and len(value) > 0 and isinstance(value[0], dict):
                        # This looks like a DataFrame converted to records
                        converted_data[key] = pd.DataFrame(value)
                    else:
                        converted_data[key] = value
                return converted_data
            elif isinstance(data, list) and len(data) > 0 and isinstance(data[0], dict):
                # This looks like a single DataFrame converted to records
                return pd.DataFrame(data)
            else:
                return data
        return None

class AgentWorkFile(db.Model):
    """Agent work file model for storing agent uploads"""
    __tablename__ = 'agent_work_files'
    
    id = db.Column(db.Integer, primary_key=True)
    agent_id = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=False)
    filename = db.Column(db.String(255), nullable=False)
    file_data = db.Column(db.Text)  # JSON string of processed data
    upload_date = db.Column(db.DateTime, default=datetime.utcnow)
    status = db.Column(db.String(50), default='uploaded')  # uploaded, processed, consolidated
    notes = db.Column(db.Text)  # Optional notes from agent
    
    # Relationships
    agent = db.relationship('User', backref='work_files')
    
    def set_file_data(self, data):
        """Set file data as JSON string"""
        if data is not None:
            # Convert pandas DataFrames to JSON-serializable format
            if isinstance(data, dict):
                serializable_data = {}
                for key, value in data.items():
                    if isinstance(value, pd.DataFrame):
                        # Convert DataFrame to records and handle Timestamps
                        df_records = value.to_dict('records')
                        # Convert any Timestamp objects to strings
                        for record in df_records:
                            for k, v in record.items():
                                if hasattr(v, 'isoformat'):  # Check if it's a Timestamp
                                    record[k] = v.isoformat()
                        serializable_data[key] = df_records
                    else:
                        serializable_data[key] = value
                self.file_data = json.dumps(serializable_data)
            elif isinstance(data, pd.DataFrame):
                # Convert DataFrame to records and handle Timestamps
                df_records = data.to_dict('records')
                # Convert any Timestamp objects to strings
                for record in df_records:
                    for k, v in record.items():
                        if hasattr(v, 'isoformat'):  # Check if it's a Timestamp
                            record[k] = v.isoformat()
                self.file_data = json.dumps(df_records)
            else:
                self.file_data = json.dumps(data)
        else:
            self.file_data = None
    
    def get_file_data(self):
        """Get file data from JSON string"""
        if self.file_data:
            data = json.loads(self.file_data)
            # Convert back to pandas DataFrames if needed
            if isinstance(data, dict):
                converted_data = {}
                for key, value in data.items():
                    if isinstance(value, list) and len(value) > 0 and isinstance(value[0], dict):
                        # This looks like a DataFrame converted to records
                        converted_data[key] = pd.DataFrame(value)
                    else:
                        converted_data[key] = value
                return converted_data
            elif isinstance(data, list) and len(data) > 0 and isinstance(data[0], dict):
                # This looks like a single DataFrame converted to records
                return pd.DataFrame(data)
            else:
                return data
        return None

# Global variables to store session data (fallback for backward compatibility)
allocation_data = None
data_file_data = None
allocation_filename = None
data_filename = None
processing_result = None

# Agent processing result
agent_processing_result = None
agent_allocations_data = None

# Database helper functions
def init_database():
    """Initialize database and create default users"""
    with app.app_context():
        db.create_all()
        
        # Create default admin user if it doesn't exist
        admin_user = User.query.filter_by(username='admin').first()
        if not admin_user:
            admin_user = User(
                username='admin',
                email='admin@example.com',
                role='admin',
                name='Administrator'
            )
            admin_user.set_password('admin123')
            db.session.add(admin_user)
        
        # Note: Agent users will be created automatically via Google OAuth
        # No need to create static agent accounts
        
        db.session.commit()

def get_user_by_username(username):
    """Get user by username"""
    return User.query.filter_by(username=username, is_active=True).first()

def create_user_session(user_id, session_data=None, expires_hours=24):
    """Create a new user session"""
    expires_at = datetime.utcnow() + timedelta(hours=expires_hours)
    session = UserSession(
        user_id=user_id,
        expires_at=expires_at
    )
    if session_data:
        session.set_data(session_data)
    db.session.add(session)
    db.session.commit()
    return session

def get_user_session(session_id):
    """Get user session by ID"""
    return UserSession.query.filter_by(id=session_id, is_active=True).first()

def delete_user_session(session_id):
    """Delete user session"""
    session = UserSession.query.filter_by(id=session_id).first()
    if session:
        session.is_active = False
        db.session.commit()

def cleanup_expired_sessions():
    """Clean up expired sessions"""
    expired_sessions = UserSession.query.filter(
        UserSession.expires_at < datetime.utcnow()
    ).all()
    for session in expired_sessions:
        session.is_active = False
    db.session.commit()

def get_or_create_allocation(user_id):
    """Get or create allocation record for user"""
    allocation = Allocation.query.filter_by(user_id=user_id).first()
    if not allocation:
        allocation = Allocation(user_id=user_id)
        db.session.add(allocation)
        db.session.commit()
    return allocation

def save_allocation_data(user_id, allocation_data=None, allocation_filename=None, 
                        data_file_data=None, data_filename=None, 
                        processing_result=None, agent_allocations_data=None):
    """Save allocation data to database"""
    allocation = get_or_create_allocation(user_id)
    
    if allocation_data is not None:
        allocation.set_allocation_data(allocation_data)
    if allocation_filename is not None:
        allocation.allocation_filename = allocation_filename
    if data_file_data is not None:
        allocation.set_data_file_data(data_file_data)
    if data_filename is not None:
        allocation.data_filename = data_filename
    if processing_result is not None:
        allocation.processing_result = processing_result
    if agent_allocations_data is not None:
        allocation.set_agent_allocations_data(agent_allocations_data)
    
    allocation.updated_at = datetime.utcnow()
    db.session.commit()
    return allocation

def get_allocation_data(user_id):
    """Get allocation data from database"""
    allocation = Allocation.query.filter_by(user_id=user_id).first()
    if allocation:
        return {
            'allocation_data': allocation.get_allocation_data(),
            'allocation_filename': allocation.allocation_filename,
            'data_file_data': allocation.get_data_file_data(),
            'data_filename': allocation.data_filename,
            'processing_result': allocation.processing_result,
            'agent_allocations_data': allocation.get_agent_allocations_data()
        }
    return None

def save_agent_work_file(agent_id, filename, file_data, notes=None):
    """Save agent work file to database"""
    work_file = AgentWorkFile(
        agent_id=agent_id,
        filename=filename,
        notes=notes
    )
    work_file.set_file_data(file_data)
    db.session.add(work_file)
    db.session.commit()
    return work_file

def get_agent_work_files(agent_id=None):
    """Get agent work files, optionally filtered by agent"""
    if agent_id:
        return AgentWorkFile.query.filter_by(agent_id=agent_id).order_by(AgentWorkFile.upload_date.desc()).all()
    return AgentWorkFile.query.order_by(AgentWorkFile.upload_date.desc()).all()

def get_all_agent_work_files():
    """Get all agent work files for consolidation"""
    return AgentWorkFile.query.filter_by(status='uploaded').order_by(AgentWorkFile.upload_date.desc()).all()

# Google OAuth helper functions
def get_google_provider_cfg():
    """Get Google OAuth provider configuration"""
    # Use hardcoded Google OAuth endpoints instead of discovery
    return {
        "authorization_endpoint": "https://accounts.google.com/o/oauth2/v2/auth",
        "token_endpoint": "https://oauth2.googleapis.com/token",
        "userinfo_endpoint": "https://www.googleapis.com/oauth2/v3/userinfo"
    }

def verify_google_token(token):
    """Verify Google OAuth token and return user info"""
    try:
        # Verify the token
        idinfo = id_token.verify_oauth2_token(token, requests.Request(), GOOGLE_CLIENT_ID)
        
        # Verify the issuer
        if idinfo['iss'] not in ['accounts.google.com', 'https://accounts.google.com']:
            raise ValueError('Wrong issuer.')
        
        return {
            'google_id': idinfo['sub'],
            'email': idinfo['email'],
            'name': idinfo['name'],
            'picture': idinfo.get('picture', '')
        }
    except ValueError as e:
        return None

def get_or_create_google_user(google_user_info):
    """Get existing user or create new user from Google OAuth info"""
    # First try to find by Google ID
    user = User.query.filter_by(google_id=google_user_info['google_id']).first()
    
    if user:
        return user
    
    # If not found by Google ID, try to find by email
    user = User.query.filter_by(email=google_user_info['email']).first()
    
    if user:
        # Update existing user with Google ID
        user.google_id = google_user_info['google_id']
        user.auth_provider = 'google'
        user.name = google_user_info['name']
        db.session.commit()
        return user
    
    # Create new user
    user = User(
        email=google_user_info['email'],
        name=google_user_info['name'],
        google_id=google_user_info['google_id'],
        auth_provider='google',
        role='agent',  # Default role for OAuth users
        is_active=True
    )
    db.session.add(user)
    db.session.commit()
    return user

# Authentication decorators
def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        # Check for database session first
        db_session_id = session.get('db_session_id')
        if db_session_id:
            db_session = get_user_session(db_session_id)
            if db_session and not db_session.is_expired():
                # Update session data in Flask session
                session_data = db_session.get_data()
                session.update(session_data)
                return f(*args, **kwargs)
            else:
                # Session expired, clean up
                if db_session:
                    delete_user_session(db_session_id)
                session.clear()
        
        # Fallback to Flask session
        if 'user_id' not in session:
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function

def admin_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        # Check for database session first
        db_session_id = session.get('db_session_id')
        if db_session_id:
            db_session = get_user_session(db_session_id)
            if db_session and not db_session.is_expired():
                session_data = db_session.get_data()
                if session_data.get('user_role') != 'admin':
                    flash('Access denied. Admin privileges required.', 'error')
                    return redirect(url_for('dashboard'))
                session.update(session_data)
                return f(*args, **kwargs)
            else:
                if db_session:
                    delete_user_session(db_session_id)
                session.clear()
        
        # Fallback to Flask session
        if 'user_id' not in session:
            return redirect(url_for('login'))
        if session.get('user_role') != 'admin':
            flash('Access denied. Admin privileges required.', 'error')
            return redirect(url_for('dashboard'))
        return f(*args, **kwargs)
    return decorated_function

def agent_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        # Check for database session first
        db_session_id = session.get('db_session_id')
        if db_session_id:
            db_session = get_user_session(db_session_id)
            if db_session and not db_session.is_expired():
                session_data = db_session.get_data()
                if session_data.get('user_role') != 'agent':
                    flash('Access denied. Agent privileges required.', 'error')
                    return redirect(url_for('dashboard'))
                session.update(session_data)
                return f(*args, **kwargs)
            else:
                if db_session:
                    delete_user_session(db_session_id)
                session.clear()
        
        # Fallback to Flask session
        if 'user_id' not in session:
            return redirect(url_for('login'))
        if session.get('user_role') != 'agent':
            flash('Access denied. Agent privileges required.', 'error')
            return redirect(url_for('dashboard'))
        return f(*args, **kwargs)
    return decorated_function

# Login Template
LOGIN_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Login - Excel Allocation System</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { 
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; 
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            display: flex;
            align-items: center;
            justify-content: center;
            padding: 20px;
        }
        .login-container { 
            background: white; 
            border-radius: 15px; 
            box-shadow: 0 10px 30px rgba(0,0,0,0.2);
            padding: 40px;
            width: 100%;
            max-width: 400px;
        }
        .login-header {
            text-align: center;
            margin-bottom: 30px;
        }
        .login-header h1 {
            color: #333;
            font-size: 2em;
            margin-bottom: 10px;
        }
        .login-header p {
            color: #666;
            font-size: 1.1em;
        }
        .form-group {
            margin-bottom: 20px;
        }
        .form-group label {
            display: block;
            margin-bottom: 8px;
            font-weight: 600;
            color: #555;
        }
        .form-group input {
            width: 100%;
            padding: 12px;
            border: 2px solid #ddd;
            border-radius: 8px;
            font-size: 16px;
            transition: border-color 0.3s;
        }
        .form-group input:focus {
            outline: none;
            border-color: #667eea;
        }
        .login-btn {
            width: 100%;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 12px;
            border: none;
            border-radius: 8px;
            cursor: pointer;
            font-size: 16px;
            font-weight: 600;
            transition: transform 0.2s;
        }
        .login-btn:hover {
            transform: translateY(-2px);
            box-shadow: 0 5px 15px rgba(0,0,0,0.2);
        }
        .alert {
            padding: 12px;
            border-radius: 8px;
            margin-bottom: 20px;
            font-weight: 500;
        }
        .alert-error {
            background: #f8d7da;
            color: #721c24;
            border: 1px solid #f5c6cb;
        }
        .alert-success {
            background: #d4edda;
            color: #155724;
            border: 1px solid #c3e6cb;
        }
        .demo-credentials {
            background: #f8f9fa;
            padding: 15px;
            border-radius: 8px;
            margin-top: 20px;
            font-size: 14px;
        }
        .demo-credentials h4 {
            color: #333;
            margin-bottom: 10px;
        }
        .demo-credentials p {
            color: #666;
            margin: 5px 0;
        }
        .demo-credentials strong {
            color: #333;
        }
        .google-login-btn {
            width: 100%;
            background: #4285f4;
            color: white;
            padding: 12px;
            border: none;
            border-radius: 8px;
            cursor: pointer;
            font-size: 16px;
            font-weight: 600;
            transition: transform 0.2s;
            margin-top: 15px;
            display: flex;
            align-items: center;
            justify-content: center;
            gap: 10px;
        }
        .google-login-btn:hover {
            background: #3367d6;
            transform: translateY(-2px);
            box-shadow: 0 5px 15px rgba(66, 133, 244, 0.3);
        }
        .divider {
            text-align: center;
            margin: 20px 0;
            position: relative;
            color: #666;
        }
        .divider::before {
            content: '';
            position: absolute;
            top: 50%;
            left: 0;
            right: 0;
            height: 1px;
            background: #ddd;
        }
        .divider span {
            background: white;
            padding: 0 15px;
            position: relative;
        }
    </style>
    <link href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0/css/all.min.css" rel="stylesheet">
</head>
<body>
    <div class="login-container">
        <div class="login-header">
            <h1><i class="fas fa-file-excel"></i> Excel Allocation System</h1>
            <p>Agents: Use Google Login | Admins: Use username/password</p>
        </div>
        
        {% with messages = get_flashed_messages(with_categories=true) %}
            {% if messages %}
                {% for category, message in messages %}
                    <div class="alert alert-{{ category }}">{{ message }}</div>
                {% endfor %}
            {% endif %}
        {% endwith %}
        
        <form method="POST" action="{{ url_for('login') }}">
            <div class="form-group">
                <label for="username"><i class="fas fa-user"></i> Username</label>
                <input type="text" id="username" name="username" required>
            </div>
            
            <div class="form-group">
                <label for="password"><i class="fas fa-lock"></i> Password</label>
                <input type="password" id="password" name="password" required>
            </div>
            
            <button type="submit" class="login-btn">
                <i class="fas fa-sign-in-alt"></i> Login
            </button>
        </form>
        
        <div class="divider">
            <span>OR</span>
        </div>
        
        {% if GOOGLE_CLIENT_ID %}
        <a href="{{ url_for('google_login') }}" class="google-login-btn">
            <i class="fab fa-google"></i> Login with Google
        </a>
        {% else %}
        <div style="background: #fff3cd; border: 1px solid #ffeaa7; color: #856404; padding: 15px; border-radius: 8px; margin-top: 15px; text-align: center;">
            <i class="fas fa-exclamation-triangle"></i>
            <strong>Google OAuth not configured</strong><br>
            <small>Contact administrator to set up Google OAuth for agent login</small>
        </div>
        {% endif %}
        
        <div class="demo-credentials">
            <h4><i class="fas fa-info-circle"></i> Login Options</h4>
            <p><strong>Admin:</strong> Use username/password (admin / admin123)</p>
            {% if GOOGLE_CLIENT_ID %}
            <p><strong>Agents:</strong> Use "Login with Google" button above</p>
            <p style="color: #666; font-size: 12px; margin-top: 10px;">
                <i class="fas fa-info-circle"></i> Agents with Gmail accounts can login instantly with Google OAuth
            </p>
            {% else %}
            <p><strong>Agents:</strong> Google OAuth not configured - contact administrator</p>
            <p style="color: #e74c3c; font-size: 12px; margin-top: 10px;">
                <i class="fas fa-exclamation-triangle"></i> No static agent credentials available - Google OAuth required
            </p>
            {% endif %}
        </div>
    </div>
</body>
</html>
"""

# HTML Template for Excel Allocation System
HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Excel Allocation System</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { 
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; 
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            padding: 20px;
        }
        .container { 
            max-width: 1400px; 
            margin: 0 auto; 
            background: white; 
            border-radius: 15px; 
            box-shadow: 0 10px 30px rgba(0,0,0,0.2);
            overflow: hidden;
        }
        .header { 
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white; 
            padding: 30px; 
            text-align: center; 
        }
        .header h1 { font-size: 2.5em; margin-bottom: 10px; }
        .header p { font-size: 1.2em; opacity: 0.9; }
        
        .role-selector {
            display: flex;
            justify-content: center;
            gap: 20px;
            margin-top: 20px;
        }
        .role-btn {
            padding: 12px 24px;
            border: none;
            border-radius: 25px;
            background: rgba(255, 255, 255, 0.2);
            color: white;
            cursor: pointer;
            transition: all 0.3s ease;
            font-weight: 600;
            display: flex;
            align-items: center;
            gap: 8px;
        }
        .role-btn:hover {
            background: rgba(255, 255, 255, 0.3);
            transform: translateY(-2px);
        }
        .role-btn.active {
            background: rgba(255, 255, 255, 0.9);
            color: #667eea;
            box-shadow: 0 4px 15px rgba(255, 255, 255, 0.3);
        }
        
        .admin-tab-content {
            display: none;
        }
        
        .admin-tab-content.active {
            display: block;
        }
        
        .admin-tab-btn.active {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%) !important;
            color: white !important;
            border-bottom-color: #667eea !important;
        }
        
        /* Toast Notifications */
        .toast-container {
            position: fixed;
            top: 20px;
            right: 20px;
            z-index: 9999;
        }
        
        .toast {
            background: white;
            border-radius: 8px;
            box-shadow: 0 4px 12px rgba(0, 0, 0, 0.15);
            padding: 16px 20px;
            margin-bottom: 10px;
            min-width: 300px;
            max-width: 400px;
            display: flex;
            align-items: center;
            gap: 12px;
            transform: translateX(100%);
            transition: transform 0.3s ease;
            border-left: 4px solid #28a745;
        }
        
        .toast.show {
            transform: translateX(0);
        }
        
        .toast.success {
            border-left-color: #28a745;
            background: linear-gradient(135deg, #d4edda 0%, #f8f9fa 100%);
            border: 1px solid #c3e6cb;
        }
        
        .toast.error {
            border-left-color: #dc3545;
            background: linear-gradient(135deg, #f8d7da 0%, #f8f9fa 100%);
            border: 1px solid #f5c6cb;
        }
        
        .toast.warning {
            border-left-color: #ffc107;
        }
        
        .toast.info {
            border-left-color: #17a2b8;
        }
        
        .toast-icon {
            font-size: 20px;
            flex-shrink: 0;
        }
        
        .toast.success .toast-icon {
            color: #28a745;
        }
        
        .toast.error .toast-icon {
            color: #dc3545;
        }
        
        .toast.warning .toast-icon {
            color: #ffc107;
        }
        
        .toast.info .toast-icon {
            color: #17a2b8;
        }
        
        .toast-content {
            flex: 1;
        }
        
        .toast-title {
            font-weight: 600;
            margin: 0 0 4px 0;
            color: #333;
        }
        
        /* Loader/Progress Bar */
        .loader-overlay {
            position: fixed;
            top: 0;
            left: 0;
            right: 0;
            bottom: 0;
            background: rgba(0, 0, 0, 0.5);
            display: flex;
            justify-content: center;
            align-items: center;
            z-index: 10000;
            opacity: 0;
            visibility: hidden;
            transition: opacity 0.3s ease, visibility 0.3s ease;
        }
        
        .loader-overlay.show {
            opacity: 1;
            visibility: visible;
        }
        
        .loader-container {
            background: white;
            border-radius: 15px;
            padding: 40px 50px;
            box-shadow: 0 10px 40px rgba(0, 0, 0, 0.3);
            text-align: center;
            max-width: 400px;
        }
        
        .loader-spinner {
            border: 4px solid #f3f3f3;
            border-top: 4px solid #667eea;
            border-radius: 50%;
            width: 50px;
            height: 50px;
            animation: spin 1s linear infinite;
            margin: 0 auto 20px;
        }
        
        @keyframes spin {
            0% { transform: rotate(0deg); }
            100% { transform: rotate(360deg); }
        }
        
        .loader-text {
            font-size: 18px;
            color: #667eea;
            font-weight: 600;
            margin: 0;
        }
        
        .loader-subtitle {
            font-size: 14px;
            color: #666;
            margin-top: 8px;
        }
        
        .progress-bar-container {
            width: 100%;
            height: 6px;
            background: #e0e0e0;
            border-radius: 3px;
            margin-top: 20px;
            overflow: hidden;
        }
        
        .progress-bar {
            height: 100%;
            background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
            border-radius: 3px;
            width: 0%;
            transition: width 0.3s ease;
            animation: progress 1.5s ease-in-out infinite;
        }
        
        @keyframes progress {
            0% { width: 0%; }
            50% { width: 70%; }
            100% { width: 100%; }
        }
        
        .toast.success .toast-title {
            color: #155724;
        }
        
        .toast.error .toast-title {
            color: #721c24;
        }
        
        .toast-message {
            margin: 0;
            color: #666;
            font-size: 14px;
        }
        
        .toast.success .toast-message {
            color: #155724;
        }
        
        .toast.error .toast-message {
            color: #721c24;
        }
        
        .toast-close {
            background: none;
            border: none;
            font-size: 18px;
            color: #999;
            cursor: pointer;
            padding: 0;
            width: 20px;
            height: 20px;
            display: flex;
            align-items: center;
            justify-content: center;
        }
        
        .toast-close:hover {
            color: #333;
        }
        
        .content { padding: 30px; }
        .panel { display: none; }
        .panel.active { display: block; }
        
        .section { 
            margin: 25px 0; 
            padding: 25px; 
            border: 1px solid #e0e0e0; 
            border-radius: 10px; 
            background: #fafafa;
        }
        .section h3 { 
            color: #333; 
            margin-bottom: 20px; 
            font-size: 1.4em;
            border-bottom: 2px solid #667eea;
            padding-bottom: 10px;
        }
        
        .upload-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(400px, 1fr));
            gap: 30px;
            margin-bottom: 30px;
        }
        
        .upload-card {
            background: white;
            border-radius: 15px;
            padding: 25px;
            border: 2px dashed #dee2e6;
            transition: all 0.3s ease;
            text-align: center;
        }
        .upload-card:hover {
            border-color: #667eea;
            transform: translateY(-5px);
            box-shadow: 0 10px 30px rgba(0, 0, 0, 0.1);
        }
        
        .upload-header {
            display: flex;
            align-items: center;
            justify-content: center;
            gap: 15px;
            margin-bottom: 20px;
        }
        .upload-header i {
            font-size: 1.5rem;
            color: #27ae60;
        }
        .upload-header h4 {
            color: #2c3e50;
            font-size: 1.3rem;
        }
        
        .form-group { margin: 15px 0; }
        label { 
            display: block; 
            margin-bottom: 8px; 
            font-weight: 600; 
            color: #555;
        }
        input[type="file"] { 
            width: 100%; 
            padding: 12px; 
            border: 2px solid #ddd; 
            border-radius: 8px; 
            font-size: 16px;
            transition: border-color 0.3s;
        }
        input[type="file"]:focus { 
            outline: none; 
            border-color: #667eea; 
        }
        
        button { 
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white; 
            padding: 12px 25px; 
            border: none; 
            border-radius: 8px; 
            cursor: pointer; 
            margin: 5px; 
            font-size: 16px;
            font-weight: 600;
            transition: transform 0.2s;
        }
        button:hover { 
            transform: translateY(-2px);
            box-shadow: 0 5px 15px rgba(0,0,0,0.2);
        }
        button:disabled {
            background: #bdc3c7;
            cursor: not-allowed;
            transform: none;
            box-shadow: none;
        }
        
        .process-btn {
            background: linear-gradient(135deg, #27ae60, #2ecc71);
            font-size: 18px;
            padding: 15px 40px;
            border-radius: 25px;
            display: flex;
            align-items: center;
            gap: 10px;
            margin: 20px auto;
        }
        
        .file-status { 
            background: #f8f9fa; 
            padding: 15px; 
            border-radius: 8px; 
            margin: 15px 0; 
            border-left: 4px solid #667eea;
        }
        .status-success { 
            background: #d4edda; 
            color: #155724; 
            border-color: #c3e6cb; 
        }
        .status-info { 
            background: #d1ecf1; 
            color: #0c5460; 
            border-color: #bee5eb; 
        }
        
        .status-message {
            background: #f3e5f5;
            border: 2px solid #9c27b0;
            color: #4a148c;
            padding: 20px;
            border-radius: 10px;
            margin: 15px 0;
            font-size: 16px;
            line-height: 1.6;
            white-space: pre-line;
            box-shadow: 0 2px 8px rgba(156, 39, 176, 0.2);
        }
        
        .processing-status {
            position: fixed;
            top: 0;
            left: 0;
            width: 100%;
            height: 100%;
            background: rgba(0, 0, 0, 0.8);
            display: none;
            justify-content: center;
            align-items: center;
            z-index: 9999;
            text-align: center;
            color: white;
        }
        
        .processing-content {
            background: white;
            color: #333;
            padding: 40px;
            border-radius: 15px;
            box-shadow: 0 10px 30px rgba(0,0,0,0.3);
            max-width: 500px;
            width: 90%;
        }
        
        .spinner {
            width: 50px;
            height: 50px;
            border: 4px solid #e9ecef;
            border-top: 4px solid #667eea;
            border-radius: 50%;
            animation: spin 1s linear infinite;
            margin: 0 auto 20px;
        }
        
        .progress-container {
            width: 100%;
            background-color: #e0e0e0;
            border-radius: 10px;
            margin: 20px 0;
            overflow: hidden;
        }
        
        .progress-bar {
            height: 30px;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            border-radius: 10px;
            width: 0%;
            transition: width 0.3s ease;
            display: flex;
            align-items: center;
            justify-content: center;
            color: white;
            font-weight: bold;
        }
        
        .progress-text {
            margin-top: 10px;
            font-size: 16px;
            color: #666;
        }
        
        @keyframes spin {
            0% { transform: rotate(0deg); }
            100% { transform: rotate(360deg); }
        }
        
        .results-section {
            background: #f8f9fa;
            border-radius: 15px;
            padding: 25px;
            border-left: 5px solid #27ae60;
            margin: 20px 0;
        }
        
        .results-section h3 {
            color: #27ae60;
            margin-bottom: 20px;
            display: flex;
            align-items: center;
            gap: 10px;
        }
        
        .results-content {
            background: white;
            border-radius: 10px;
            padding: 20px;
            border: 1px solid #e9ecef;
            white-space: pre-wrap;
            font-family: 'Courier New', monospace;
            max-height: 400px;
            overflow-y: auto;
        }
        
        .coming-soon {
            text-align: center;
            padding: 60px 20px;
            color: #7f8c8d;
        }
        .coming-soon i {
            font-size: 4rem;
            margin-bottom: 20px;
            color: #bdc3c7;
        }
        .coming-soon h3 {
            font-size: 1.5rem;
            margin-bottom: 10px;
        }
        
        .reset-btn {
            background: linear-gradient(135deg, #ff6b6b, #ee5a52);
            color: white;
            border: none;
            padding: 12px 24px;
            border-radius: 8px;
            cursor: pointer;
            font-size: 16px;
            font-weight: 600;
            transition: all 0.3s ease;
            box-shadow: 0 4px 15px rgba(255, 107, 107, 0.3);
        }
        .reset-btn:hover {
            background: linear-gradient(135deg, #ff5252, #d32f2f);
            transform: translateY(-2px);
            box-shadow: 0 6px 20px rgba(255, 107, 107, 0.4);
        }
        
        .priority-panel {
            background: white;
            border-radius: 10px;
            padding: 20px;
            margin: 10px 0;
            border: 2px solid #e9ecef;
        }
        
        .priority-panel.active {
            border-color: #667eea;
            box-shadow: 0 4px 15px rgba(102, 126, 234, 0.1);
        }
        
        .tab-button {
            transition: all 0.3s ease;
            opacity: 0.7;
        }
        
        .tab-button:hover {
            opacity: 1;
            transform: translateY(-2px);
        }
        
        .tab-button.active {
            opacity: 1;
            transform: translateY(-2px);
            box-shadow: 0 4px 8px rgba(0,0,0,0.2);
        }
        
        @media (max-width: 768px) {
            .upload-grid {
                grid-template-columns: 1fr;
            }
            .role-selector {
                flex-direction: column;
                align-items: center;
            }
            .header h1 {
                font-size: 2rem;
            }
        }
        
        /* Table styling */
        .agent-table tbody tr:hover {
            background-color: #f8f9fa;
        }
        
        .agent-table .process-btn:hover {
            transform: scale(1.05);
        }
        
        /* Modal styling */
        .modal {
            animation: fadeIn 0.3s ease;
        }
        
        .modal-content {
            animation: slideIn 0.3s ease;
        }
        
        .close:hover {
            opacity: 0.7;
        }
        
        @keyframes fadeIn {
            from { opacity: 0; }
            to { opacity: 1; }
        }
        
        @keyframes slideIn {
            from { transform: translateY(-50px); opacity: 0; }
            to { transform: translateY(0); opacity: 1; }
        }
        
        .modal-table {
            width: 100%;
            border-collapse: collapse;
            margin-top: 15px;
        }
        
        .modal-table th,
        .modal-table td {
            padding: 8px 12px;
            text-align: left;
            border-bottom: 1px solid #e9ecef;
        }
        
        .modal-table th {
            background-color: #f8f9fa;
            font-weight: 600;
            color: #333;
        }
        
        .modal-table tr:hover {
            background-color: #f8f9fa;
        }
        
        /* Style for serial number column */
        .modal-table th:first-child,
        .modal-table td:first-child {
            text-align: center;
            width: 60px;
            font-weight: 600;
            color: #667eea;
            background-color: #f0f2ff;
        }
        
        .modal-table th:first-child {
            background-color: #e8ecff;
        }
        
        /* Show all agent rows */
        .agent-row {
            display: table-row;
        }
    </style>
    <link href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0/css/all.min.css" rel="stylesheet">
</head>
<body>
    <div class="container">
        <div class="header">
            <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px;">
                <div>
            <h1><i class="fas fa-file-excel"></i> Excel Allocation System</h1>
            <p>Upload and process Excel files for allocation management</p>
                </div>
                <div style="display: flex; align-items: center; gap: 15px;">
                    <div style="color: white; text-align: right;">
                        <div style="font-size: 1.1em; font-weight: 600;">Welcome, {{ session.user_name }}</div>
                        <div style="font-size: 0.9em; opacity: 0.8;">{{ session.user_role.title() }} User</div>
                    </div>
                    <a href="{{ url_for('logout') }}" style="
                        background: rgba(255, 255, 255, 0.2);
                        color: white;
                        padding: 8px 16px;
                        border-radius: 20px;
                        text-decoration: none;
                        font-weight: 600;
                        transition: all 0.3s ease;
                        display: flex;
                        align-items: center;
                        gap: 8px;
                    " onmouseover="this.style.background='rgba(255, 255, 255, 0.3)'" onmouseout="this.style.background='rgba(255, 255, 255, 0.2)'">
                        <i class="fas fa-sign-out-alt"></i> Logout
                    </a>
                </div>
            </div>
            
            {% if session.user_role == 'admin' %}
            {% endif %}
        </div>

        <div class="content">
            <!-- Admin Panel -->
            <div id="admin-panel" class="panel active">
                <!-- Admin Tab Navigation -->
                <div class="admin-tabs" style="margin-bottom: 30px;">
                    <div class="tab-nav" style="display: flex; border-bottom: 2px solid #e9ecef; margin-bottom: 20px;">
                        <button class="admin-tab-btn active" onclick="switchAdminTab('file-management')" style="padding: 12px 24px; border: none; background: #f8f9fa; color: #666; cursor: pointer; border-bottom: 3px solid transparent; transition: all 0.3s;">
                            <i class="fas fa-upload"></i> File Management
                        </button>
                        <button class="admin-tab-btn" onclick="switchAdminTab('agent-allocation')" style="padding: 12px 24px; border: none; background: #f8f9fa; color: #666; cursor: pointer; border-bottom: 3px solid transparent; transition: all 0.3s;">
                            <i class="fas fa-users"></i> Agent Allocation
                        </button>
                        <button class="admin-tab-btn" onclick="switchAdminTab('agent-consolidation')" style="padding: 12px 24px; border: none; background: #f8f9fa; color: #666; cursor: pointer; border-bottom: 3px solid transparent; transition: all 0.3s;">
                            <i class="fas fa-compress-arrows-alt"></i> Agent Consolidation
                        </button>
                        <button class="admin-tab-btn" onclick="switchAdminTab('system-settings')" style="padding: 12px 24px; border: none; background: #f8f9fa; color: #666; cursor: pointer; border-bottom: 3px solid transparent; transition: all 0.3s;">
                            <i class="fas fa-cog"></i> System Settings
                        </button>
                    </div>
                </div>
                
                <!-- File Management Tab -->
                <div id="file-management-tab" class="admin-tab-content active">
                <div class="upload-grid">
                    <div class="upload-card">
                        <form action="/upload_allocation" method="post" enctype="multipart/form-data" id="allocation-form">
                            <div class="form-group">
                                <input type="file" id="allocation_file" name="file" accept=".xlsx,.xls" required>
                            </div>
                            <button type="submit" id="allocation-btn">📤 Upload Staff Details</button>
                        </form>
                    </div>

                    <div class="upload-card">
                        <form action="/upload_data" method="post" enctype="multipart/form-data" id="data-form">
                            <div class="form-group">
                                <input type="file" id="data_file" name="file" accept=".xlsx,.xls" required>
                            </div>
                            <button type="submit" id="data-btn">📤 Upload Insurance Details</button>
                        </form>
                    </div>
                </div>

                <!-- File Status -->
                <div class="section">
                    <h3>📊 File Status</h3>
                    <div class="file-status">
                        {% if allocation_filename %}
                            <div class="status-success">
                                ✅ Allocation File: {{ allocation_filename }}<br>
                                📋 Sheets: {{ allocation_data.keys() | list | length if allocation_data else 0 }}
                            </div>
                        {% else %}
                            <div class="status-info">
                                ℹ️ No agent allocation details file uploaded yet.
                            </div>
                        {% endif %}
                        
                        {% if data_filename %}
                            <div class="status-success">
                                ✅ Data File: {{ data_filename }}<br>
                                📋 Sheets: {{ data_file_data.keys() | list | length if data_file_data else 0 }}
                            </div>
                        {% else %}
                            <div class="status-info">
                                ℹ️ No insurance details file uploaded yet.
                            </div>
                        {% endif %}
                    </div>
                </div>


                <!-- Processing Section -->
                {% if data_file_data %}
                <div class="section">
                    <h3>🔄 Process Data File</h3>
                    
                    <!-- Priority Date Selection -->
                    <div class="section" style="background: #f8f9fa; margin-bottom: 20px;">
                        
                        <!-- Priority Tabs -->
                        <div class="tab-container" style="margin-bottom: 20px;">
                            <div class="tab-buttons" style="display: flex; border-bottom: 2px solid #ddd;">
                                <div class="tab-button active" id="first-priority-tab" onclick="switchPriorityTab('first')" style="padding: 12px 24px; cursor: pointer; background: #27ae60; color: white; border-radius: 8px 8px 0 0; margin-right: 2px; font-weight: bold;">First Priority</div>
                                <div class="tab-button" id="second-priority-tab" onclick="switchPriorityTab('second')" style="padding: 12px 24px; cursor: pointer; background: #f39c12; color: white; border-radius: 8px 8px 0 0; margin-right: 2px; font-weight: bold;">Second Priority</div>
                                <div class="tab-button" id="third-priority-tab" onclick="switchPriorityTab('third')" style="padding: 12px 24px; cursor: pointer; background: #e74c3c; color: white; border-radius: 8px 8px 0 0; font-weight: bold;">Third Priority</div>
                            </div>
                        </div>
                        
                        <!-- First Priority Panel -->
                        <div id="first-priority-panel" class="priority-panel" style="display: block;">
                            
                            
                            <!-- Calendar Input for First Priority Dates -->
                            <div class="form-group">
                                <div id="calendar_container" style="border: 1px solid #ddd; padding: 15px; background: white; border-radius: 8px; margin: 10px 0;"></div>
                                <div id="selected_dates_info" style="background: #f8f9fa; padding: 10px; border-radius: 5px; border: 1px solid #e9ecef;">
                                    <strong>Selected First Priority Dates:</strong> <span id="selected_count">0</span> <span id="selected_text">dates selected</span>
                                    <div id="selected_dates_list" style="margin-top: 5px; font-size: 12px; color: #666;"></div>
                                </div>
                            </div>
                            
                            <!-- Receive Date Panel (appears when appointment dates are selected) -->
                            <div id="receive-date-panel" style="display: none; margin-top: 20px; padding: 15px; background: #f0f8ff; border: 1px solid #b3d9ff; border-radius: 8px;">
                                <h4 style="margin: 0 0 15px 0; color: #2c5aa0; font-size: 16px;">
                                    <i class="fas fa-calendar-check"></i> Second Level Priority - Receive Dates
                                </h4>
                                <p style="margin: 0 0 15px 0; color: #666; font-size: 14px;">
                                    Select receive dates for each appointment date. Each appointment date has its own independent receive date selections.
                                </p>
                                <div id="appointment_receive_dates_container">
                                    <!-- Individual receive date panels for each appointment date will be populated here -->
                                </div>
                            </div>
                        </div>
                        
                        <!-- Second Priority Panel -->
                        <div id="second-priority-panel" class="priority-panel" style="display: none;">
                            
                            
                            <!-- Calendar Input for Second Priority Dates -->
                            <div class="form-group">
                                <div id="calendar_container_second" style="border: 1px solid #ddd; padding: 15px; background: white; border-radius: 8px; margin: 10px 0;"></div>
                                <div id="selected_dates_info_second" style="background: #f8f9fa; padding: 10px; border-radius: 5px; border: 1px solid #e9ecef;">
                                    <strong>Selected Second Priority Dates:</strong> <span id="selected_count_second">0</span> <span id="selected_text_second">dates selected</span>
                                    <div id="selected_dates_list_second" style="margin-top: 5px; font-size: 12px; color: #666;">No dates selected</div>
                                </div>
                            </div>
                        </div>
                        
                        <!-- Third Priority Panel -->
                        <div id="third-priority-panel" class="priority-panel" style="display: none;">
                            <p>All remaining dates will be automatically assigned "Third Priority":</p>
                            
                            <!-- Info about Third Priority -->
                            
                            <!-- Show remaining dates that will be Third Priority -->
                            <div class="form-group">
                                <div id="third_priority_dates_info" style="background: #f8f9fa; padding: 10px; border-radius: 5px; border: 1px solid #e9ecef;">
                                    <strong>Remaining Dates:</strong> <span id="third_priority_count">0</span> dates will be Third Priority
                                    <div id="third_priority_dates_list" style="margin-top: 5px; font-size: 12px; color: #666;"></div>
                                </div>
                            </div>
                        </div>
                        
                        <form action="/process_files" method="post" id="process-form">
                            <button type="submit" class="process-btn" id="process-btn">
                                <i class="fas fa-cogs"></i> Process Data File
                            </button>
                        </form>
                    </div>
                    
                    <div class="processing-status" id="processing-status">
                        <div class="processing-content">
                            <div class="spinner"></div>
                            <h3>Processing Data File...</h3>
                            <div class="progress-container">
                                <div class="progress-bar" id="progress-bar">0%</div>
                            </div>
                            <div class="progress-text" id="progress-text">Initializing...</div>
                        </div>
                    </div>
                </div>
                {% endif %}

                <!-- Status Messages -->
                {% if processing_result %}
                <div class="section">
                    <h3>📢 Processing Results</h3>
                    <div class="status-message">
                        {{ processing_result | safe }}
                    </div>
                </div>
                {% endif %}

                <!-- Download Section -->
                {% if processing_result and 'Priority processing completed successfully' in processing_result %}
                <div class="section">
                    <h3>💾 Download your Excel file with updated Priority Status assignments.</h3>
                    <form action="/download_result" method="post">
                        <button type="submit" class="process-btn" style="background: linear-gradient(135deg, #3498db, #2980b9);">
                            <i class="fas fa-download"></i> Download Processed File
                        </button>
                    </form>
                </div>
                {% endif %}

                </div>
                
                <!-- Agent Allocation Tab -->
                <div id="agent-allocation-tab" class="admin-tab-content">
                    <!-- Individual Agent Downloads -->
                    {% if agent_allocations_data %}
                    <div class="section">
                        <h3>👥 Agent Allocation Overview</h3>
                        <p>View and manage agent allocations. Each agent has been assigned specific rows based on their capacity and the allocation rules.</p>
                        
                        <div style="margin-bottom: 15px; text-align: right; display: flex; gap: 10px; justify-content: flex-end;">
                            <button type="button" class="process-btn" style="background: linear-gradient(135deg, #3498db, #2980b9); font-size: 14px; padding: 10px 20px; border: none; border-radius: 6px; color: white; cursor: pointer; transition: transform 0.2s; font-weight: 600;" onclick="viewShiftTimes()" id="view-shift-btn">
                                <i class="fas fa-clock"></i> View Shift Times
                            </button>
                            <button type="button" class="process-btn" style="background: linear-gradient(135deg, #27ae60, #2ecc71); font-size: 14px; padding: 10px 20px; border: none; border-radius: 6px; color: white; cursor: pointer; transition: transform 0.2s; font-weight: 600;" onclick="approveAllAllocations()" id="approve-all-btn">
                                <i class="fas fa-check-double"></i> Approve All Allocations
                            </button>
                        </div>
                        
                        <div style="overflow-x: auto; margin-top: 15px;">
                            <table class="agent-table" style="width: 100%; border-collapse: collapse; background: white; border-radius: 8px; overflow: hidden; box-shadow: 0 2px 10px rgba(0,0,0,0.1);">
                                <thead>
                                    <tr style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white;">
                                        <th style="padding: 15px; text-align: center; font-weight: 600; border: none; width: 60px;">Sr No</th>
                                        <th style="padding: 15px; text-align: left; font-weight: 600; border: none;">Agent Name</th>
                                        <th style="padding: 15px; text-align: center; font-weight: 600; border: none;">Allocated</th>
                                        <th style="padding: 15px; text-align: center; font-weight: 600; border: none;">Capacity</th>
                                        <th style="padding: 15px; text-align: center; font-weight: 600; border: none;">Actions</th>
                                    </tr>
                                </thead>
                                <tbody id="agentTableBody">
                                    {% for agent in agent_allocations_data %}
                                    <tr class="agent-row" style="border-bottom: 1px solid #e9ecef; transition: background-color 0.2s;" data-index="{{ loop.index0 }}">
                                        <td style="padding: 15px; text-align: center; font-weight: 600; color: #667eea;">{{ loop.index }}</td>
                                        <td style="padding: 15px; font-weight: 500; color: #333;">{{ agent.name }}</td>
                                        <td style="padding: 15px; text-align: center; color: #27ae60; font-weight: 600;">{{ agent.allocated }}</td>
                                        <td style="padding: 15px; text-align: center; color: #666;">{{ agent.capacity }}</td>
                                        <td style="padding: 15px; text-align: center;">
                                            <div style="display: flex; gap: 8px; justify-content: center;">
                                                <button type="button" class="process-btn view-btn" style="background: linear-gradient(135deg, #f39c12, #e67e22); font-size: 12px; padding: 6px 12px; border: none; border-radius: 4px; color: white; cursor: pointer; transition: transform 0.2s;" onclick="viewAgentAllocation('{{ agent.name }}')">
                                                    <i class="fas fa-eye"></i> View
                                                </button>
                                                <button type="button" class="process-btn approve-btn" style="background: linear-gradient(135deg, #3498db, #2980b9); font-size: 12px; padding: 6px 12px; border: none; border-radius: 4px; color: white; cursor: pointer; transition: transform 0.2s;" onclick="approveAllocation('{{ agent.name }}')">
                                                    <i class="fas fa-check"></i> Approve
                                                </button>
                                            </div>
                                        </td>
                                    </tr>
                                    {% endfor %}
                                </tbody>
                            </table>
                        </div>
                    </div>
                    {% else %}
                    <div class="section">
                        <h3>👥 Agent Allocation Overview</h3>
                        <div style="background: #f8f9fa; padding: 20px; border-radius: 10px;">
                            <p style="color: #666;">No agent allocation data available. Please upload allocation and data files, then process them to see agent allocations.</p>
                        </div>
                    </div>
                    {% endif %}
                    
                    <!-- Shift Times Modal -->
                    <div id="shiftTimesModal" class="modal" style="display: none; position: fixed; z-index: 1000; left: 0; top: 0; width: 100%; height: 100%; background-color: rgba(0,0,0,0.5);">
                        <div class="modal-content" style="background-color: #fefefe; margin: 5% auto; padding: 0; border: none; border-radius: 10px; width: 90%; max-width: 1400px; max-height: 85vh; overflow: hidden; box-shadow: 0 10px 40px rgba(0,0,0,0.3);">
                            <div style="background: linear-gradient(135deg, #3498db, #2980b9); color: white; padding: 20px; border-radius: 10px 10px 0 0; display: flex; justify-content: space-between; align-items: center;">
                                <h2 style="margin: 0; font-size: 1.5em;"><i class="fas fa-clock"></i> Agent Shift Times</h2>
                                <span class="close" onclick="document.getElementById('shiftTimesModal').style.display='none'" style="color: white; font-size: 28px; font-weight: bold; cursor: pointer; line-height: 1;">&times;</span>
                            </div>
                            <div style="padding: 20px; max-height: calc(85vh - 80px); overflow-y: auto;" id="shiftTimesContent">
                                <!-- Content will be loaded here -->
                            </div>
                        </div>
                    </div>
                    
                    <!-- Agent Allocation Modal -->
                    <div id="agentModal" class="modal" style="display: none; position: fixed; z-index: 1000; left: 0; top: 0; width: 100%; height: 100%; background-color: rgba(0,0,0,0.5);">
                        <div class="modal-content" style="background-color: #fefefe; margin: 5% auto; padding: 0; border: none; border-radius: 10px; width: 90%; max-width: 1200px; max-height: 80vh; overflow: hidden; box-shadow: 0 10px 30px rgba(0,0,0,0.3);">
                            <div class="modal-header" style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 20px; display: flex; justify-content: space-between; align-items: center;">
                                <h2 style="margin: 0; font-size: 1.5em;" id="modalAgentName">Agent Allocation</h2>
                                <span class="close" style="color: white; font-size: 28px; font-weight: bold; cursor: pointer; transition: opacity 0.3s;">&times;</span>
                            </div>
                            <div class="modal-body" style="padding: 20px; max-height: 60vh; overflow-y: auto;">
                                <div id="modalContent">
                                    <div style="text-align: center; padding: 40px;">
                                        <i class="fas fa-spinner fa-spin" style="font-size: 2em; color: #667eea;"></i>
                                        <p style="margin-top: 15px; color: #666;">Loading agent allocation data...</p>
                                    </div>
                                </div>
                            </div>
                            <div class="modal-footer" style="background: #f8f9fa; padding: 15px 20px; border-top: 1px solid #e9ecef; display: flex; justify-content: space-between; align-items: center;">
                                <div id="modalStats" style="color: #666; font-size: 14px;"></div>
                                <div style="display: flex; gap: 10px;">
                                    <button id="downloadBtn" class="process-btn" style="background: linear-gradient(135deg, #27ae60, #2ecc71); padding: 8px 16px; border: none; border-radius: 5px; color: white; cursor: pointer; font-size: 14px;">
                                        <i class="fas fa-download"></i> Download Excel
                                    </button>
                                    <button class="close-btn process-btn" style="background: linear-gradient(135deg, #95a5a6, #7f8c8d); padding: 8px 16px; border: none; border-radius: 5px; color: white; cursor: pointer; font-size: 14px;">
                                        Close
                                    </button>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
                
                <!-- Agent Consolidation Tab -->
                <div id="agent-consolidation-tab" class="admin-tab-content">
                    <!-- Agent Files Consolidation -->
                    {% if all_agent_work_files %}
                    <div class="section">
                        <h3>📋 Agent Work Files Consolidation</h3>
                        <div style="background: #f8f9fa; padding: 20px; border-radius: 10px; margin-bottom: 20px;">
                            <h4>Available Agent Files:</h4>
                            {% for work_file in all_agent_work_files %}
                            <div style="border-bottom: 1px solid #dee2e6; padding: 10px 0; {% if loop.last %}border-bottom: none;{% endif %}">
                                <strong>{{ work_file.agent.name }}</strong> - {{ work_file.filename }}
                                <br>
                                <small style="color: #666;">
                                    Uploaded: {{ work_file.upload_date.strftime('%Y-%m-%d %H:%M') }}
                                    | Status: <span style="color: {% if work_file.status == 'uploaded' %}#28a745{% elif work_file.status == 'consolidated' %}#007bff{% else %}#6c757d{% endif %}">{{ work_file.status.title() }}</span>
                                </small>
                                {% if work_file.notes %}
                                <br>
                                <small style="color: #666;"><em>{{ work_file.notes }}</em></small>
                                {% endif %}
                            </div>
                            {% endfor %}
                        </div>
                        <form action="/consolidate_agent_files" method="post">
                            <button type="submit" class="process-btn" style="background: linear-gradient(135deg, #e74c3c, #c0392b);">
                                <i class="fas fa-compress-arrows-alt"></i> Consolidate All Agent Files
                            </button>
                        </form>
                    </div>
                    {% else %}
                    <div class="section">
                        <h3>📋 Agent Work Files</h3>
                        <div style="background: #f8f9fa; padding: 20px; border-radius: 10px;">
                            <p style="color: #666;">No agent work files uploaded yet.</p>
                        </div>
                    </div>
                    {% endif %}
                </div>
                
                <!-- System Settings Tab -->
                <div id="system-settings-tab" class="admin-tab-content">
                    <!-- Reset Section -->
                    <div class="section">
                        <h3>🔄 Reset Application</h3>
                        <p>Clear all uploaded files, agent consolidation files, and reset the application to start fresh.</p>
                        <form action="/reset_app" method="post" onsubmit="return confirm('Are you sure you want to reset the application? This will clear all uploaded files, agent consolidation files, and data.')">
                            <button type="submit" class="reset-btn">🗑️ Reset Application</button>
                        </form>
                    </div>
                </div>
            </div>

            <!-- Agent Panel -->
            <div id="agent-panel" class="panel">
           
          
                
                <div class="section">
                    <h3><i class="fas fa-upload"></i> Upload Work File</h3>
                    <div class="upload-card">
                        <form id="agentUploadForm" enctype="multipart/form-data">
                            <div class="form-group">
                            
                                <input type="file" name="file" id="agentFile" accept=".xlsx,.xls" required>
                            </div>
                        
                            <button type="submit" class="process-btn" id="agentUploadBtn">
                                <i class="fas fa-upload"></i> Upload Work File
                            </button>
                        </form>
                    </div>
                </div>
                
                {% if agent_work_files %}
                <div class="section">
                    <h3><i class="fas fa-history"></i> My Uploaded Files</h3>
                    <div style="background: #f8f9fa; padding: 20px; border-radius: 10px;">
                        {% for work_file in agent_work_files %}
                        <div style="border-bottom: 1px solid #dee2e6; padding: 10px 0; {% if loop.last %}border-bottom: none;{% endif %}">
                            <div style="display: flex; justify-content: space-between; align-items: center;">
                                <div>
                                    <strong>{{ work_file.filename }}</strong>
                                    <br>
                                    <small style="color: #666;">
                                        Uploaded: {{ work_file.upload_date.strftime('%Y-%m-%d %H:%M') }}
                                        | Status: <span style="color: {% if work_file.status == 'uploaded' %}#28a745{% elif work_file.status == 'consolidated' %}#007bff{% else %}#6c757d{% endif %}">{{ work_file.status.title() }}</span>
                                    </small>
                                    {% if work_file.notes %}
                                    <br>
                                    <small style="color: #666;"><em>{{ work_file.notes }}</em></small>
                                    {% endif %}
                                </div>
                            </div>
                        </div>
                        {% endfor %}
                    </div>
                </div>
                {% endif %}
                
            
            </div>
        </div>
    </div>

    <!-- Toast Container -->
    <div class="toast-container" id="toastContainer"></div>

    <script>
        function switchRole(role) {
            // Update button states
            document.querySelectorAll('.role-btn').forEach(btn => btn.classList.remove('active'));
            event.target.classList.add('active');
            
            // Show/hide panels
            document.querySelectorAll('.panel').forEach(panel => panel.classList.remove('active'));
            document.getElementById(role + '-panel').classList.add('active');
        }
        
        function switchAdminTab(tabName) {
            // Update tab button states
            document.querySelectorAll('.admin-tab-btn').forEach(btn => btn.classList.remove('active'));
            event.target.classList.add('active');
            
            // Show/hide tab content
            document.querySelectorAll('.admin-tab-content').forEach(tab => tab.classList.remove('active'));
            document.getElementById(tabName + '-tab').classList.add('active');
        }
        
        // Toast Notification Functions
        function showToast(type, title, message, duration = 5000) {
            const container = document.getElementById('toastContainer');
            const toast = document.createElement('div');
            toast.className = `toast ${type}`;
            
            const icons = {
                success: 'fas fa-check-circle',
                error: 'fas fa-exclamation-circle',
                warning: 'fas fa-exclamation-triangle',
                info: 'fas fa-info-circle'
            };
            
            toast.innerHTML = `
                <i class="toast-icon ${icons[type]}"></i>
                <div class="toast-content">
                    <div class="toast-title">${title}</div>
                    <div class="toast-message">${message}</div>
                </div>
                <button class="toast-close" onclick="closeToast(this)">&times;</button>
            `;
            
            container.appendChild(toast);
            
            // Trigger animation
            setTimeout(() => toast.classList.add('show'), 100);
            
            // Auto remove
            setTimeout(() => {
                if (toast.parentNode) {
                    closeToast(toast.querySelector('.toast-close'));
                }
            }, duration);
        }
        
        function closeToast(button) {
            const toast = button.closest('.toast');
            toast.classList.remove('show');
            setTimeout(() => {
                if (toast.parentNode) {
                    toast.parentNode.removeChild(toast);
                }
            }, 300);
        }
        
        // Global toast functions for easy access
        window.showSuccessToast = (title, message) => showToast('success', title, message);
        window.showErrorToast = (title, message) => showToast('error', title, message);
        window.showWarningToast = (title, message) => showToast('warning', title, message);
        window.showInfoToast = (title, message) => showToast('info', title, message);
        
        // Auto-switch to appropriate panel based on user role
        document.addEventListener('DOMContentLoaded', function() {
            const userRole = '{{ session.user_role }}';
            if (userRole === 'agent') {
                // For agents, show agent panel and hide role selector
                document.querySelectorAll('.panel').forEach(panel => panel.classList.remove('active'));
                document.getElementById('agent-panel').classList.add('active');
                const roleSelector = document.querySelector('.role-selector');
                if (roleSelector) {
                    roleSelector.style.display = 'none';
                }
            }
            
            // Note: Flash messages are handled by the server-side template rendering
            // Toast notifications are only used for file uploads via JavaScript
        });
        
        function switchPriorityTab(priority) {
            // Update tab button states
            document.querySelectorAll('.tab-button').forEach(btn => btn.classList.remove('active'));
            document.getElementById(priority + '-priority-tab').classList.add('active');
            
            // Show/hide panels
            document.querySelectorAll('.priority-panel').forEach(panel => {
                panel.style.display = 'none';
            });
            
            const targetPanel = document.getElementById(priority + '-priority-panel');
            if (targetPanel) {
                targetPanel.style.display = 'block';
            }
            
            // Load dates for the selected priority panel and refresh displays
            if (priority === 'first') {
                loadAppointmentDates(); // Refresh First Priority display
            } else if (priority === 'second') {
                loadAppointmentDatesSecond(); // Refresh Second Priority display
            } else if (priority === 'third') {
                updateThirdPriorityInfo();
                loadReceiveDateCheckboxes(); // Load receive date checkboxes
            }
        }

        // Form submission with loading states and toast notifications
        const allocationForm = document.getElementById('allocation-form');
        if (allocationForm) {
            allocationForm.addEventListener('submit', function(e) {
                e.preventDefault(); // Prevent default form submission
                
                const btn = document.getElementById('allocation-btn');
                const fileInput = document.getElementById('allocation_file');
                
                if (!fileInput.files[0]) {
                    showErrorToast('Upload Error', 'Please select a file to upload');
                    return;
                }
                
                if (btn) {
                    btn.disabled = true;
                    btn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Uploading...';
                }
                
                const formData = new FormData();
                formData.append('file', fileInput.files[0]);
                
                fetch('/upload_allocation', {
                    method: 'POST',
                    body: formData
                })
                .then(response => {
                    if (!response.ok) {
                        throw new Error(`HTTP error! status: ${response.status}`);
                    }
                    return response.text();
                })
                .then(() => {
                    showSuccessToast('Upload Successful', 'Allocation file uploaded successfully!');
                    // Reset form
                    allocationForm.reset();
                    // Don't show loader yet - wait until both files are uploaded
                    // Reload page to show updated status
                    setTimeout(() => {
                        window.location.reload();
                    }, 1500);
                })
                    .catch(error => {
                        showErrorToast('Upload Failed', 'Error uploading allocation file. Please try again.');
                    })
                .finally(() => {
                    if (btn) {
                        btn.disabled = false;
                        btn.innerHTML = '<i class="fas fa-upload"></i> Upload Allocation File';
                    }
                });
            });
        }

        const dataForm = document.getElementById('data-form');
        if (dataForm) {
            dataForm.addEventListener('submit', function(e) {
                e.preventDefault(); // Prevent default form submission
                
                const btn = document.getElementById('data-btn');
                const fileInput = document.getElementById('data_file');
                
                if (!fileInput.files[0]) {
                    showErrorToast('Upload Error', 'Please select a file to upload');
                    return;
                }
                
                if (btn) {
                    btn.disabled = true;
                    btn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Uploading...';
                }
                
                const formData = new FormData();
                formData.append('file', fileInput.files[0]);
                
                fetch('/upload_data', {
                    method: 'POST',
                    body: formData
                })
                .then(response => {
                    if (!response.ok) {
                        throw new Error(`HTTP error! status: ${response.status}`);
                    }
                    return response.text();
                })
                .then(() => {
                    showSuccessToast('Upload Successful', 'Data file uploaded successfully!');
                    // Reset form
                    dataForm.reset();
                    // Mark that files were just uploaded (both files should be present now)
                    sessionStorage.setItem('filesJustUploaded', 'true');
                    // Show loader before reloading
                    showLoader();
                    // Reload page to show updated status
                    setTimeout(() => {
                        window.location.reload();
                    }, 1500);
                })
                    .catch(error => {
                        showErrorToast('Upload Failed', 'Error uploading data file. Please try again.');
                    })
                .finally(() => {
                    if (btn) {
                        btn.disabled = false;
                        btn.innerHTML = '<i class="fas fa-upload"></i> Upload Data File';
                    }
                });
            });
        }

        const processForm = document.getElementById('process-form');
        if (processForm) {
            processForm.addEventListener('submit', function(e) {
                e.preventDefault();
                processFiles();
            });
        }
        
        // Agent upload form handler
        const agentUploadForm = document.getElementById('agentUploadForm');
        if (agentUploadForm) {
            agentUploadForm.addEventListener('submit', function(e) {
                e.preventDefault();
                uploadAgentWorkFile();
            });
        }
        
        // Populate date fields when page loads
        document.addEventListener('DOMContentLoaded', function() {
            // Only load appointment dates if files have been uploaded
            // Check if data file container exists and has content before showing loader
            const calendarContainer = document.getElementById('calendar_container');
            if (calendarContainer) {
                // Try to load appointment dates, but don't show loader on initial page load
                // Loader will only show if files were just uploaded (handled in upload handlers)
                loadAppointmentDatesWithoutLoader();
            }
        });
        
        // Version of loadAppointmentDates that checks if loader should be shown (for initial page load after file upload)
        function loadAppointmentDatesWithoutLoader() {
            const calendarContainer = document.getElementById('calendar_container');
            if (!calendarContainer) return;
            
            // Check if loader was shown before page reload (e.g., after data file upload)
            // If loader overlay is visible or was just uploaded, we'll show it when dates are successfully loaded
            const wasJustUploaded = sessionStorage.getItem('filesJustUploaded') === 'true';
            sessionStorage.removeItem('filesJustUploaded');
            
            // Show loading message in the container
            calendarContainer.innerHTML = '<p style="color: #666; font-style: italic; text-align: center; padding: 20px;">Checking for uploaded files...</p>';
            
            // Fetch appointment dates from server
            fetch('/get_appointment_dates')
                .then(response => {
                    return response.json();
                })
                .then(data => {
                    if (data.error) {
                        calendarContainer.innerHTML = `<p style="color: #666; font-style: italic; text-align: center; padding: 20px;">${data.error}</p>`;
                        // Hide loader if there's an error
                        if (isLoaderVisible()) {
                            hideLoader();
                        }
                        return;
                    }
                    
                    const dates = data.appointment_dates;
                    const datesWithCounts = data.appointment_dates_with_counts;
                    const columnName = data.column_name;
                    
                    if (!dates || dates.length === 0) {
                        calendarContainer.innerHTML = '<p style="color: #666; font-style: italic; text-align: center; padding: 20px;">No appointment dates found in the file.</p>';
                        // Hide loader if no dates found
                        if (isLoaderVisible()) {
                            hideLoader();
                        }
                        return;
                    }
                    
                    // If both files exist and we have dates, show loader (if files were just uploaded)
                    if (wasJustUploaded && dates && dates.length > 0) {
                        showLoader();
                    }
                    
                    // Store appointment dates
                    appointmentDates = new Set(dates);
                    // Directly show checkbox list (no calendar view)
                    // Loader will be hidden in showFallbackDateList after dates are displayed
                    showFallbackDateList(datesWithCounts, columnName);
                    updateSelectedDatesInfo();
                })
                .catch(error => {
                    calendarContainer.innerHTML = '<p style="color: #666; font-style: italic; text-align: center; padding: 20px;">No data file uploaded yet.</p>';
                    // Hide loader on error
                    if (isLoaderVisible()) {
                        hideLoader();
                    }
                });
        }
        
        
        // Global variables for calendar
        let currentDate = new Date();
        let appointmentDates = new Set();
        let selectedDates = new Set();
        let selectedSecondDates = new Set();
        
        // Store receive date selections per appointment date
        let receiveDateSelections = new Map(); // appointmentDate -> Set of selected receive dates
        
        // Loader functions
        function showLoader() {
            const loader = document.getElementById('loader-overlay');
            if (loader) {
                loader.classList.add('show');
            }
        }
        
        function hideLoader() {
            const loader = document.getElementById('loader-overlay');
            if (loader) {
                loader.classList.remove('show');
            }
        }
        
        function isLoaderVisible() {
            const loader = document.getElementById('loader-overlay');
            return loader && loader.classList.contains('show');
        }
        
        function loadAppointmentDates() {
            const calendarContainer = document.getElementById('calendar_container');
            if (!calendarContainer) return;
            
            // Show loader when starting to load (assuming both files exist when this is called manually)
            showLoader();
            
            // Always try to load appointment dates (file might be uploaded via form submission)
            calendarContainer.innerHTML = '<p style="color: #666; font-style: italic; text-align: center; padding: 20px;">Loading appointment dates...</p>';
            
            // Fetch appointment dates from server
            fetch('/get_appointment_dates')
                .then(response => {
                    return response.json();
                })
                .then(data => {
                    if (data.error) {
                        calendarContainer.innerHTML = `<p style="color: #e74c3c; text-align: center; padding: 20px;">Error: ${data.error}</p>`;
                        // Hide loader if there's an error (likely files not uploaded yet)
                        hideLoader();
                        return;
                    }
                    
                    const dates = data.appointment_dates;
                    const datesWithCounts = data.appointment_dates_with_counts;
                    const columnName = data.column_name;
                    
                    if (!dates || dates.length === 0) {
                        calendarContainer.innerHTML = '<p style="color: #666; font-style: italic; text-align: center; padding: 20px;">No appointment dates found in the file.</p>';
                        // Hide loader if no dates found
                        hideLoader();
                        return;
                    }
                    
                    // Store appointment dates
                    appointmentDates = new Set(dates);
                    // Directly show checkbox list (no calendar view)
                    // Loader will be hidden in showFallbackDateList after dates are displayed
                    showFallbackDateList(datesWithCounts, columnName);
                    updateSelectedDatesInfo();
                })
                .catch(error => {
                    // Hide loader on error
                    hideLoader();
                    calendarContainer.innerHTML = `<p style="color: #e74c3c; text-align: center; padding: 20px;">Error loading appointment dates: ${error.message}</p>`;
                });
        }
        
        function loadAppointmentDatesSecond() {
            const calendarContainer = document.getElementById('calendar_container_second');
            if (!calendarContainer) return;
            
            // Show loader when starting to load appointment dates
            showLoader();
            
            // Always try to load appointment dates (file might be uploaded via form submission)
            calendarContainer.innerHTML = '<p style="color: #666; font-style: italic; text-align: center; padding: 20px;">Loading appointment dates...</p>';
            
            // Fetch appointment dates from server
            fetch('/get_appointment_dates')
                .then(response => {
                    return response.json();
                })
                .then(data => {
                    if (data.error) {
                        calendarContainer.innerHTML = `<p style="color: #e74c3c; text-align: center; padding: 20px;">Error: ${data.error}</p>`;
                        hideLoader();
                        return;
                    }
                    
                    const dates = data.appointment_dates;
                    const datesWithCounts = data.appointment_dates_with_counts;
                    const columnName = data.column_name;
                    
                    if (!dates || dates.length === 0) {
                        calendarContainer.innerHTML = '<p style="color: #666; font-style: italic; text-align: center; padding: 20px;">No appointment dates found in the file.</p>';
                        hideLoader();
                        return;
                    }
                    
                    
                    // Store appointment dates
                    appointmentDates = new Set(dates);
                    // Directly show checkbox list (no calendar view)
                    // Loader will be hidden in showFallbackDateListSecond after dates are displayed
                    showFallbackDateListSecond(datesWithCounts, columnName);
                    updateSelectedDatesInfoSecond();
                })
                .catch(error => {
                    // Hide loader on error
                    hideLoader();
                    calendarContainer.innerHTML = `<p style="color: #e74c3c; text-align: center; padding: 20px;">Error loading appointment dates: ${error.message}</p>`;
                });
        }
        
        function loadReceiveDateCheckboxes() {
            const appointmentReceiveDatesContainer = document.getElementById('appointment_receive_dates_container');
            if (!appointmentReceiveDatesContainer) return;
            
            // Get selected appointment dates
            const selectedAppointmentDates = getSelectedAppointmentDates();
            
            if (selectedAppointmentDates.length === 0) {
                appointmentReceiveDatesContainer.innerHTML = '<p style="color: #666; font-style: italic; text-align: center; padding: 20px;">No appointment dates selected.</p>';
                return;
            }
            
            // Create individual panels for each appointment date
            let html = '';
            selectedAppointmentDates.forEach((appointmentDate, appointmentIndex) => {
                const appointmentDateObj = new Date(appointmentDate);
                const appointmentFormatted = appointmentDateObj.toLocaleDateString('en-US', { 
                    year: 'numeric', 
                    month: 'short', 
                    day: 'numeric',
                    weekday: 'short'
                });
                
                html += `
                    <div class="appointment-receive-panel" style="margin-bottom: 20px; padding: 15px; background: white; border: 1px solid #e9ecef; border-radius: 8px;">
                        <h5 style="margin: 0 0 10px 0; color: #2c5aa0; font-size: 14px;">
                            <i class="fas fa-calendar"></i> ${appointmentFormatted}
                        </h5>
                        <div id="receive_dates_${appointmentDate.replace(/-/g, '_')}" style="display: flex; flex-wrap: wrap; gap: 8px; margin-bottom: 10px;">
                            <p style="color: #666; font-style: italic; text-align: center; padding: 10px; width: 100%;">Loading receive dates...</p>
                        </div>
                        <div id="receive_info_${appointmentDate.replace(/-/g, '_')}" style="background: #f8f9fa; padding: 8px; border-radius: 5px; border: 1px solid #e9ecef; font-size: 12px;">
                            <strong>Selected:</strong> <span id="receive_count_${appointmentDate.replace(/-/g, '_')}">0</span> receive dates
                        </div>
                    </div>
                `;
            });
            
            appointmentReceiveDatesContainer.innerHTML = html;
            
            // Load receive dates for each appointment date
            selectedAppointmentDates.forEach((appointmentDate, appointmentIndex) => {
                loadReceiveDatesForAppointment(appointmentDate);
            });
        }
        
        function loadReceiveDatesForAppointment(appointmentDate) {
            const containerId = `receive_dates_${appointmentDate.replace(/-/g, '_')}`;
            const container = document.getElementById(containerId);
            if (!container) return;
            
            // Build query parameters for this specific appointment date
            const url = `/get_receive_dates?appointment_dates=${appointmentDate}`;
            
            // Fetch receive dates for this specific appointment date
            fetch(url)
                .then(response => response.json())
                .then(data => {
                    if (data.error) {
                        container.innerHTML = `<p style="color: #e74c3c; text-align: center; padding: 10px;">Error: ${data.error}</p>`;
                        return;
                    }
                    
                    const dates = data.receive_dates;
                    
                    if (!dates || dates.length === 0) {
                        container.innerHTML = '<p style="color: #666; font-style: italic; text-align: center; padding: 10px;">No receive dates found for this appointment date.</p>';
                        return;
                    }
                    
                    // Get saved selections for this appointment date
                    const savedSelections = receiveDateSelections.get(appointmentDate) || new Set();
                    
                    // Display receive dates as checkboxes with saved selections
                    let html = '';
                    dates.forEach((date, index) => {
                        const dateObj = new Date(date);
                        const dayName = dateObj.toLocaleDateString('en-US', { weekday: 'short' });
                        const formattedDate = dateObj.toLocaleDateString('en-US', { 
                            year: 'numeric', 
                            month: 'short', 
                            day: 'numeric' 
                        });
                        
                        // Check if this receive date should be selected based on saved selections
                        // If no saved selections exist, default to all selected
                        const isSelected = savedSelections.size === 0 ? true : savedSelections.has(date);
                        const uniqueId = `receive_${appointmentDate.replace(/-/g, '_')}_${index}`;
                        
                        html += `
                            <label style="display: flex; align-items: center; padding: 6px 10px; border: 1px solid #ddd; border-radius: 4px; background: white; cursor: pointer; transition: all 0.3s; min-width: 120px; font-size: 12px;" 
                                   onclick="toggleReceiveDateForAppointment('${appointmentDate}', '${date}', ${index})">
                                <input type="checkbox" id="${uniqueId}" data-date="${date}" ${isSelected ? 'checked' : ''} style="margin-right: 6px; transform: scale(0.9);">
                                <div>
                                    <div style="font-weight: bold; font-size: 12px;">${formattedDate}</div>
                                    <div style="color: #666; font-size: 10px;">${dayName}</div>
                                </div>
                            </label>
                        `;
                    });
                    
                    container.innerHTML = html;
                    
                    // Update receive date info for this appointment date
                    updateReceiveDateInfoForAppointment(appointmentDate);
                })
                .catch(error => {
                    container.innerHTML = `<p style="color: #e74c3c; text-align: center; padding: 10px;">Error loading receive dates: ${error.message}</p>`;
                });
        }
        
        function toggleReceiveDateForAppointment(appointmentDate, dateStr, index) {
            const uniqueId = `receive_${appointmentDate.replace(/-/g, '_')}_${index}`;
            const checkbox = document.getElementById(uniqueId);
            if (!checkbox) return;
            
            checkbox.checked = !checkbox.checked;
            
            // Update the visual state
            const container = checkbox.closest('label');
            if (checkbox.checked) {
                container.style.background = '#e3f2fd';
                container.style.borderColor = '#2196f3';
            } else {
                container.style.background = 'white';
                container.style.borderColor = '#ddd';
            }
            
            // Save the receive date selection for this specific appointment date
            saveReceiveDateSelectionForAppointment(appointmentDate);
            
            // Update receive date info for this appointment date
            updateReceiveDateInfoForAppointment(appointmentDate);
        }
        
        function saveReceiveDateSelectionForAppointment(appointmentDate) {
            // Get current receive date selections for this specific appointment date
            const containerId = `receive_dates_${appointmentDate.replace(/-/g, '_')}`;
            const container = document.getElementById(containerId);
            if (!container) return;
            
            const currentSelections = new Set();
            const checkboxes = container.querySelectorAll('input[type="checkbox"]');
            checkboxes.forEach(checkbox => {
                if (checkbox.checked) {
                    currentSelections.add(checkbox.dataset.date);
                }
            });
            
            // Save the selections for this appointment date
            receiveDateSelections.set(appointmentDate, currentSelections);
        }
        
        function updateReceiveDateInfoForAppointment(appointmentDate) {
            const countElement = document.getElementById(`receive_count_${appointmentDate.replace(/-/g, '_')}`);
            if (!countElement) return;
            
            const containerId = `receive_dates_${appointmentDate.replace(/-/g, '_')}`;
            const container = document.getElementById(containerId);
            if (!container) return;
            
            const checkboxes = container.querySelectorAll('input[type="checkbox"]');
            const selectedCount = Array.from(checkboxes).filter(cb => cb.checked).length;
            
            countElement.textContent = selectedCount;
        }
        
        function updateReceiveDateInfo() {
            const selectedCount = document.getElementById('receive_selected_count');
            const selectedText = document.getElementById('receive_selected_text');
            const selectedList = document.getElementById('receive_selected_list');
            
            if (!selectedCount || !selectedText || !selectedList) return;
            
            // Get all checked receive date checkboxes
            const checkboxes = document.querySelectorAll('input[id^="receive_checkbox_"]:checked');
            const selectedDates = Array.from(checkboxes).map(cb => cb.dataset.date);
            
            selectedCount.textContent = selectedDates.length;
            selectedText.textContent = selectedDates.length === 1 ? 'date selected' : 'dates selected';
            
            if (selectedDates.length > 0) {
                const formattedDates = selectedDates.map(date => {
                    const dateObj = new Date(date);
                    return dateObj.toLocaleDateString('en-US', { 
                        month: 'short', 
                        day: 'numeric' 
                    });
                });
                selectedList.textContent = formattedDates.join(', ');
            } else {
                selectedList.textContent = 'No receive dates selected';
            }
        }
        
        function initializeCalendar() {
            renderCalendar();
        }
        
        function renderCalendar() {
            const year = currentDate.getFullYear();
            const month = currentDate.getMonth();
            
            // Update header
            const monthYearElement = document.getElementById('current_month_year');
            if (monthYearElement) {
                monthYearElement.textContent = currentDate.toLocaleDateString('en-US', { 
                    year: 'numeric', 
                    month: 'long' 
                });
            }
            
            // Get first day of month and number of days
            const firstDay = new Date(year, month, 1);
            const lastDay = new Date(year, month + 1, 0);
            const daysInMonth = lastDay.getDate();
            const startingDayOfWeek = firstDay.getDay();
            
            // Day headers
            const dayHeaders = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];
            let calendarHTML = '';
            
            // Add day headers
            dayHeaders.forEach(day => {
                calendarHTML += `<div style="text-align: center; font-weight: bold; padding: 8px; background: #f8f9fa; border: 1px solid #dee2e6;">${day}</div>`;
            });
            
            // Add empty cells for days before month starts
            for (let i = 0; i < startingDayOfWeek; i++) {
                calendarHTML += `<div style="height: 40px; border: 1px solid #dee2e6; background: #f8f9fa;"></div>`;
            }
            
            // Add days of the month
            for (let day = 1; day <= daysInMonth; day++) {
                const dateStr = `${year}-${String(month + 1).padStart(2, '0')}-${String(day).padStart(2, '0')}`;
                const isAppointmentDate = appointmentDates.has(dateStr);
                const isSelected = selectedDates.has(dateStr);
                const isSelectedSecond = selectedSecondDates.has(dateStr);
                const isToday = isTodayDate(year, month, day);
                
                let cellClass = 'calendar-day';
                let cellStyle = 'height: 40px; border: 1px solid #dee2e6; display: flex; align-items: center; justify-content: center; cursor: pointer; position: relative;';
                
                if (isToday) {
                    cellStyle += ' background: #e3f2fd; font-weight: bold;';
                } else if (isAppointmentDate) {
                    cellStyle += ' background: #fff3e0;';
                } else {
                    cellStyle += ' background: #f8f9fa; color: #6c757d;';
                }
                
                if (isSelected) {
                    cellStyle += ' background: #4caf50; color: white; font-weight: bold;';
                } else if (isSelectedSecond) {
                    cellStyle += ' background: #f39c12; color: white; font-weight: bold;';
                }
                
                if (!isAppointmentDate) {
                    cellStyle += ' cursor: not-allowed; opacity: 0.5;';
                }
                
                calendarHTML += `
                    <div class="${cellClass}" 
                         data-date="${dateStr}" 
                         style="${cellStyle}"
                         onclick="${isAppointmentDate ? `toggleDate('${dateStr}')` : ''}">
                        ${day}
                        ${isAppointmentDate ? '<div style="position: absolute; top: 2px; right: 2px; width: 6px; height: 6px; background: #ff9800; border-radius: 50%;"></div>' : ''}
                    </div>
                `;
            }
            
            // Update calendar grid
            const calendarGrid = document.getElementById('calendar_grid');
            if (calendarGrid) {
                calendarGrid.innerHTML = calendarHTML;
            }
            
            // Update selected dates info
            updateSelectedDatesInfo();
        }
        
        function isTodayDate(year, month, day) {
            const today = new Date();
            return today.getFullYear() === year && 
                   today.getMonth() === month && 
                   today.getDate() === day;
        }
        
        function getSelectedAppointmentDates() {
            return Array.from(selectedDates);
        }
        
        
        function toggleDate(dateStr) {
            if (!appointmentDates.has(dateStr)) return;
            
            if (selectedDates.has(dateStr)) {
                selectedDates.delete(dateStr);
            } else {
                // Remove from Second Priority if it was selected there
                if (selectedSecondDates.has(dateStr)) {
                    selectedSecondDates.delete(dateStr);
                    updateSelectedDatesInfoSecond();
                    syncFallbackCheckboxesSecond();
                }
                selectedDates.add(dateStr);
            }
            
            renderCalendar();
            syncFallbackCheckboxes();
            updateThirdPriorityInfo(); // Update Third Priority info when First Priority changes
            
            // Show/hide receive date panel based on whether any appointment dates are selected
            const receiveDatePanel = document.getElementById('receive-date-panel');
            if (receiveDatePanel) {
                if (selectedDates.size > 0) {
                    receiveDatePanel.style.display = 'block';
                    // Always reload receive dates when appointment dates change
                    loadReceiveDateCheckboxes();
                } else {
                    receiveDatePanel.style.display = 'none';
                }
            }
        }
        
        function previousMonth() {
            currentDate.setMonth(currentDate.getMonth() - 1);
            renderCalendar();
        }
        
        function nextMonth() {
            currentDate.setMonth(currentDate.getMonth() + 1);
            renderCalendar();
        }
        
        function updateSelectedDatesInfo() {
            const selectedCount = document.getElementById('selected_count');
            const selectedText = document.getElementById('selected_text');
            const selectedDatesList = document.getElementById('selected_dates_list');
            
            if (selectedCount) {
                selectedCount.textContent = selectedDates.size;
            }
            
            if (selectedText) {
                selectedText.textContent = selectedDates.size === 1 ? 'date selected' : 'dates selected';
            }
            
            if (selectedDatesList) {
                if (selectedDates.size === 0) {
                    selectedDatesList.textContent = 'No dates selected';
                } else {
                    const sortedDates = Array.from(selectedDates).sort();
                    const formattedDates = sortedDates.map(date => {
                        const dateObj = new Date(date);
                        return dateObj.toLocaleDateString('en-US', { 
                            month: 'short', 
                            day: 'numeric' 
                        });
                    });
                    selectedDatesList.textContent = formattedDates.join(', ');
                }
            }
            // Keep toggle button label in sync
            const btn = document.getElementById('toggle-select-btn');
            if (btn) {
                const total = appointmentDates ? appointmentDates.size : 0;
                if (selectedDates.size === total && total > 0) {
                    btn.textContent = 'Deselect All Dates';
                    btn.style.background = '#e74c3c';
                } else {
                    btn.textContent = 'Select All Dates';
                    btn.style.background = '#27ae60';
                }
            }
        }
        
        function showFallbackDateList(datesWithCounts, columnName) {
            const calendarContainer = document.getElementById('calendar_container');
            if (!calendarContainer) {
                // Hide loader even if container doesn't exist
                hideLoader();
                return;
            }
            
            try {
                // Ensure variables are initialized
                if (typeof selectedDates === 'undefined') {
                    selectedDates = new Set();
                }
                if (typeof selectedSecondDates === 'undefined') {
                    selectedSecondDates = new Set();
                }
            
            let html = `
                <div style="text-align: center; margin-bottom: 20px;">
                    <p>Click on dates to select them for First Priority:</p>
                </div>
                <div style="display: grid; grid-template-columns: repeat(auto-fill, minmax(200px, 1fr)); gap: 10px; max-height: 300px; overflow-y: auto;">
            `;
            
                datesWithCounts.forEach((dateData, index) => {
                    const date = dateData.date;
                    const rowCount = dateData.row_count || 0;
                const dateObj = new Date(date);
                const dayName = dateObj.toLocaleDateString('en-US', { weekday: 'long' });
                const formattedDate = dateObj.toLocaleDateString('en-US', { 
                    year: 'numeric', 
                    month: 'short', 
                    day: 'numeric' 
                });
                
                const isSelectedInFirst = selectedDates.has(date);
                const isSelectedInSecond = selectedSecondDates.has(date);
                const isDisabled = isSelectedInSecond;
                
                let itemStyle = 'display: flex; align-items: center; padding: 10px; border: 2px solid #e0e0e0; border-radius: 8px; background: #f9f9f9; cursor: pointer; transition: all 0.3s;';
                let textStyle = 'font-weight: bold; font-size: 16px;';
                let dayStyle = 'color: #666; font-size: 14px;';
                let countStyle = 'color: #666; font-size: 12px; margin-top: 2px;';
                
                if (isSelectedInFirst) {
                    itemStyle = 'display: flex; align-items: center; padding: 10px; border: 2px solid #4caf50; border-radius: 8px; background: #4caf50; color: white; cursor: pointer; transition: all 0.3s;';
                    textStyle = 'font-weight: bold; font-size: 16px; color: white;';
                    dayStyle = 'color: rgba(255,255,255,0.8); font-size: 14px;';
                    countStyle = 'color: rgba(255,255,255,0.9); font-size: 12px; margin-top: 2px;';
                } else if (isDisabled) {
                    itemStyle = 'display: flex; align-items: center; padding: 10px; border: 2px solid #f39c12; border-radius: 8px; background: #f39c12; color: white; cursor: not-allowed; opacity: 0.7; transition: all 0.3s;';
                    textStyle = 'font-weight: bold; font-size: 16px; color: white;';
                    dayStyle = 'color: rgba(255,255,255,0.8); font-size: 14px;';
                    countStyle = 'color: rgba(255,255,255,0.9); font-size: 12px; margin-top: 2px;';
                }
                
                html += `
                    <div style="${itemStyle}"
                         onclick="${isDisabled ? '' : `toggleDate('${date}')`}" 
                         id="date_${index}">
                        <input type="checkbox" id="checkbox_${index}" data-date="${date}" style="margin-right: 10px; transform: scale(1.2);" ${isDisabled ? 'disabled' : ''}>
                        <div>
                            <div style="${textStyle}">${formattedDate}${isDisabled ? ' (Second Priority)' : ''}</div>
                            <div style="${dayStyle}">${dayName}</div>
                                <div style="${countStyle}">${rowCount} rows</div>
                        </div>
                    </div>
                `;
            });
            
            html += '</div>';
            calendarContainer.innerHTML = html;
            // Sync checkboxes to current selection
            syncFallbackCheckboxes();
            } catch (error) {
                console.error('Error displaying dates (First Priority):', error);
                console.error('Error details:', error.message, error.stack);
                calendarContainer.innerHTML = '<p style="color: #e74c3c; text-align: center; padding: 20px;">Error displaying dates: ' + (error.message || 'Unknown error') + '. Please try again.</p>';
            } finally {
                // Always hide loader after attempting to display dates
                // Use setTimeout to ensure DOM updates complete
                setTimeout(function() {
                    hideLoader();
                }, 100);
            }
        }
        
        function showFallbackDateListSecond(datesWithCounts, columnName) {
            const calendarContainer = document.getElementById('calendar_container_second');
            if (!calendarContainer) {
                // Hide loader even if container doesn't exist
                hideLoader();
                return;
            }
            
            try {
                // Ensure variables are initialized
                if (typeof selectedDates === 'undefined') {
                    selectedDates = new Set();
                }
                if (typeof selectedSecondDates === 'undefined') {
                    selectedSecondDates = new Set();
                }
            
            let html = `
                <div style="text-align: center; margin-bottom: 20px;">
                    <p>Click on dates to select them for Second Priority:</p>
                </div>
                <div style="display: grid; grid-template-columns: repeat(auto-fill, minmax(200px, 1fr)); gap: 10px; max-height: 300px; overflow-y: auto;">
            `;
            
                datesWithCounts.forEach((dateData, index) => {
                    const date = dateData.date;
                    const rowCount = dateData.row_count || 0;
                const dateObj = new Date(date);
                const dayName = dateObj.toLocaleDateString('en-US', { weekday: 'long' });
                const formattedDate = dateObj.toLocaleDateString('en-US', { 
                    year: 'numeric', 
                    month: 'short', 
                    day: 'numeric' 
                });
                
                const isSelectedInFirst = selectedDates.has(date);
                const isSelectedInSecond = selectedSecondDates.has(date);
                const isDisabled = isSelectedInFirst;
                
                let itemStyle = 'display: flex; align-items: center; padding: 10px; border: 2px solid #e0e0e0; border-radius: 8px; background: #f9f9f9; cursor: pointer; transition: all 0.3s;';
                let textStyle = 'font-weight: bold; font-size: 16px;';
                let dayStyle = 'color: #666; font-size: 14px;';
                let countStyle = 'color: #666; font-size: 12px; margin-top: 2px;';
                
                if (isSelectedInSecond) {
                    itemStyle = 'display: flex; align-items: center; padding: 10px; border: 2px solid #f39c12; border-radius: 8px; background: #f39c12; color: white; cursor: pointer; transition: all 0.3s;';
                    textStyle = 'font-weight: bold; font-size: 16px; color: white;';
                    dayStyle = 'color: rgba(255,255,255,0.8); font-size: 14px;';
                    countStyle = 'color: rgba(255,255,255,0.9); font-size: 12px; margin-top: 2px;';
                } else if (isDisabled) {
                    itemStyle = 'display: flex; align-items: center; padding: 10px; border: 2px solid #4caf50; border-radius: 8px; background: #4caf50; color: white; cursor: not-allowed; opacity: 0.7; transition: all 0.3s;';
                    textStyle = 'font-weight: bold; font-size: 16px; color: white;';
                    dayStyle = 'color: rgba(255,255,255,0.8); font-size: 14px;';
                    countStyle = 'color: rgba(255,255,255,0.9); font-size: 12px; margin-top: 2px;';
                }
                
                html += `
                    <div style="${itemStyle}"
                         onclick="${isDisabled ? '' : `toggleDateSecond('${date}')`}" 
                         id="date_second_${index}">
                        <input type="checkbox" id="checkbox_second_${index}" data-date="${date}" style="margin-right: 10px; transform: scale(1.2);" ${isDisabled ? 'disabled' : ''}>
                        <div>
                            <div style="${textStyle}">${formattedDate}${isDisabled ? ' (First Priority)' : ''}</div>
                            <div style="${dayStyle}">${dayName}</div>
                            <div style="${countStyle}">${rowCount} rows</div>
                        </div>
                    </div>
                `;
            });
            
            html += '</div>';
            calendarContainer.innerHTML = html;
            // Sync checkboxes to current selection
            syncFallbackCheckboxesSecond();
            } catch (error) {
                console.error('Error displaying dates (Second Priority):', error);
                console.error('Error details:', error.message, error.stack);
                calendarContainer.innerHTML = '<p style="color: #e74c3c; text-align: center; padding: 20px;">Error displaying dates: ' + (error.message || 'Unknown error') + '. Please try again.</p>';
            } finally {
                // Always hide loader after attempting to display dates
                // Use setTimeout to ensure DOM updates complete
                setTimeout(function() {
                    hideLoader();
                }, 100);
            }
        }
        
        function toggleSelectAllDates() {
            const btn = document.getElementById('toggle-select-btn');
            const total = appointmentDates ? appointmentDates.size : 0;
            const selected = selectedDates ? selectedDates.size : 0;
            const shouldSelectAll = selected < total;
            if (shouldSelectAll) {
                // Select all
                selectedDates = new Set();
                appointmentDates.forEach(d => selectedDates.add(d));
            } else {
                // Deselect all
                selectedDates.clear();
            }
            renderCalendar();
            updateSelectedDatesInfo();
            syncFallbackCheckboxes();
            // Update button label and style
            if (btn) {
                if (selectedDates.size === total && total > 0) {
                    btn.textContent = 'Deselect All Dates';
                    btn.style.background = '#e74c3c';
                } else {
                    btn.textContent = 'Select All Dates';
                    btn.style.background = '#27ae60';
                }
            }
        }

        function syncFallbackCheckboxes() {
            const checkboxes = document.querySelectorAll('#calendar_container input[type="checkbox"][data-date]');
            if (!checkboxes || checkboxes.length === 0) return;
            checkboxes.forEach(cb => {
                const d = cb.getAttribute('data-date');
                cb.checked = selectedDates.has(d);
            });
        }
        
        function toggleSelectAllSecondDates() {
            const btn = document.getElementById('toggle-select-second-btn');
            const total = appointmentDates ? appointmentDates.size : 0;
            const selected = selectedSecondDates ? selectedSecondDates.size : 0;
            const shouldSelectAll = selected < total;
            if (shouldSelectAll) {
                // Select all
                selectedSecondDates = new Set();
                appointmentDates.forEach(d => selectedSecondDates.add(d));
            } else {
                // Deselect all
                selectedSecondDates.clear();
            }
            updateSelectedDatesInfoSecond();
            syncFallbackCheckboxesSecond();
            // Update button label and style
            if (btn) {
                if (selectedSecondDates.size === total && total > 0) {
                    btn.textContent = 'Deselect All Dates';
                    btn.style.background = '#e74c3c';
                } else {
                    btn.textContent = 'Select All Dates';
                    btn.style.background = '#f39c12';
                }
            }
        }
        
        function toggleDateSecond(dateStr) {
            if (!appointmentDates.has(dateStr)) return;
            
            if (selectedSecondDates.has(dateStr)) {
                selectedSecondDates.delete(dateStr);
            } else {
                // Remove from First Priority if it was selected there
                if (selectedDates.has(dateStr)) {
                    selectedDates.delete(dateStr);
                    renderCalendar();
                    syncFallbackCheckboxes();
                }
                selectedSecondDates.add(dateStr);
            }
            
            updateSelectedDatesInfoSecond();
            syncFallbackCheckboxesSecond();
            updateThirdPriorityInfo(); // Update Third Priority info when Second Priority changes
        }
        
        function updateSelectedDatesInfoSecond() {
            const selectedCount = document.getElementById('selected_count_second');
            const selectedText = document.getElementById('selected_text_second');
            const selectedDatesList = document.getElementById('selected_dates_list_second');
            
            if (selectedCount) {
                selectedCount.textContent = selectedSecondDates.size;
            }
            
            if (selectedText) {
                selectedText.textContent = selectedSecondDates.size === 1 ? 'date selected' : 'dates selected';
            }
            
            if (selectedDatesList) {
                if (selectedSecondDates.size === 0) {
                    selectedDatesList.textContent = 'No dates selected';
                } else {
                    const sortedDates = Array.from(selectedSecondDates).sort();
                    const formattedDates = sortedDates.map(date => {
                        const dateObj = new Date(date);
                        return dateObj.toLocaleDateString('en-US', { 
                            month: 'short', 
                            day: 'numeric' 
                        });
                    });
                    selectedDatesList.textContent = formattedDates.join(', ');
                }
            }
            // Keep toggle button label in sync
            const btn = document.getElementById('toggle-select-second-btn');
            if (btn) {
                const total = appointmentDates ? appointmentDates.size : 0;
                if (selectedSecondDates.size === total && total > 0) {
                    btn.textContent = 'Deselect All Dates';
                    btn.style.background = '#e74c3c';
                } else {
                    btn.textContent = 'Select All Dates';
                    btn.style.background = '#f39c12';
                }
            }
        }
        
        function syncFallbackCheckboxesSecond() {
            const checkboxes = document.querySelectorAll('#calendar_container_second input[type="checkbox"][data-date]');
            if (!checkboxes || checkboxes.length === 0) return;
            checkboxes.forEach(cb => {
                const d = cb.getAttribute('data-date');
                cb.checked = selectedSecondDates.has(d);
            });
        }
        
        function updateThirdPriorityInfo() {
            // Calculate remaining dates that will be Third Priority
            const allDates = new Set(appointmentDates);
            const firstPriorityDates = new Set(selectedDates);
            const secondPriorityDates = new Set(selectedSecondDates);
            
            // Find dates that are not in First or Second Priority
            const thirdPriorityDates = new Set();
            allDates.forEach(date => {
                if (!firstPriorityDates.has(date) && !secondPriorityDates.has(date)) {
                    thirdPriorityDates.add(date);
                }
            });
            
            // Update the display
            const thirdPriorityCount = document.getElementById('third_priority_count');
            const thirdPriorityDatesList = document.getElementById('third_priority_dates_list');
            
            if (thirdPriorityCount) {
                thirdPriorityCount.textContent = thirdPriorityDates.size;
            }
            
            if (thirdPriorityDatesList) {
                if (thirdPriorityDates.size === 0) {
                    thirdPriorityDatesList.textContent = 'No remaining dates (all dates are assigned to First or Second Priority)';
                } else {
                    const sortedDates = Array.from(thirdPriorityDates).sort();
                    const formattedDates = sortedDates.map(date => {
                        const dateObj = new Date(date);
                        return dateObj.toLocaleDateString('en-US', { 
                            month: 'short', 
                            day: 'numeric' 
                        });
                    });
                    thirdPriorityDatesList.textContent = formattedDates.join(', ');
                }
            }
        }
        
        function selectBusinessDays() {
            // Clear all first
            clearAllDates();
            // This function is now simplified since we removed the business day checkboxes
            // Users can select dates directly from the calendar
        }
        
        
        function getNextBusinessDay(startDate, n) {
            let currentDate = new Date(startDate);
            let businessDaysCount = 0;
            
            while (businessDaysCount < n) {
                currentDate.setDate(currentDate.getDate() + 1);
                // Check if it's a weekday (Monday=1, Sunday=0)
                if (currentDate.getDay() >= 1 && currentDate.getDay() <= 5) {
                    businessDaysCount++;
                }
            }
            
            return currentDate.toISOString().split('T')[0];
        }
        
        function processFiles() {
            // Add selected calendar dates to form
            const form = document.getElementById('process-form');
            if (form) {
                // Remove existing hidden inputs for appointment dates
                const existingFirstInputs = form.querySelectorAll('input[name="appointment_dates"]');
                existingFirstInputs.forEach(input => input.remove());
                
                const existingSecondInputs = form.querySelectorAll('input[name="appointment_dates_second"]');
                existingSecondInputs.forEach(input => input.remove());
                
                // Add First Priority selected dates as hidden inputs
                selectedDates.forEach(date => {
                    const input = document.createElement('input');
                    input.type = 'hidden';
                    input.name = 'appointment_dates';
                    input.value = date;
                    form.appendChild(input);
                });
                
                // Add Second Priority selected dates as hidden inputs
                selectedSecondDates.forEach(date => {
                    const input = document.createElement('input');
                    input.type = 'hidden';
                    input.name = 'appointment_dates_second';
                    input.value = date;
                    form.appendChild(input);
                });
                
                // Add receive dates as hidden inputs from all appointment dates
                selectedDates.forEach(appointmentDate => {
                    const containerId = `receive_dates_${appointmentDate.replace(/-/g, '_')}`;
                    const container = document.getElementById(containerId);
                    if (container) {
                        const receiveCheckboxes = container.querySelectorAll('input[type="checkbox"]:checked');
                        receiveCheckboxes.forEach(checkbox => {
                            const input = document.createElement('input');
                            input.type = 'hidden';
                            input.name = 'receive_dates';
                            input.value = checkbox.dataset.date;
                            form.appendChild(input);
                        });
                    }
                });
                
                // If no dates selected for First Priority, add all appointment dates as fallback
                if (selectedDates.size === 0) {
                    appointmentDates.forEach(date => {
                        const input = document.createElement('input');
                        input.type = 'hidden';
                        input.name = 'appointment_dates';
                        input.value = date;
                        form.appendChild(input);
                    });
                }
                
                // Add debug inputs to see what's being sent
                const debugFirstInput = document.createElement('input');
                debugFirstInput.type = 'hidden';
                debugFirstInput.name = 'debug_selected_count';
                debugFirstInput.value = selectedDates.size;
                form.appendChild(debugFirstInput);
                
                const debugSecondInput = document.createElement('input');
                debugSecondInput.type = 'hidden';
                debugSecondInput.name = 'debug_selected_count_second';
                debugSecondInput.value = selectedSecondDates.size;
                form.appendChild(debugSecondInput);
            }
            
            const processingStatus = document.getElementById('processing-status');
            const processBtn = document.getElementById('process-btn');
            
            if (processingStatus) {
                processingStatus.style.display = 'flex';
            }
            if (processBtn) {
                processBtn.disabled = true;
                processBtn.textContent = 'Processing...';
            }
            
            // Simulate progress updates
            let progress = 0;
            const progressBar = document.getElementById('progress-bar');
            const progressText = document.getElementById('progress-text');
            
            if (!progressBar || !progressText) {
                return;
            }
            
            const progressInterval = setInterval(() => {
                progress += Math.random() * 15;
                if (progress > 90) progress = 90;
                
                progressBar.style.width = progress + '%';
                progressBar.textContent = Math.round(progress) + '%';
                
                if (progress < 30) {
                    progressText.textContent = 'Reading files...';
                } else if (progress < 60) {
                    progressText.textContent = 'Analyzing appointment dates...';
                } else if (progress < 90) {
                    progressText.textContent = 'Assigning priorities...';
                } else {
                    progressText.textContent = 'Finalizing results...';
                }
            }, 200);
            
            // Make AJAX request with form body
            const formData = new FormData(form);
            fetch('/process_files', {
                method: 'POST',
                body: new URLSearchParams(formData)
            })
            .then(response => response.text())
            .then(html => {
                clearInterval(progressInterval);
                if (progressBar) {
                    progressBar.style.width = '100%';
                    progressBar.textContent = '100%';
                }
                if (progressText) {
                    progressText.textContent = 'Processing complete!';
                }
                
                setTimeout(() => {
                    document.body.innerHTML = html;
                }, 1000);
            })
            .catch(error => {
                clearInterval(progressInterval);
                if (progressText) {
                    progressText.textContent = 'Error: ' + error.message;
                }
            });
        }
        
        function uploadAgentWorkFile() {
            const form = document.getElementById('agentUploadForm');
            const fileInput = document.getElementById('agentFile');
            const notesInput = document.getElementById('agentNotes');
            const uploadBtn = document.getElementById('agentUploadBtn');
            
            if (!fileInput.files[0]) {
                showErrorToast('Upload Error', 'Please select a file to upload');
                return;
            }
            
            // Show loading state
            if (uploadBtn) {
                uploadBtn.disabled = true;
                uploadBtn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Uploading...';
            }
            
            const formData = new FormData();
            formData.append('file', fileInput.files[0]);
            formData.append('notes', notesInput ? notesInput.value || '' : '');
            
            fetch('/upload_work_file', {
                method: 'POST',
                body: formData
            })
            .then(response => {
                if (!response.ok) {
                    throw new Error(`HTTP error! status: ${response.status}`);
                }
                return response.json();
            })
            .then(data => {
                if (data.success) {
                    showSuccessToast('Upload Successful', data.message);
                    // Reset form
                    if (form) form.reset();
                    // Reload page to show updated file list after a short delay
                    setTimeout(() => {
                        window.location.reload();
                    }, 2000);
                } else {
                    showErrorToast('Upload Failed', data.message || 'Upload failed');
                }
            })
            .catch(error => {
                showErrorToast('Upload Error', 'Error uploading file. Please try again.');
            })
            .finally(() => {
                // Reset button state
                if (uploadBtn) {
                    uploadBtn.disabled = false;
                    uploadBtn.innerHTML = '<i class="fas fa-upload"></i> Upload Work File';
                }
            });
        }
        
        // Initialize agent table when page loads
        document.addEventListener('DOMContentLoaded', function() {
            // Update serial numbers for all agent rows
            const agentRows = document.querySelectorAll('.agent-row');
            agentRows.forEach((row, index) => {
                const srNoCell = row.querySelector('td:first-child');
                if (srNoCell) {
                    srNoCell.textContent = index + 1;
                }
            });
        });
        
        function approveAllocation(agentName) {
            if (confirm(`Are you sure you want to approve the allocation for ${agentName}? This will send an email with the allocated data.`)) {
                // Add visual feedback
                const button = event.target;
                const originalText = button.innerHTML;
                button.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Sending Email...';
                button.disabled = true;
                
                // Send approval email
                fetch('/send_approval_email', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                    },
                    body: JSON.stringify({
                        agent_name: agentName
                    })
                })
                .then(response => response.json())
                .then(data => {
                    if (data.success) {
                        button.innerHTML = '<i class="fas fa-check"></i> Email Sent';
                        button.style.background = 'linear-gradient(135deg, #27ae60, #2ecc71)';
                        showSuccessToast('Email Sent', data.message);
                    } else {
                        button.innerHTML = originalText;
                        button.disabled = false;
                        showErrorToast('Email Failed', data.message);
                    }
                })
                .catch(error => {
                    button.innerHTML = originalText;
                    button.disabled = false;
                    showErrorToast('Email Error', `Error sending email: ${error.message}`);
                });
            }
        }
        
        function approveAllAllocations() {
            if (confirm('Are you sure you want to approve ALL allocations? This will send emails to all agents with their allocated data.')) {
                const button = document.getElementById('approve-all-btn');
                const originalText = button.innerHTML;
                button.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Processing...';
                button.disabled = true;
                
                // Send approval for all allocations
                fetch('/approve_all_allocations', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                    }
                })
                .then(response => response.json())
                .then(data => {
                    if (data.success) {
                        button.innerHTML = '<i class="fas fa-check-double"></i> All Approved';
                        button.style.background = 'linear-gradient(135deg, #27ae60, #2ecc71)';
                        showSuccessToast('All Allocations Approved', data.message);
                        
                        // Update individual approve buttons to show as sent
                        const approveButtons = document.querySelectorAll('.approve-btn');
                        approveButtons.forEach(btn => {
                            if (!btn.disabled) {
                                btn.innerHTML = '<i class="fas fa-check"></i> Email Sent';
                                btn.style.background = 'linear-gradient(135deg, #27ae60, #2ecc71)';
                                btn.disabled = true;
                            }
                        });
                    } else {
                        button.innerHTML = originalText;
                        button.disabled = false;
                        showErrorToast('Approval Failed', data.message);
                    }
                })
                .catch(error => {
                    button.innerHTML = originalText;
                    button.disabled = false;
                    showErrorToast('Error', `Error approving allocations: ${error.message}`);
                });
            }
        }
        
        function viewShiftTimes() {
            // Show modal
            const modal = document.getElementById('shiftTimesModal');
            if (!modal) {
                showErrorToast('Error', 'Shift times modal not found');
                return;
            }
            modal.style.display = 'block';
            
            // Show loading state
            const modalContent = document.getElementById('shiftTimesContent');
            modalContent.innerHTML = `
                <div style="text-align: center; padding: 40px;">
                    <i class="fas fa-spinner fa-spin" style="font-size: 2em; color: #667eea;"></i>
                    <p style="margin-top: 15px; color: #666;">Loading shift information...</p>
                </div>
            `;
            
            // Fetch shift times
            fetch('/view_shift_times', {
                method: 'GET',
                headers: {
                    'Content-Type': 'application/json',
                }
            })
            .then(response => response.json())
            .then(data => {
                if (data.success) {
                    let html = `<h3 style="margin-top: 0; color: #333;">⏰ Shift Times Overview (${data.total_agents} agents)</h3>`;
                    html += `<div style="overflow-x: auto; margin-top: 15px;">`;
                    html += `<table class="modal-table" style="width: 100%; border-collapse: collapse;">`;
                    html += `<thead><tr style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white;">`;
                    html += `<th style="padding: 12px; text-align: center;">Sr No</th>`;
                    html += `<th style="padding: 12px; text-align: left;">Agent Name</th>`;
                    html += `<th style="padding: 12px; text-align: left;">Email</th>`;
                    html += `<th style="padding: 12px; text-align: center;">Original Shift Time</th>`;
                    html += `<th style="padding: 12px; text-align: center;">Parsed Start Time</th>`;
                    html += `<th style="padding: 12px; text-align: center;">Shift Group</th>`;
                    html += `<th style="padding: 12px; text-align: center;">Capacity</th>`;
                    html += `<th style="padding: 12px; text-align: center;">Allocated</th>`;
                    html += `</tr></thead><tbody>`;
                    
                    data.agents.forEach((agent, index) => {
                        const rowColor = agent.shift_start_time_parsed === 'Not parsed' ? '#fff3cd' : 'white';
                        html += `<tr style="background-color: ${rowColor}; border-bottom: 1px solid #e9ecef;">`;
                        html += `<td style="padding: 10px; text-align: center; font-weight: 600; color: #667eea;">${index + 1}</td>`;
                        html += `<td style="padding: 10px;"><strong>${agent.agent_name || 'N/A'}</strong><br><small style="color: #666;">ID: ${agent.agent_id || 'N/A'}</small></td>`;
                        html += `<td style="padding: 10px;">${agent.email || 'Not set'}</td>`;
                        html += `<td style="padding: 10px; text-align: center;"><code>${agent.shift_time_original || 'Not set'}</code></td>`;
                        html += `<td style="padding: 10px; text-align: center;"><strong style="color: ${agent.shift_start_time_parsed === 'Not parsed' ? '#dc3545' : '#28a745'};">${agent.shift_start_time_display || 'Not parsed'}</strong></td>`;
                        html += `<td style="padding: 10px; text-align: center;"><span style="background: ${agent.shift_group === 1 ? '#e3f2fd' : agent.shift_group === 2 ? '#fff3cd' : '#f3e5f5'}; padding: 5px 10px; border-radius: 4px; font-size: 12px;">${agent.shift_group_name || 'Not set'}</span></td>`;
                        html += `<td style="padding: 10px; text-align: center;">${agent.capacity || 0}</td>`;
                        html += `<td style="padding: 10px; text-align: center;">${agent.allocated || 0}</td>`;
                        html += `</tr>`;
                    });
                    
                    html += `</tbody></table></div>`;
                    
                    modalContent.innerHTML = html;
                } else {
                    modalContent.innerHTML = `<div style="padding: 20px; color: #dc3545;"><strong>Error:</strong> ${data.error || 'Failed to load shift times'}</div>`;
                }
            })
            .catch(error => {
                modalContent.innerHTML = `<div style="padding: 20px; color: #dc3545;"><strong>Error:</strong> ${error.message}</div>`;
            });
        }
        
        function viewAgentAllocation(agentName) {
            const modal = document.getElementById('agentModal');
            const modalAgentName = document.getElementById('modalAgentName');
            const modalContent = document.getElementById('modalContent');
            const modalStats = document.getElementById('modalStats');
            const downloadBtn = document.getElementById('downloadBtn');
            
            // Show modal and set agent name
            modal.style.display = 'block';
            modalAgentName.textContent = `${agentName} - Allocation Details`;
            
            // Show loading state
            modalContent.innerHTML = `
                <div style="text-align: center; padding: 40px;">
                    <i class="fas fa-spinner fa-spin" style="font-size: 2em; color: #667eea;"></i>
                    <p style="margin-top: 15px; color: #666;">Loading allocation data for ${agentName}...</p>
                </div>
            `;
            
            // Fetch agent allocation data
            fetch('/get_agent_allocation', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({ agent_name: agentName })
            })
            .then(response => response.json())
            .then(data => {
                if (data.success) {
                    // Display the data table
                    modalContent.innerHTML = data.html_table;
                    
                    // Update statistics
                    const stats = data.stats;
                    modalStats.innerHTML = `
                        <strong>Allocation Summary:</strong> 
                        ${stats.total_rows} rows allocated | 
                        Capacity: ${stats.capacity} | 
                        First Priority: ${stats.first_priority} | 
                        Second Priority: ${stats.second_priority} | 
                        Third Priority: ${stats.third_priority}
                    `;
                    
                    // Set up download button
                    downloadBtn.onclick = function() {
                        // Create a form and submit it to download the file
                        const form = document.createElement('form');
                        form.method = 'POST';
                        form.action = '/download_agent_file';
                        
                        const input = document.createElement('input');
                        input.type = 'hidden';
                        input.name = 'agent_name';
                        input.value = agentName;
                        
                        form.appendChild(input);
                        document.body.appendChild(form);
                        form.submit();
                        document.body.removeChild(form);
                    };
                } else {
                    modalContent.innerHTML = `
                        <div style="text-align: center; padding: 40px; color: #e74c3c;">
                            <i class="fas fa-exclamation-triangle" style="font-size: 2em;"></i>
                            <p style="margin-top: 15px;">Error loading allocation data: ${data.error}</p>
                        </div>
                    `;
                }
            })
            .catch(error => {
                modalContent.innerHTML = `
                    <div style="text-align: center; padding: 40px; color: #e74c3c;">
                        <i class="fas fa-exclamation-triangle" style="font-size: 2em;"></i>
                        <p style="margin-top: 15px;">Error loading allocation data: ${error.message}</p>
                    </div>
                `;
            });
        }
        
        // Modal close functionality
        function closeModal() {
            const modal = document.getElementById('agentModal');
            if (modal) {
                modal.style.display = 'none';
            }
        }
        
        // Set up modal close event listeners
        document.addEventListener('DOMContentLoaded', function() {
            // Close modal when clicking outside of it
            document.addEventListener('click', function(event) {
                const modal = document.getElementById('agentModal');
                if (modal && event.target === modal) {
                    closeModal();
                }
            });
            
            // Close modal when clicking X button
            document.addEventListener('click', function(event) {
                if (event.target.classList.contains('close')) {
                    closeModal();
                }
            });
            
            // Close modal when clicking close button in footer
            document.addEventListener('click', function(event) {
                if (event.target.classList.contains('close-btn')) {
                    closeModal();
                }
            });
            
            // Close modal when pressing Escape key
            document.addEventListener('keydown', function(event) {
                if (event.key === 'Escape') {
                    closeModal();
                }
            });
        });
    </script>
    
    <!-- Toast Notification Container -->
    <div id="toastContainer" style="position: fixed; top: 20px; right: 20px; z-index: 10000; display: flex; flex-direction: column; gap: 10px;"></div>
    
    <script>
    // Toast notification system - using CSS classes for proper styling
    function showToast(type, title, message, duration = 5000) {
        const container = document.getElementById('toastContainer');
        if (!container) {
            alert(message); // Fallback to alert
            return;
        }
        
        const toast = document.createElement('div');
        toast.className = `toast ${type}`;
        
        const icons = {
            success: 'fas fa-check-circle',
            error: 'fas fa-exclamation-circle',
            warning: 'fas fa-exclamation-triangle',
            info: 'fas fa-info-circle'
        };
        
        toast.innerHTML = `
            <i class="toast-icon ${icons[type]}"></i>
            <div class="toast-content">
                <div class="toast-title">${title}</div>
                <div class="toast-message">${message}</div>
            </div>
            <button class="toast-close" onclick="closeToast(this)">&times;</button>
        `;
        
        container.appendChild(toast);
        
        // Trigger animation
        setTimeout(() => toast.classList.add('show'), 100);
        
        // Auto remove
        setTimeout(() => {
            if (toast.parentNode) {
                closeToast(toast.querySelector('.toast-close'));
            }
        }, duration);
    }
    
    function closeToast(button) {
        const toast = button.closest('.toast');
        toast.classList.remove('show');
        setTimeout(() => {
            if (toast.parentNode) {
                toast.parentNode.removeChild(toast);
            }
        }, 300);
    }
    
    // Global toast functions for easy access
    window.showSuccessToast = (title, message) => showToast('success', title, message);
    window.showErrorToast = (title, message) => showToast('error', title, message);
    window.showWarningToast = (title, message) => showToast('warning', title, message);
    window.showInfoToast = (title, message) => showToast('info', title, message);
    </script>
    
    <!-- Loader/Progress Bar -->
    <div id="loader-overlay" class="loader-overlay">
        <div class="loader-container">
            <div class="loader-spinner"></div>
            <p class="loader-text">Loading Appointment Dates</p>
            <p class="loader-subtitle">Please wait while we process your files...</p>
            <div class="progress-bar-container">
                <div class="progress-bar"></div>
            </div>
        </div>
    </div>
</body>
</html>
"""

def get_business_days_until_date(start_date, target_date):
    """Calculate business days between start_date and target_date (excluding weekends)"""
    from datetime import timedelta
    
    if target_date < start_date:
        return -1  # Past date
    
    current_date = start_date
    business_days = 0
    
    while current_date < target_date:
        current_date += timedelta(days=1)
        # Check if it's a weekday (Monday=0, Sunday=6)
        if current_date.weekday() < 5:  # Monday to Friday
            business_days += 1
    
    return business_days

# Global variable to cache insurance name mapping
_insurance_name_mapping = None
_insurance_name_mapping_loaded = False
_formatted_insurance_names = set()  # Track formatted insurance names
_formatted_insurance_details = []  # Track original -> formatted mappings

# Insurance corrected list mappings (stored directly in code since file will be deleted)
CORRECTED_LIST_MAPPINGS = {
    'Always Care': 'Always Care',
    'Always Care Dental Benefits': 'Always Care',
    'BCBS Arizona': 'BCBS Arizona',
    'BCBS Arizona FEP': 'BCBS Arizona',
    'BCBS California Dental Plan': 'BCBS California',
    'BCBS California FEP': 'BCBS California FEP',
    'BCBS FEP BLUEDENTAL': 'BCBS FEP',
    'BCBS FEP Dental': 'BCBS FEP',
    'BCBS FEP Program': 'BCBS FEP',
    'BCBS FEPOREGON': 'BCBS FEP',
    'BCBS Federal Dental': 'BCBS Federal',
    'BCBS Federal Gov`t': 'BCBS Federal',
    'BCBS IDAHO': 'BCBS IDAHO',
    'BCBS Idaho': 'BCBS IDAHO',
    'BCBS Illinois  Federal': 'BCBS Illinois',
    'BCBS Oregon FEP Program': 'BCBS Oregon FEP Program',
    'BCBS Tennessee Federal Gov`t': 'BCBS Tennessee Federal',
    'Beam Insurance Administrators': 'Beam',
    'Benefit & Risk Management (BRMS  CA)': 'Benefit & Risk Management',
    'Best Life': 'Best Life',
    'Best Life & Health Insurance Co.': 'Best Life',
    'BlueCross BlueShield AZ': 'BCBS Arizona',
    'BlueShield AZ': 'BCBS Arizona',
    'CCPOA': 'CCPOA',
    'CENTRAL STATES': 'CENTRAL STATES',
    'CONVERSION DEFAULT  Do NOT Delete! Change Pt Ins!': 'CONVERSION DEFAULT  Do NOT Delete! Change Pt Ins!',
    'CarePlus': 'CarePlus',
    'Careington Benefit Solutions': 'Careington Benefit Solutions',
    'Central States Health & Life Co. Of Omaha': 'Central States Health & Life Co. Of Omaha',
    'Cigna': 'Cigna',
    'Community Dental Associates': 'Community Dental Associates',
    'Core Five Solutions': 'Core Five Solutions',
    'Cypress Ancillary Benefits': 'Cypress Ancillary Benefits',
    'DD $2000 MAX': 'DD $2000 MAX',
    'DD California Federal Plan': 'DD California Federal Plan',
    'DD California Federal Services': 'DD California Federal Plan',
    'DD DI': 'DD DI',
    'DD Dental Choice': 'DD Dental Choice',
    'DD FE': 'DD FEP',
    'DD Fed Govt': 'DD FEP',
    'DD Federal Employee Dental Pro': 'DD FEP',
    'DD Federal Government Programs': 'DD FEP',
    'DD GE': 'DD GE',
    'DD GeorgiaBasic': 'DD GeorgiaBasic',
    'DD IO': 'DD IO',
    'DD Idaho': 'DD Idaho',
    'DD Individual': 'DD Individual',
    'DD Individual Plan': 'DD Individual',
    'DD Indv': 'DD Individual',
    'DD Ins Company': 'DD Ins Company',
    'DD Insurance Colorado': 'DD Colorado',
    'DD Iowa': 'DD Iowa',
    'DD KA': 'DD KA',
    'DD KE': 'DD KE',
    'DD M': 'DD M',
    'DD Mass': 'DD Mass',
    'DD NO': 'DD NO',
    'DD PE': 'DD PE',
    'DD PL': 'DD PL',
    'DD PLAN OF Wisconsin.': 'DD Wisconsin.',
    'DD PP': 'DD',
    'DD PPO': 'DD',
    'DD Plan': 'DD',
    'DD Plan Of Arizona': 'DD Arizona',
    'DD Plan of Arizona': 'DD Arizona',
    'DD RH': 'DD Rhode Island',
    'DD Rhode Island': 'DD Rhode Island',
    'DD SO': 'DD SO',
    'DD TE': 'DD Tennesse',
    'DD VI': 'DD VI',
    'DD Wisconsin INDV': 'DD Wisconsin INDV',
    'DD plan': 'DD plan',
    'DDIC': 'DDIC',
    'DELTA': 'DD',
    'DENCAP Dental Plans': 'DENCAP Dental Plans',
    'Delt Dental of CA': 'DD California',
    'Delta': 'DD',
    'Delta Deltal premier': 'DD',
    'Delta Denta': 'DD',
    'Delta MN': 'DD Minnesota',
    'Delta WI': 'DD Wisconsin',
    'Delta Wi': 'DD Wisconsin',
    'Delta of WA': 'DD of Washington',
    'DeltaCare USA': 'DeltaCare USA',
    'Dental Claims': 'Dental Claims',
    'Dental Claims Administrator': 'Dental Claims',
    'FEP Blue Dental': 'FEP Blue Dental',
    'FEP BlueDental': 'FEP BlueDental',
    'Fiedler Dentistry Membership Plan': 'Fiedler Dentistry Membership Plan',
    'LINE CONSTRUCTION  LINECO': 'LIneco',
    'LIneco': 'LIneco',
    'Liberty Dental Plan': 'Liberty Dental',
    'Lincoln Financial Group': 'Lincoln Financial Group',
    'Lincoln Financial Group (Lincoln Nationa': 'Lincoln Financial Group',
    'Line Construction Benefit Fund': 'LIneco',
    'Manhattan Life': 'Manhattan Life',
    'Medical Mutual': 'Medical Mutual',
    'Medico Insurance Company': 'Medico Insurance Company',
    'Meritain': 'Meritain',
    'Meritain Health': 'Meritain',
    'Met': 'Metlife',
    'Metlife': 'Metlife',
    'Metropolitan': 'Metropolitan',
    'Moonlight Graham': 'Moonlight Graham',
    'Mutual Omaha': 'Mutual Omaha',
    'NECAIBEW Welfare Trust Fund': 'NECAIBEW Welfare Trust Fund',
    'NHW': 'NHW',
    'NTCA': 'NTCA',
    'NTCA Benefits': 'NTCA',
    'National Elevator Industry Health Benefit Plan': 'National Elevator Industry Plan',
    'National Elevator Industry Plan': 'National Elevator Industry Plan',
    'Network Health Wisconsin': 'Network Health Wisconsin',
    'Nippon Life Insurance': 'Nippon Life Insurance',
    'Novartis Corporation': 'Novartis Corporation',
    'OSF MedAdvantage': 'OSF MedAdvantage',
    'Oakland County Discount Plan': 'Oakland County Discount Plan',
    'Operating Engineers Local #49': 'Operating Engineers Local #49',
    'PACIFIC SOURCE': 'PACIFIC SOURCE',
    'PacificSource Health Plans': 'PacificSource Health Plans',
    'Paramount Dental': 'Paramount Dental',
    'Perio Membership Plan August': 'Perio Membership Plan August',
    "Physician's Mutual": 'Physicians Mutual',
    'Physicians Mutual': 'Physicians Mutual',
    'Plan for Health': 'Plan for Health',
    'Prairie States': 'Prairie States',
    'Principal': 'Principal',
    'Principlal': 'Principal',
    'Professional Benefits Administr': 'Professional Benefits Administr',
    'REGENCE BCBS': 'REGENCE BCBS',
    'Regarding Dentistry  Membership': 'Regarding Dentistry  Membership',
    'Reliance Standard': 'Reliance Standard',
    'Renaissance': 'Renaissance',
    'Renaissance Life and Health': 'Renaissance',
    'Renaissance, Dental': 'Renaissance',
    'SIHO': 'SIHO',
    'Secure Care Dental': 'Secure Care Dental',
    'Security Life Ins of America': 'Security Life Ins of America',
    'Simple Dental': 'Simple Dental',
    'Standard Life Insurance': 'Standard Life Insurance',
    'Strong Family Health': 'Strong Family Health',
    'Sunlife': 'Sunlife',
    'Superior Dental Care': 'Superior Dental Care',
    'THE UNITED FURNITURE WORKERS INSURANCE F': 'THE UNITED FURNITURE WORKERS INSURANCE F',
    'Team Care': 'Teamcare',
    'Teamcare': 'Teamcare',
    'Texas International Life Ins Co': 'Texas International Life Ins Co',
    'The Benefit Group': 'The Benefit Group',
    'Tricare': 'Tricare',
    'TruAssure Insurance Company': 'TruAssure',
    'UHC': 'UHC',
    'UMR': 'UMR',
    'US Health Group': 'US Health Group',
    'United Concordia': 'UCCI',
    'Unum': 'Unum',
    'WilsonMcShane Corporation': 'WilsonMcShane Corporation',
}

def clean_insurance_name(name):
    """Remove spaces and special characters from the beginning and end of insurance name"""
    if not name or pd.isna(name):
        return name
    
    name_str = str(name)
    # Remove spaces and special characters from start and end
    # Special characters: - . , ; : | / \ _ ( ) [ ] { } * # @ $ % ^ & + = ~ ` ' " < > ?
    name_str = re.sub(r'^[\s\-.,;:|/\\_()\[\]{}*#@$%^&+=\~`\'"<>?]+', '', name_str)
    name_str = re.sub(r'[\s\-.,;:|/\\_()\[\]{}*#@$%^&+=\~`\'"<>?]+$', '', name_str)
    return name_str.strip()

def load_insurance_name_mapping():
    """Load insurance name mapping from Insurance Uniform Name.xlsx file and CORRECTED_LIST_MAPPINGS dictionary"""
    global _insurance_name_mapping, _insurance_name_mapping_loaded
    
    if _insurance_name_mapping_loaded:
        return _insurance_name_mapping
    
    _insurance_name_mapping = {}
    total_mappings = 0
    
    # Load from Insurance Uniform Name.xlsx
    mapping_file1 = 'Insurance Uniform Name.xlsx'
    try:
        if os.path.exists(mapping_file1):
            df = pd.read_excel(mapping_file1)
            count = 0
            # Create mapping dictionary: original name -> uniform name
            # Handle case-insensitive matching by storing both original and lowercase keys
            for _, row in df.iterrows():
                original = str(row['Insurance']).strip() if pd.notna(row['Insurance']) else ''
                uniform = str(row['Insurance New']).strip() if pd.notna(row['Insurance New']) else ''
                
                if original and uniform:
                    # Clean both original and formatted names
                    original = clean_insurance_name(original)
                    uniform = clean_insurance_name(uniform)
                    
                    if original and uniform:
                        # Store with original case
                        _insurance_name_mapping[original] = uniform
                        # Also store with lowercase for case-insensitive lookup
                        _insurance_name_mapping[original.lower()] = uniform
                        count += 1
            
            total_mappings += count
        else:
            pass
    except Exception as e:
        pass
    
    # Load from CORRECTED_LIST_MAPPINGS dictionary (stored directly in code)
    try:
        count = 0
        for original, formatted in CORRECTED_LIST_MAPPINGS.items():
            # Clean both original and formatted names
            original_clean = clean_insurance_name(original)
            formatted_clean = clean_insurance_name(formatted)
            
            if original_clean and formatted_clean:
                # Store with original case
                _insurance_name_mapping[original_clean] = formatted_clean
                # Also store with lowercase for case-insensitive lookup
                _insurance_name_mapping[original_clean.lower()] = formatted_clean
                count += 1
        
        total_mappings += count
    except Exception as e:
        pass
    
    if total_mappings > 0:
        pass
    else:
        pass
    
    _insurance_name_mapping_loaded = True
    return _insurance_name_mapping

def format_insurance_company_name(insurance_text):
    """Format insurance company name for better allocation matching - uses Insurance Uniform Name.xlsx mapping first"""
    global _formatted_insurance_names, _formatted_insurance_details
    
    if pd.isna(insurance_text):
        return insurance_text
    
    insurance_str = clean_insurance_name(insurance_text)
    if not insurance_str:
        return insurance_text  # Return original if cleaning results in empty
    
    original_name = insurance_str  # Keep original for tracking
    matched_from_mapping = False  # Track if matched from mapping file
    
    # Handle special cases first (after cleaning)
    if insurance_str.upper() == 'NO INSURANCE':
        formatted = clean_insurance_name('No Insurance')
    elif insurance_str.upper() == 'PATIENT NOT FOUND':
        formatted = clean_insurance_name('PATIENT NOT FOUND')
    elif insurance_str.upper() == 'DUPLICATE':
        formatted = clean_insurance_name('DUPLICATE')
    elif insurance_str.upper() == 'UNKNOWN':
        formatted = clean_insurance_name('Unknown')
    else:
        # Load insurance name mapping (will only load once)
        mapping = load_insurance_name_mapping()
        formatted = None
        
        # Try exact match first (with original text)
        if insurance_str in mapping:
            formatted = clean_insurance_name(str(mapping[insurance_str]))
            matched_from_mapping = True
        else:
            # Extract company name before "Ph#" or phone numbers for matching
            if "Ph#" in insurance_str:
                company_name = insurance_str.split("Ph#")[0]
            elif re.search(r'Ph#:?-?\s*\(?\d{3}', insurance_str):
                # Handle various phone number patterns
                company_name = re.split(r'Ph#:?-?\s*\(?\d', insurance_str)[0]
            else:
                company_name = insurance_str
            
            # Clean the company name
            company_name = clean_insurance_name(company_name)
            
            # Try matching with cleaned company name
            if company_name and company_name in mapping:
                formatted = clean_insurance_name(str(mapping[company_name]))
                matched_from_mapping = True
            elif company_name and company_name.lower() in mapping:
                formatted = clean_insurance_name(str(mapping[company_name.lower()]))
                matched_from_mapping = True
            
            # Try matching original string (lowercase) as fallback
            if not formatted and insurance_str.lower() in mapping:
                formatted = clean_insurance_name(str(mapping[insurance_str.lower()]))
                matched_from_mapping = True
            
            # Remove "Primary" and "Secondary" text
            if not formatted and company_name:
                company_name = re.sub(r'\s*\(Primary\)', '', company_name, flags=re.IGNORECASE)
                company_name = re.sub(r'\s*\(Secondary\)', '', company_name, flags=re.IGNORECASE)
                company_name = re.sub(r'\s*Primary', '', company_name, flags=re.IGNORECASE)
                company_name = re.sub(r'\s*Secondary', '', company_name, flags=re.IGNORECASE)
                company_name = clean_insurance_name(company_name)
                
                # Try matching again after removing Primary/Secondary
                if company_name and company_name in mapping:
                    formatted = clean_insurance_name(str(mapping[company_name]))
                    matched_from_mapping = True
                elif company_name and company_name.lower() in mapping:
                    formatted = clean_insurance_name(str(mapping[company_name.lower()]))
                    matched_from_mapping = True
        
        # If no match found in mapping, use fallback logic (existing code continues below)
        if not formatted:
            formatted = None  # Will be set by fallback logic
    
    # Continue with fallback logic if no mapping match found
    if not formatted:
        # Use existing fallback formatting logic
        # State abbreviations mapping
        STATE_ABBREVIATIONS = {
            'AL': 'Alabama', 'AK': 'Alaska', 'AR': 'Arkansas', 'AZ': 'Arizona',
            'CA': 'California', 'CL': 'California', 'CO': 'Colorado', 'CT': 'Connecticut', 'DE': 'Delaware',
            'DC': 'District of Columbia', 'FL': 'Florida', 'GA': 'Georgia', 'HI': 'Hawaii',
            'ID': 'Idaho', 'IL': 'Illinois', 'IN': 'Indiana', 'IA': 'Iowa',
            'KS': 'Kansas', 'KY': 'Kentucky', 'LA': 'Louisiana', 'ME': 'Maine',
            'MD': 'Maryland', 'MA': 'Massachusetts', 'MI': 'Michigan', 'MN': 'Minnesota',
            'MS': 'Mississippi', 'MO': 'Missouri', 'MT': 'Montana', 'NE': 'Nebraska',
            'NV': 'Nevada', 'NH': 'New Hampshire', 'NJ': 'New Jersey', 'NM': 'New Mexico',
            'NY': 'New York', 'NC': 'North Carolina', 'ND': 'North Dakota', 'OH': 'Ohio',
            'OK': 'Oklahoma', 'OR': 'Oregon', 'PA': 'Pennsylvania', 'RI': 'Rhode Island',
            'SC': 'South Carolina', 'SD': 'South Dakota', 'TN': 'Tennessee', 'TX': 'Texas',
            'UT': 'Utah', 'VT': 'Vermont', 'VA': 'Virginia', 'WA': 'Washington',
            'WV': 'West Virginia', 'WI': 'Wisconsin', 'WY': 'Wyoming'
        }
        
        # Common state name typos and variations
        STATE_TYPO_CORRECTIONS = {
            'californi': 'California',  # Missing last letter
            'californa': 'California',  # Common typo
            'californai': 'California',  # Letter order typo
            'colarado': 'Colorado',  # Missing 'o' typo
            'minnesotta': 'Minnesota',  # Extra 't' typo
        }
        
        def expand_state_abbreviations(text):
            """Expand state abbreviations to full state names"""
            for abbr, full_name in STATE_ABBREVIATIONS.items():
                pattern = r'\b' + re.escape(abbr) + r'\b'
                text = re.sub(pattern, full_name, text, flags=re.IGNORECASE)
            return text
        
        def correct_state_typos(text):
            """Correct common state name typos"""
            if not text:
                return text
            
            # Replace any occurrence of typo words with correct spelling (case-insensitive)
            for typo, correct in STATE_TYPO_CORRECTIONS.items():
                pattern = r'\b' + re.escape(typo) + r'\b'
                text = re.sub(pattern, correct, text, flags=re.IGNORECASE)
            
            return text
        
        def format_state_name(state_text):
            """Format state name: first letter capital, rest lowercase (handles all caps, mixed case, etc.)"""
            if not state_text:
                return state_text
            
            # Handle multi-word state names (e.g., "NEW YORK" -> "New York", "NORTH CAROLINA" -> "North Carolina")
            words = state_text.split()
            formatted_words = []
            for word in words:
                if word.isupper() and len(word) > 1:
                    # If word is all caps, convert to title case (first letter capital, rest lowercase)
                    formatted_words.append(word.capitalize())
                elif word.isupper() and len(word) == 1:
                    # Single letter stays as is (for abbreviations like "I" in "Rhode Island")
                    formatted_words.append(word)
                elif word.islower():
                    # All lowercase, capitalize first letter
                    formatted_words.append(word.capitalize())
                elif word.istitle():
                    # Already in title case (e.g., "New"), keep as is
                    formatted_words.append(word)
                else:
                    # Mixed case or other, convert to title case
                    formatted_words.append(word.capitalize())
            
            return ' '.join(formatted_words)
        
        # Handle Delta Dental variations - normalize to "DD {state}" format
        if re.search(r'\bDD\b', company_name, re.IGNORECASE):
            # Handle existing "DD" patterns like "DD California", "DD of California", "DD CA", "DD PLAN OF Wisconsin"
            dd_match = re.search(r'\bDD\b\s+(?:plan\s+of\s+|of\s+)?(.+)', company_name, re.IGNORECASE)
            if dd_match:
                state = clean_insurance_name(dd_match.group(1))
                # Remove "PLAN OF" if it appears in the state text
                state = re.sub(r'\bplan\s+of\s+', '', state, flags=re.IGNORECASE)
                # Remove common suffixes
                state = re.sub(r'\s*\(.*?\)', '', state)
                # Remove trailing periods, pipes, and other special characters
                state = re.sub(r'[|.]+\s*$', '', state)
                state = correct_state_typos(state)
                state = expand_state_abbreviations(state)
                state = format_state_name(state)
                formatted = clean_insurance_name(f"DD {state}")
            else:
                formatted = clean_insurance_name("DD")
        elif re.search(r'delta\s+dental', company_name, re.IGNORECASE):
            # Handle "Delta Dental" variations
            delta_match = re.search(r'delta\s+dental\s+(?:of\s+)?(.+)', company_name, re.IGNORECASE)
            if delta_match:
                state = clean_insurance_name(delta_match.group(1))
                # Remove "PLAN OF" if it appears in the state text
                state = re.sub(r'\bplan\s+of\s+', '', state, flags=re.IGNORECASE)
                # Remove common suffixes
                state = re.sub(r'\s*\(.*?\)', '', state)
                # Remove trailing periods, pipes, and other special characters
                state = re.sub(r'[|.]+\s*$', '', state)
                state = correct_state_typos(state)
                state = expand_state_abbreviations(state)
                state = format_state_name(state)
                formatted = clean_insurance_name(f"DD {state}")
            else:
                formatted = clean_insurance_name("DD")
        
        # Handle Anthem variations FIRST (before BCBS to avoid conflicts)
        elif re.search(r'anthem|blue\s+cross.*anthem|anthem.*blue\s+cross', company_name, re.IGNORECASE):
            formatted = clean_insurance_name("Anthem")
        
        # Handle BCBS variations (including BC/BS with slash)
        elif re.search(r'bc\s*/\s*bs|bcbs|bc\s+of|blue\s+cross|blue\s+shield|bcbbs', company_name, re.IGNORECASE):
            # Check for "BCBS / BLUE SHEILD", "BCBS Blue Shiel", "BCBS Blue Shield" -> just "BCBS"
            # Handles: "shiel" (without 'd'), "shield" (correct spelling), "sheild" (misspelling)
            if (re.search(r'bcbs\s*/\s*blue\s+(shiel|shield|sheild)', company_name, re.IGNORECASE) or
                re.search(r'bcbs\s+blue\s+(shiel|shield|sheild)', company_name, re.IGNORECASE)):
                formatted = clean_insurance_name("BCBS")
            # Handle BCBBS typo
            elif re.search(r'bcbbs', company_name, re.IGNORECASE):
                formatted = clean_insurance_name("BCBS")
            # Check for full "Blue Cross Blue Shield" pattern first
            elif re.search(r'blue\s+cross\s+blue\s+shield', company_name, re.IGNORECASE):
                bcbs_match = re.search(r'blue\s+cross\s+blue\s+shield\s+(?:of\s+)?(.+)', company_name, re.IGNORECASE)
                if bcbs_match:
                    state = bcbs_match.group(1)
                    # Remove trailing dashes and extra text like "- federal", "- Federal", etc.
                    state = re.sub(r'\s*-\s*(federal|Federal|FEDERAL).*$', '', state, flags=re.IGNORECASE)
                    # Remove common suffixes in parentheses
                    state = re.sub(r'\s*\(.*?\)', '', state)
                    state = clean_insurance_name(state)
                    state = re.sub(r'[|.]+\s*$', '', state)
                    state = correct_state_typos(state)
                    state = expand_state_abbreviations(state)
                    state = format_state_name(state)
                    formatted = clean_insurance_name(f"BCBS {state}") if state else clean_insurance_name("BCBS")
                else:
                    formatted = clean_insurance_name("BCBS")
            # Handle BC/BS patterns
            elif re.search(r'bc/bs', company_name, re.IGNORECASE):
                bcbs_match = re.search(r'bc/bs\s+(?:of\s+)?(.+)', company_name, re.IGNORECASE)
                if bcbs_match:
                    state = bcbs_match.group(1)
                    state = re.sub(r'\s*-\s*(federal|Federal|FEDERAL).*$', '', state, flags=re.IGNORECASE)
                    state = re.sub(r'\s*\(.*?\)', '', state)
                    state = clean_insurance_name(state)
                    state = re.sub(r'[|.]+\s*$', '', state)
                    state = correct_state_typos(state)
                    state = expand_state_abbreviations(state)
                    state = format_state_name(state)
                    formatted = clean_insurance_name(f"BCBS {state}") if state else clean_insurance_name("BCBS")
                else:
                    formatted = clean_insurance_name("BCBS")
            # Handle BC Of patterns
            elif re.search(r'bc\s+of', company_name, re.IGNORECASE):
                bcbs_match = re.search(r'bc\s+of\s+(.+)', company_name, re.IGNORECASE)
                if bcbs_match:
                    state = bcbs_match.group(1)
                    state = re.sub(r'\s*-\s*(federal|Federal|FEDERAL).*$', '', state, flags=re.IGNORECASE)
                    state = re.sub(r'\s*\(.*?\)', '', state)
                    state = clean_insurance_name(state)
                    state = re.sub(r'[|.]+\s*$', '', state)
                    state = correct_state_typos(state)
                    state = expand_state_abbreviations(state)
                    state = format_state_name(state)
                    formatted = clean_insurance_name(f"BCBS {state}") if state else clean_insurance_name("BCBS")
                else:
                    formatted = clean_insurance_name("BCBS")
            # Handle other BCBS patterns
            else:
                bcbs_match = re.search(r'(?:bcbs|blue\s+cross|blue\s+shield)\s+(?:of\s+)?(.+)', company_name, re.IGNORECASE)
                if bcbs_match:
                    state = bcbs_match.group(1)
                    # Remove trailing dashes and extra text like "- federal", "- Federal", etc.
                    state = re.sub(r'\s*-\s*(federal|Federal|FEDERAL).*$', '', state, flags=re.IGNORECASE)
                    # Remove common suffixes in parentheses
                    state = re.sub(r'\s*\(.*?\)', '', state)
                    # Remove trailing dashes and special characters
                    state = clean_insurance_name(state)
                    # Remove trailing periods, pipes, and other special characters
                    state = re.sub(r'[|.]+\s*$', '', state)
                    state = correct_state_typos(state)
                    state = expand_state_abbreviations(state)
                    state = format_state_name(state)
                    if state:
                        formatted = clean_insurance_name(f"BCBS {state}")
                    else:
                        formatted = clean_insurance_name("BCBS")
                else:
                    formatted = clean_insurance_name("BCBS")
        
        # Handle other specific companies
        elif re.search(r'metlife|met\s+life', company_name, re.IGNORECASE):
            formatted = clean_insurance_name("Metlife")
        elif re.search(r'cigna', company_name, re.IGNORECASE):
            formatted = clean_insurance_name("Cigna")
        elif re.search(r'aarp', company_name, re.IGNORECASE):
            formatted = clean_insurance_name("AARP")
        elif re.search(r'uhc|united\s*healthcare|united\s*health\s*care', company_name, re.IGNORECASE):
            formatted = clean_insurance_name("UHC")
        elif re.search(r'teamcare', company_name, re.IGNORECASE):
            formatted = clean_insurance_name("Teamcare")
        elif re.search(r'humana', company_name, re.IGNORECASE):
            formatted = clean_insurance_name("Humana")
        elif re.search(r'aetna', company_name, re.IGNORECASE):
            formatted = clean_insurance_name("Aetna")
        elif re.search(r'guardian', company_name, re.IGNORECASE):
            formatted = clean_insurance_name("Guardian")
        elif re.search(r'g\s*e\s*h\s*a', company_name, re.IGNORECASE):
            formatted = clean_insurance_name("GEHA")
        elif re.search(r'principal', company_name, re.IGNORECASE):
            formatted = clean_insurance_name("Principal")
        elif re.search(r'ameritas', company_name, re.IGNORECASE):
            formatted = clean_insurance_name("Ameritas")
        elif re.search(r'physicians\s+mutual', company_name, re.IGNORECASE):
            formatted = clean_insurance_name("Physicians Mutual")
        elif re.search(r'mutual\s+of\s+omaha', company_name, re.IGNORECASE):
            formatted = clean_insurance_name("Mutual Omaha")
        elif re.search(r'sunlife|sun\s+life', company_name, re.IGNORECASE):
            formatted = clean_insurance_name("Sunlife")
        elif re.search(r'careington', company_name, re.IGNORECASE):
            formatted = clean_insurance_name("Careington Benefit Solutions")
        elif re.search(r'automated\s+benefit', company_name, re.IGNORECASE):
            formatted = clean_insurance_name("Automated Benefit Services Inc")
        elif re.search(r'regence', company_name, re.IGNORECASE):
            formatted = clean_insurance_name("REGENCE BCBS")
        elif re.search(r'united\s+concordia', company_name, re.IGNORECASE):
            formatted = clean_insurance_name("United Concordia")
        elif re.search(r'medical\s+mutual', company_name, re.IGNORECASE):
            formatted = clean_insurance_name("Medical Mutual")
        elif re.search(r'unum', company_name, re.IGNORECASE):
            formatted = clean_insurance_name("Unum")
        elif re.search(r'wilson\s+mcshane', company_name, re.IGNORECASE):
            formatted = clean_insurance_name("Wilson McShane- Delta Dental")
        elif re.search(r'dentaquest', company_name, re.IGNORECASE):
            formatted = clean_insurance_name("Dentaquest")
        elif re.search(r'umr', company_name, re.IGNORECASE):
            formatted = clean_insurance_name("UMR")
        elif re.search(r'adn\s+administrators', company_name, re.IGNORECASE):
            formatted = clean_insurance_name("ADN Administrators")
        elif re.search(r'beam', company_name, re.IGNORECASE):
            formatted = clean_insurance_name("Beam")
        elif re.search(r'liberty(?:\s+dental)?', company_name, re.IGNORECASE):
            formatted = clean_insurance_name("Liberty Dental Plan")
        elif re.search(r'ucci', company_name, re.IGNORECASE):
            formatted = clean_insurance_name("UCCI")
        elif re.search(r'ccpoa|cc\s*poa|c\s+c\s+p\s+o\s+a', company_name, re.IGNORECASE):
            formatted = clean_insurance_name("CCPOA")
        elif re.search(r'kansas\s+city', company_name, re.IGNORECASE):
            formatted = clean_insurance_name("Kansas City")
        elif re.search(r'the\s+guardian', company_name, re.IGNORECASE):
            formatted = clean_insurance_name("The Guardian")
        elif re.search(r'community\s+dental', company_name, re.IGNORECASE):
            formatted = clean_insurance_name("Community Dental Associates")
        elif re.search(r'northeast\s+delta\s+dental', company_name, re.IGNORECASE):
            formatted = clean_insurance_name("Northeast Delta Dental")
        elif re.search(r'equitable', company_name, re.IGNORECASE):
            formatted = clean_insurance_name("Equitable")
        elif re.search(r'manhattan\s+life', company_name, re.IGNORECASE):
            formatted = clean_insurance_name("Manhattan Life")
        elif re.search(r'standard\s+(?:life\s+)?insurance', company_name, re.IGNORECASE):
            formatted = clean_insurance_name("Standard Life Insurance")
        elif re.search(r'keenan', company_name, re.IGNORECASE):
            formatted = clean_insurance_name("Keenan")
        elif re.search(r'plan\s+for\s+health', company_name, re.IGNORECASE):
            formatted = clean_insurance_name("Plan for Health")
        elif re.search(r'conversion\s+default', company_name, re.IGNORECASE):
            formatted = clean_insurance_name("CONVERSION DEFAULT - Do NOT Delete! Change Pt Ins!")
        elif re.search(r'health\s*partners', company_name, re.IGNORECASE):
            # Check if it has "of [State]" pattern
            hp_match = re.search(r'health\s*partners\s+of\s+(.+)', company_name, re.IGNORECASE)
            if hp_match:
                state = hp_match.group(1).strip()
                state = clean_insurance_name(state)
                state = format_state_name(state)
                formatted = clean_insurance_name(f"Health Partners {state}")
            else:
                formatted = clean_insurance_name("Health Partners")
        elif re.search(r'network\s+health', company_name, re.IGNORECASE):
            # Check if it has "Wisconsin" in the name
            if re.search(r'wisconsin', company_name, re.IGNORECASE):
                formatted = clean_insurance_name("Network Health Wisconsin")
            else:
                formatted = clean_insurance_name("Network Health Go")
        else:
            # If no specific pattern matches, return the cleaned company name
            formatted = clean_insurance_name(company_name) if company_name else company_name
    
    # Track formatted names (only if different from original)
    if formatted and formatted != original_name:
        if original_name not in _formatted_insurance_names:
            _formatted_insurance_names.add(original_name)
            _formatted_insurance_details.append({
                'original': original_name,
                'formatted': formatted,
                'from_mapping': matched_from_mapping
            })
    
    # Ensure final output is cleaned (remove any spaces/special chars before/after)
    return clean_insurance_name(formatted) if formatted else formatted

def print_formatted_insurance_companies():
    """Print list of all formatted insurance companies to console"""
    global _formatted_insurance_details
    
    if not _formatted_insurance_details:
        return
    
    
    # Group by source (mapping vs fallback)
    from_mapping = [d for d in _formatted_insurance_details if d['from_mapping']]
    from_fallback = [d for d in _formatted_insurance_details if not d['from_mapping']]
    
    if from_mapping:
        for i, detail in enumerate(from_mapping, 1):
            pass
    
    if from_fallback:
        for i, detail in enumerate(from_fallback, 1):
            pass
    

# DD INS group mapping - these companies should be treated as part of "DD INS" or "INS" group
DD_INS_GROUP = [
    'DD California',
    'DD Florida',
    'DD Texas',
    'DD Pennsylvania',
    'DD New York',
    'DD Alabama',
    'DD Georgia',
    'DD Delaware'
]

# DD Toolkit group mapping - these companies should be treated as part of "DD Toolkit", "DD Toolkits", or "DD" group
DD_TOOLKIT_GROUP = [
    'DD New Mexico',
    'DD Ohio',
    'DD Indiana',
    'DD Michigan',  # Note: user mentioned "Michigen" but correct spelling is "Michigan"
    'DD Minnesota',
    'DD Tennessee',
    'DD Arizona',
    'DD North Carolina',
    'DD California Federal'
]

def expand_insurance_groups(insurance_list_str):
    """
    Expand insurance group names to include all companies in those groups.
    Handles:
    - "DD INS" or "INS" -> expands to DD_INS_GROUP companies
    - "DD Toolkit", "DD Toolkits", or "DD" (when used as group) -> expands to DD_TOOLKIT_GROUP companies
    
    Args:
        insurance_list_str: String containing insurance companies separated by ; , or |
    
    Returns:
        String with group names expanded to individual companies
    """
    if pd.isna(insurance_list_str) or not insurance_list_str:
        return insurance_list_str
    
    value_str = str(insurance_list_str)
    # Split by common delimiters
    companies = [comp.strip() for comp in re.split(r'[;,\|]', value_str) if comp.strip()]
    
    expanded_companies = []
    has_dd_ins = False
    has_ins = False
    has_dd_toolkit = False
    has_dd_toolkits = False
    has_dd_group = False
    
    for comp in companies:
        comp_lower = comp.lower().strip()
        
        # Check for "DD INS" or "INS" (case-insensitive)
        if comp_lower == 'dd ins' or comp_lower == 'ins':
            if 'dd' in comp_lower:
                has_dd_ins = True
            else:
                has_ins = True
            # Don't add the group name itself, we'll add the group companies
        # Check for "DD Toolkit", "DD Toolkits", or "DD" (as group name)
        elif comp_lower == 'dd toolkit':
            has_dd_toolkit = True
        elif comp_lower == 'dd toolkits':
            has_dd_toolkits = True
        elif comp_lower == 'dd':
            # "DD" can be a group name OR a standalone company name
            # We'll treat it as a group name if it appears alone or with other groups
            # To be safe, we'll check if there are other group keywords nearby
            has_dd_group = True
        else:
            # Keep other companies as-is
            expanded_companies.append(comp)
    
    # Add all DD INS group companies if DD INS or INS was found
    if has_dd_ins or has_ins:
        for dd_ins_company in DD_INS_GROUP:
            # Check if company is already in the list (case-insensitive)
            if not any(existing.lower() == dd_ins_company.lower() for existing in expanded_companies):
                expanded_companies.append(dd_ins_company)
        expansion_type = 'DD INS' if has_dd_ins else 'INS'
    
    # Add all DD Toolkit group companies if DD Toolkit/Toolkits/DD was found
    if has_dd_toolkit or has_dd_toolkits or has_dd_group:
        for dd_toolkit_company in DD_TOOLKIT_GROUP:
            # Check if company is already in the list (case-insensitive)
            if not any(existing.lower() == dd_toolkit_company.lower() for existing in expanded_companies):
                expanded_companies.append(dd_toolkit_company)
        expansion_type = 'DD Toolkit' if has_dd_toolkit else ('DD Toolkits' if has_dd_toolkits else 'DD')
    
    # Join back with semicolon
    return '; '.join(expanded_companies) if expanded_companies else insurance_list_str

def format_insurance_column_in_dataframe(df, column_name):
    """Format insurance company names in a DataFrame column"""
    if column_name not in df.columns:
        return df
    
    original_count = len(df[column_name].dropna())
    
    # Apply formatting
    df[column_name] = df[column_name].apply(format_insurance_company_name)
    
    formatted_count = len(df[column_name].dropna())
    
    return df

def detect_and_assign_new_insurance_companies(data_df, agent_data, insurance_carrier_col, insurance_working_col, agent_name_col=None):
    """Detect new insurance companies in data file and automatically assign them to senior agents"""
    try:
        if not insurance_carrier_col or not insurance_working_col:
            return agent_data, []
        
        # Get all insurance companies from data file
        data_insurance_companies = set()
        for _, row in data_df.iterrows():
            if pd.notna(row[insurance_carrier_col]):
                company = str(row[insurance_carrier_col]).strip()
                if company and company.lower() != 'unknown':
                    data_insurance_companies.add(company)
        
        # Get all insurance companies currently assigned to agents
        agent_insurance_companies = set()
        for _, row in agent_data.iterrows():
            if pd.notna(row[insurance_working_col]):
                companies_str = str(row[insurance_working_col])
                companies = [comp.strip() for comp in companies_str.replace(',', ';').replace('|', ';').split(';') if comp.strip()]
                for comp in companies:
                    if comp.lower() != 'senior':
                        agent_insurance_companies.add(comp)
        
        # Find new insurance companies
        new_insurance_companies = data_insurance_companies - agent_insurance_companies
        
        if not new_insurance_companies:
            return agent_data, []
        
        # Find senior agents (those with 'senior' in their Insurance List column)
        senior_agents = []
        for idx, row in agent_data.iterrows():
            if pd.notna(row[insurance_working_col]):
                companies_str = str(row[insurance_working_col])
                if 'senior' in companies_str.lower():
                    senior_agents.append(idx)
        
        # Console log senior agents found
        if senior_agents:
            for idx in senior_agents:
                if agent_name_col and agent_name_col in agent_data.columns:
                    agent_name = agent_data.iloc[idx][agent_name_col]
                else:
                    agent_name = f"Agent {idx}"
        else:
            pass
        
        # Assign new insurance companies to senior agents
        updated_agents = []
        for idx, row in agent_data.iterrows():
            if idx in senior_agents:
                # Add new insurance companies to senior agents
                current_companies = str(row[insurance_working_col]) if pd.notna(row[insurance_working_col]) else ''
                new_companies_str = '; '.join(new_insurance_companies)
                
                if current_companies:
                    updated_companies = f"{current_companies}; {new_companies_str}"
                else:
                    updated_companies = new_companies_str
                
                # Update the row
                row_copy = row.copy()
                row_copy[insurance_working_col] = updated_companies
                updated_agents.append(row_copy)
            else:
                updated_agents.append(row)
        
        # Convert back to DataFrame
        updated_agent_data = pd.DataFrame(updated_agents)
        
        return updated_agent_data, list(new_insurance_companies)
        
    except Exception as e:
        return agent_data, []

def get_nth_business_day(start_date, n):
    """Get the nth business day from start_date"""
    from datetime import timedelta
    
    current_date = start_date
    business_days_count = 0
    
    while business_days_count < n:
        current_date += timedelta(days=1)
        # Check if it's a weekday (Monday=0, Sunday=6)
        if current_date.weekday() < 5:  # Monday to Friday
            business_days_count += 1
    
    return current_date

def process_allocation_files(allocation_df, data_df):
    """Process data file with priority assignment based on business days calendar"""
    try:
        from datetime import datetime, timedelta
        import pandas as pd
        
        # Use data_df as the main file to process (ignore allocation_df for now)
        processed_df = data_df.copy()
        
        # Find the appointment date column (case-insensitive search)
        appointment_date_col = None
        for col in processed_df.columns:
            if 'appointment' in col.lower() and 'date' in col.lower():
                appointment_date_col = col
                break
        
        if appointment_date_col is None:
            return f"❌ Error: 'Appointment Date' column not found in data file.\nAvailable columns: {list(processed_df.columns)}", None
        
        # Convert appointment date column to datetime and remove time component
        try:
            processed_df[appointment_date_col] = pd.to_datetime(processed_df[appointment_date_col], errors='coerce').dt.date
        except Exception as e:
            return f"❌ Error converting appointment dates: {str(e)}", None
        
        # Get today's date
        today = datetime.now().date()
        
        # Check if Priority Status column exists, if not create it
        if 'Priority Status' not in processed_df.columns:
            processed_df['Priority Status'] = ''
        
        # Convert Priority Status column to object type to avoid dtype warnings
        processed_df['Priority Status'] = processed_df['Priority Status'].astype('object')
        
        # Calculate business day targets
        first_business_day = get_nth_business_day(today, 1)
        second_business_day = get_nth_business_day(today, 2)
        seventh_business_day = get_nth_business_day(today, 7)
        
        # Count statistics
        total_rows = len(processed_df)
        first_priority_count = 0
        invalid_dates = 0
        
        # Process each row
        for idx, row in processed_df.iterrows():
            appointment_date = row[appointment_date_col]
            
            # Skip rows with invalid dates
            if pd.isna(appointment_date):
                processed_df.at[idx, 'Priority Status'] = 'Invalid Date'
                invalid_dates += 1
                continue
            
            # Convert to date if it's datetime
            if hasattr(appointment_date, 'date'):
                appointment_date = appointment_date.date()
            
            # Check if appointment date matches First Priority criteria
            if (appointment_date == today or 
                appointment_date == first_business_day or 
                appointment_date == second_business_day or 
                appointment_date == seventh_business_day):
                processed_df.at[idx, 'Priority Status'] = 'First Priority'
                first_priority_count += 1
            else:
                # Keep blank for now as requested
                processed_df.at[idx, 'Priority Status'] = ''
        
        # Generate result message
        result_message = f"""✅ Priority processing completed successfully!

📊 Processing Statistics:
- Total rows processed: {total_rows}
- First Priority: {first_priority_count} rows
- Other rows: {total_rows - first_priority_count - invalid_dates} rows (kept blank for now)
- Invalid dates: {invalid_dates} rows

📅 Business Day Calendar Logic Applied:
1. First Priority: Same day, 1st business day, 2nd business day, and 7th business day from today
2. Second Priority: (To be implemented later)
3. Third Priority: (To be implemented later)

📅 Business Day Targets:
- Today: {today.strftime('%Y-%m-%d (%A)')}
- 1st Business Day: {first_business_day.strftime('%Y-%m-%d (%A)')}
- 2nd Business Day: {second_business_day.strftime('%Y-%m-%d (%A)')}
- 7th Business Day: {seventh_business_day.strftime('%Y-%m-%d (%A)')}

📋 Updated column: 'Priority Status'
📅 Based on column: '{appointment_date_col}'

🔍 Sample of processed data:
{processed_df[['Priority Status', appointment_date_col]].head(10).to_string()}

💾 Ready to download the processed result file!"""
        
        return result_message, processed_df
        
    except Exception as e:
        return f"❌ Error during processing: {str(e)}", None

def process_allocation_files_with_dates(allocation_df, data_df, selected_dates, custom_dates, appointment_dates, appointment_dates_second=None, receive_dates=None):
    """Process data file with priority assignment and generate agent allocation summary"""
    global agent_allocations_data
    try:
        from datetime import datetime, timedelta
        import pandas as pd
        
        # Use data_df as the main file to process
        processed_df = data_df.copy()
        
        # Find the appointment date column, receive date column, and insurance carrier column
        appointment_date_col = None
        receive_date_col = None
        insurance_carrier_col = None
        for col in processed_df.columns:
            if 'appointment' in col.lower() and 'date' in col.lower():
                appointment_date_col = col
            elif 'receive' in col.lower() and 'date' in col.lower():
                receive_date_col = col
            elif 'dental' in col.lower() and 'primary' in col.lower() and 'ins' in col.lower() and 'carr' in col.lower():
                insurance_carrier_col = col
        
        if appointment_date_col is None:
            return f"❌ Error: 'Appointment Date' column not found in data file.\nAvailable columns: {list(processed_df.columns)}", None
        
        # Convert appointment date column to datetime and remove time component
        try:
            processed_df[appointment_date_col] = pd.to_datetime(processed_df[appointment_date_col], errors='coerce').dt.date
        except Exception as e:
            return f"❌ Error converting appointment dates: {str(e)}", None
        
        # Check if Priority Status column exists, if not create it
        if 'Priority Status' not in processed_df.columns:
            processed_df['Priority Status'] = ''
        
        # Convert Priority Status column to object type
        processed_df['Priority Status'] = processed_df['Priority Status'].astype('object')
        
        # Build list of priority dates from selection (as strings)
        first_priority_dates = set(appointment_dates) if appointment_dates else set()
        second_priority_dates = set(appointment_dates_second) if appointment_dates_second else set()
        
        # Count statistics
        total_rows = len(processed_df)
        first_priority_count = 0
        second_priority_count = 0
        third_priority_count = 0
        invalid_dates = 0
        
        # Collect Third Priority dates
        third_priority_dates_set = set()
        
        # Process each row
        for idx, row in processed_df.iterrows():
            appointment_date = row[appointment_date_col]
            
            # Skip rows with invalid dates
            if pd.isna(appointment_date):
                processed_df.at[idx, 'Priority Status'] = 'Invalid Date'
                invalid_dates += 1
                continue
            
            # Convert appointment date to string and handle different formats
            appointment_date_str = str(appointment_date)
            
            # If it's a datetime string like '2025-11-03 00:00:00', extract just the date part
            if ' ' in appointment_date_str:
                appointment_date_str = appointment_date_str.split(' ')[0]
            
            # Convert calendar dates (YYYY-MM-DD) to YYYY-MM-DD format for comparison
            def convert_calendar_to_original_format(calendar_date):
                try:
                    from datetime import datetime
                    # Parse YYYY-MM-DD format
                    dt = datetime.strptime(calendar_date, '%Y-%m-%d')
                    # Return in YYYY-MM-DD format for comparison
                    return dt.strftime('%Y-%m-%d')
                except:
                    return calendar_date
            
            # Convert priority dates to YYYY-MM-DD format for comparison
            first_priority_dates_yyyy_mm_dd = set()
            for calendar_date in first_priority_dates:
                converted_date = convert_calendar_to_original_format(calendar_date)
                first_priority_dates_yyyy_mm_dd.add(converted_date)
            
            second_priority_dates_yyyy_mm_dd = set()
            for calendar_date in second_priority_dates:
                converted_date = convert_calendar_to_original_format(calendar_date)
                second_priority_dates_yyyy_mm_dd.add(converted_date)
            
            # Check if appointment date is in First Priority dates
            if appointment_date_str in first_priority_dates_yyyy_mm_dd:
                # Additional filtering: check receive dates if provided
                should_include = True
                if receive_dates and receive_date_col and receive_date_col in processed_df.columns:
                    receive_date = row[receive_date_col]
                    if not pd.isna(receive_date):
                        # Convert receive date to string format
                        receive_date_str = str(receive_date)
                        if ' ' in receive_date_str:
                            receive_date_str = receive_date_str.split(' ')[0]
                        
                        # Convert receive dates to YYYY-MM-DD format for comparison
                        receive_dates_yyyy_mm_dd = set()
                        for calendar_date in receive_dates:
                            converted_date = convert_calendar_to_original_format(calendar_date)
                            receive_dates_yyyy_mm_dd.add(converted_date)
                        
                        # Only include if receive date is in selected receive dates
                        if receive_date_str not in receive_dates_yyyy_mm_dd:
                            should_include = False
                
                if should_include:
                    processed_df.at[idx, 'Priority Status'] = 'First Priority'
                    first_priority_count += 1
                else:
                    # If receive date is not selected, assign to Second Priority
                    processed_df.at[idx, 'Priority Status'] = 'Second Priority'
                    second_priority_count += 1
            # Check if appointment date is in Second Priority dates
            elif appointment_date_str in second_priority_dates_yyyy_mm_dd:
                processed_df.at[idx, 'Priority Status'] = 'Second Priority'
                second_priority_count += 1
            else:
                # All remaining dates get Third Priority
                processed_df.at[idx, 'Priority Status'] = 'Third Priority'
                third_priority_count += 1
                # Add to Third Priority dates set (convert back to calendar format for display)
                try:
                    from datetime import datetime
                    dt = datetime.strptime(appointment_date_str, '%Y-%m-%d')
                    calendar_date = dt.strftime('%Y-%m-%d')
                    third_priority_dates_set.add(calendar_date)
                except:
                    # If conversion fails, use the original string
                    third_priority_dates_set.add(appointment_date_str)
        
        # Generate agent allocation summary if allocation_df is provided
        agent_summary = ""
        if allocation_df is not None:
            try:
                # Get the "main" sheet from allocation data, fallback to first sheet if "main" doesn't exist
                agent_df = None
                if 'main' in allocation_df:
                    agent_df = allocation_df['main']
                elif len(allocation_df) > 0:
                    agent_df = list(allocation_df.values())[0]
                
                if agent_df is None:
                    agent_summary = "\n⚠️ No sheets found in allocation file."
                    return processed_df, agent_summary
                
                # Find agent name, ID, counts, insurance list, exceptions, email, role, shift time, and shift group columns
                agent_name_col = None
                agent_id_col = None
                counts_col = None
                insurance_working_col = None
                insurance_needs_training_col = None
                email_col = None
                role_col = None
                shift_time_col = None
                shift_group_col = None
                for col in agent_df.columns:
                    col_lower = col.lower()
                    if 'agent' in col_lower and 'name' in col_lower:
                        agent_name_col = col
                    elif col_lower == 'id':
                        agent_id_col = col
                    elif col_lower == 'tfd':
                        counts_col = col
                    elif 'insurance' in col_lower and 'list' in col_lower:
                        insurance_working_col = col
                    elif 'exception' in col_lower:
                        insurance_needs_training_col = col
                    elif 'email' in col_lower and 'id' in col_lower:
                        email_col = col
                    elif col_lower == 'role' or col_lower == 'job role' or col_lower == 'position' or ('role' in col_lower and 'type' in col_lower):
                        role_col = col
                    elif 'shift' in col_lower and 'time' in col_lower:
                        shift_time_col = col
                    elif 'shift' in col_lower and 'group' in col_lower:
                        shift_group_col = col
                
                if agent_name_col and counts_col:
                    # Get agent data with their capacities and insurance capabilities
                    columns_to_select = [agent_name_col, counts_col]
                    if agent_id_col:
                        columns_to_select.append(agent_id_col)
                    if insurance_working_col:
                        columns_to_select.append(insurance_working_col)
                    if insurance_needs_training_col:
                        columns_to_select.append(insurance_needs_training_col)
                    if email_col:
                        columns_to_select.append(email_col)
                    if role_col:
                        columns_to_select.append(role_col)
                    if shift_time_col:
                        columns_to_select.append(shift_time_col)
                    if shift_group_col:
                        columns_to_select.append(shift_group_col)
                    
                    agent_data = agent_df[columns_to_select].dropna(subset=[agent_name_col, counts_col])
                    
                    # Filter out "Auditor" and "caller" roles
                    if role_col:
                        # Filter based on role column (case-insensitive)
                        agent_data = agent_data[~agent_data[role_col].astype(str).str.lower().str.strip().isin(['auditor', 'caller'])]
                    else:
                        # If no role column found, check if agent name column contains these roles (case-insensitive)
                        agent_data = agent_data[~agent_data[agent_name_col].astype(str).str.lower().str.strip().isin(['auditor', 'caller'])]
                    
                    # Add empty columns if not found
                    if not insurance_working_col:
                        agent_data['Insurance List'] = ''
                        insurance_working_col = 'Insurance List'
                    
                    # Detect and assign new insurance companies to senior agents
                    if insurance_carrier_col and insurance_working_col:
                        agent_data, new_insurance_companies = detect_and_assign_new_insurance_companies(
                            processed_df, agent_data, insurance_carrier_col, insurance_working_col, agent_name_col
                        )
                        if new_insurance_companies:
                            agent_summary += f"\n🆕 New insurance companies detected and assigned to senior agents: {', '.join(new_insurance_companies)}"
                    if not insurance_needs_training_col:
                        agent_data['Exceptions'] = ''
                        insurance_needs_training_col = 'Exceptions'
                    
                    total_agents = len(agent_data)
                    
                    # Calculate total capacity with proper type conversion
                    total_capacity = 0
                    for _, row in agent_data.iterrows():
                        try:
                            if pd.notna(row[counts_col]):
                                capacity = int(float(str(row[counts_col]).replace(',', '')))
                                total_capacity += capacity
                        except (ValueError, TypeError):
                            continue
                    
                    # Create capability-based allocation
                    agent_allocations = []
                    
                    # First, prepare agent data with their capabilities
                    for _, row in agent_data.iterrows():
                        agent_name = str(row[agent_name_col]).strip() if pd.notna(row[agent_name_col]) else 'Unknown'
                        
                        # Create unique agent_id: Use ID if available, otherwise use name + index as fallback
                        if agent_id_col and pd.notna(row[agent_id_col]):
                            agent_id = str(row[agent_id_col]).strip()
                        else:
                            # Fallback: Use name + row index to ensure uniqueness
                            agent_id = f"{agent_name}_{row.name}"
                        
                        # Handle different data types in counts column
                        try:
                            if pd.notna(row[counts_col]):
                                capacity = int(float(str(row[counts_col]).replace(',', '')))
                            else:
                                capacity = 0
                        except (ValueError, TypeError):
                            capacity = 0
                        
                        # Get insurance companies this agent can work with and check if senior
                        insurance_companies = []
                        is_senior = False
                        if insurance_working_col and pd.notna(row[insurance_working_col]):
                            # Split by common delimiters and clean up
                            companies_str = str(row[insurance_working_col])
                            companies = [comp.strip() for comp in companies_str.replace(',', ';').replace('|', ';').split(';') if comp.strip()]
                            
                            # Check if agent is senior
                            if any('senior' in comp.lower() for comp in companies):
                                is_senior = True
                                # For senior agents, they can work with any insurance company
                                insurance_companies = ['ALL_COMPANIES']
                            else:
                                insurance_companies = companies
                        
                        # Get insurance companies this agent needs training for
                        insurance_needs_training = []
                        if insurance_needs_training_col and pd.notna(row[insurance_needs_training_col]):
                            # Split by common delimiters and clean up
                            training_str = str(row[insurance_needs_training_col])
                            training_companies = [comp.strip() for comp in training_str.replace(',', ';').replace('|', ';').split(';') if comp.strip()]
                            insurance_needs_training = training_companies
                        
                        # Get agent email
                        agent_email = ''
                        if email_col and pd.notna(row[email_col]):
                            agent_email = str(row[email_col]).strip()
                        
                        # Get shift group (1=day, 2=afternoon, 3=night) to help parse ambiguous times
                        shift_group = None
                        if shift_group_col and pd.notna(row[shift_group_col]):
                            try:
                                shift_group = int(float(str(row[shift_group_col]).strip()))
                            except (ValueError, TypeError):
                                shift_group = None
                        
                        # Parse shift time (could be a range like "10-7pm", "1-10pm", "7-5 am")
                        shift_start_time = None
                        if shift_time_col and pd.notna(row[shift_time_col]):
                            shift_time_str = str(row[shift_time_col]).strip()
                            try:
                                from datetime import time as dt_time
                                import re
                                
                                # Try parsing various time formats
                                if isinstance(row[shift_time_col], pd.Timestamp):
                                    shift_start_time = row[shift_time_col].time()
                                elif '-' in shift_time_str:
                                    # Parse time range (e.g., "10-7pm", "1-10pm", "7-5 am", "10am-7pm")
                                    # Extract start time (first part before the dash)
                                    parts = shift_time_str.split('-')
                                    if len(parts) >= 2:
                                        start_time_str = parts[0].strip()
                                        end_time_str = parts[1].strip()
                                        
                                        # Check if end time has AM/PM indicator
                                        has_end_am = 'am' in end_time_str.lower()
                                        has_end_pm = 'pm' in end_time_str.lower()
                                        has_start_am = 'am' in start_time_str.lower()
                                        has_start_pm = 'pm' in start_time_str.lower()
                                        
                                        # Extract start hour (could be just a number like "10" or "7")
                                        start_match = re.search(r'(\d{1,2})', start_time_str)
                                        if start_match:
                                            hour = int(start_match.group(1))
                                            minute = 0  # Default to 0 minutes if not specified
                                            
                                            # Check for explicit AM/PM in start time
                                            if has_start_am:
                                                if hour == 12:
                                                    hour = 0  # 12 AM = 0
                                            elif has_start_pm:
                                                if hour != 12:
                                                    hour += 12  # Convert to 24-hour format
                                            else:
                                                # No AM/PM in start time - infer from context using Shift Group if available
                                                # Extract end hour for comparison
                                                end_match = re.search(r'(\d{1,2})', end_time_str)
                                                if end_match:
                                                    end_hour_12 = int(end_match.group(1))  # 12-hour format
                                                    
                                                    # Use Shift Group to help determine AM/PM (1=day, 2=afternoon, 3=night)
                                                    if shift_group == 1:
                                                        # Day shift: typically starts in AM (morning, e.g., 8-5pm, 10-7pm)
                                                        # If hour >= end_hour_12, it's likely AM (day shift starts morning)
                                                        if hour >= end_hour_12:
                                                            pass  # Keep as AM
                                                        else:
                                                            # If start < end, could still be AM for day shift
                                                            pass  # Keep as AM
                                                    elif shift_group == 2:
                                                        # Afternoon shift: typically starts in PM (afternoon, e.g., 1-10pm, 3-6pm)
                                                        # But can also start in late AM and extend into evening (e.g., 11-8pm = 11 AM to 8 PM)
                                                        if hour >= end_hour_12:
                                                            # Start >= end, likely AM (e.g., "11-8pm" = 11 AM to 8 PM)
                                                            pass  # Keep as AM
                                                        else:
                                                            # Start < end, likely PM (e.g., "1-10pm" = 1 PM to 10 PM, "6-10pm" = 6 PM to 10 PM)
                                                            if hour != 12:
                                                                hour += 12  # Convert to PM
                                                    elif shift_group == 3:
                                                        # Night shift: can start in PM and go into AM (e.g., 7-5 am, 10-7am)
                                                        if has_end_am:
                                                            # End is AM - if start >= end, it's overnight (start is PM)
                                                            if hour >= end_hour_12:
                                                                if hour != 12:
                                                                    hour += 12  # Overnight shift - start is PM
                                                            else:
                                                                # Start < end AM, likely same day AM
                                                                if hour == 12:
                                                                    hour = 0
                                                        elif has_end_pm:
                                                            # End is PM - night shift might start late PM
                                                            if hour < end_hour_12:
                                                                if hour != 12:
                                                                    hour += 12  # Late PM start
                                                            else:
                                                                # Hour >= end, might be early AM (unusual but possible)
                                                                pass  # Keep as AM
                                                    
                                                    # If no shift group, use original logic
                                                    if shift_group is None:
                                                        # Infer start time AM/PM using original heuristics
                                                        if has_end_am:
                                                            # End is AM (e.g., "7-5 am", "10-6 am")
                                                            # If start hour >= end hour, it's likely an overnight shift (start is PM)
                                                            # If start hour < end hour, it's likely same day (start is AM)
                                                            if hour >= end_hour_12:
                                                                # Overnight shift - start is PM (e.g., "7-5 am" = 7 PM to 5 AM)
                                                                if hour != 12:
                                                                    hour += 12  # Convert to PM (24-hour format)
                                                                # If hour is 12, it's 12 PM (noon)
                                                            else:
                                                                # Same day shift - start is AM (e.g., "2-5 am" = 2 AM to 5 AM)
                                                                if hour == 12:
                                                                    hour = 0  # 12 AM
                                                                # Otherwise keep hour as is (AM)
                                                        elif has_end_pm:
                                                            # End is PM
                                                            # If start hour < end hour (both in 12-hour format), likely both PM (e.g., "1-10pm")
                                                            # If start hour >= end hour, likely start AM and end PM (e.g., "10-7pm", "9-5pm")
                                                            if hour >= end_hour_12 and hour < 12:
                                                                # Start hour is >= end hour, likely AM (day shift)
                                                                pass  # Keep as is (AM)
                                                            elif hour < end_hour_12:
                                                                # Start hour < end hour, likely PM (afternoon/evening shift)
                                                                if hour != 12:
                                                                    hour += 12
                                                            elif hour == 12:
                                                                # Start is 12, check if end is also 12 or less
                                                                if end_hour_12 == 12:
                                                                    hour = 12  # Noon to noon (unlikely but handle it)
                                                                else:
                                                                    hour = 12  # 12 PM (noon)
                                            
                                            # Check for minutes in start time (e.g., "10:30-7pm")
                                            minute_match = re.search(r':(\d{2})', start_time_str)
                                            if minute_match:
                                                minute = int(minute_match.group(1))
                                            
                                            shift_start_time = dt_time(hour % 24, minute)
                                        
                                elif ':' in shift_time_str:
                                    # Parse single time string (e.g., "09:00", "9:00 AM", "09:00:00")
                                    time_match = re.search(r'(\d{1,2}):(\d{2})', shift_time_str)
                                    if time_match:
                                        hour = int(time_match.group(1))
                                        minute = int(time_match.group(2))
                                        # Check for AM/PM
                                        if 'pm' in shift_time_str.lower() and hour != 12:
                                            hour += 12
                                        elif 'am' in shift_time_str.lower() and hour == 12:
                                            hour = 0
                                        shift_start_time = dt_time(hour, minute)
                            except Exception as e:
                                shift_start_time = None
                        
                        # Store original shift time for admin review
                        original_shift_time = None
                        if shift_time_col and pd.notna(row[shift_time_col]):
                            original_shift_time = str(row[shift_time_col]).strip()
                        
                        agent_allocations.append({
                            'id': agent_id,  # Unique identifier (ID column or name + index)
                            'name': agent_name,  # Display name
                            'capacity': capacity,
                            'allocated': 0,
                            'email': agent_email,
                            'insurance_companies': insurance_companies,
                            'insurance_needs_training': insurance_needs_training,
                            'is_senior': is_senior,
                            'shift_start_time': shift_start_time.strftime('%H:%M') if shift_start_time else None,  # Store as HH:MM string
                            'shift_time_original': original_shift_time,  # Original shift time value from Excel
                            'shift_group': shift_group,  # Shift group (1=day, 2=afternoon, 3=night)
                            'row_indices': []
                        })
                    
                    # Now allocate rows based on insurance company matching and priority
                    unmatched_insurance_companies = set()  # Initialize for use in summary
                    if insurance_carrier_col:
                        # Step 1: Identify all insurance companies in the data and all agent insurance companies
                        all_data_insurance_companies = set()
                        all_agent_insurance_companies = set()
                        
                        for idx, row in processed_df.iterrows():
                            insurance_carrier = str(row[insurance_carrier_col]).strip() if pd.notna(row[insurance_carrier_col]) else 'Unknown'
                            if insurance_carrier and insurance_carrier.lower() != 'unknown':
                                all_data_insurance_companies.add(insurance_carrier)
                        
                        # Collect all insurance companies from non-senior agents (normalize to lowercase for comparison)
                        agent_insurance_lower = set()
                        for agent in agent_allocations:
                            if not agent['is_senior'] and agent['insurance_companies']:
                                for comp in agent['insurance_companies']:
                                    if comp != 'ALL_COMPANIES':
                                        agent_insurance_lower.add(comp.strip().lower())
                        
                        # Identify unmatched insurance companies (not in any non-senior agent's list)
                        # Compare case-insensitively
                        unmatched_insurance_companies = set()
                        for data_comp in all_data_insurance_companies:
                            data_comp_lower = data_comp.lower()
                            # Check if this insurance company matches any agent's insurance companies
                            is_matched = False
                            for agent_comp_lower in agent_insurance_lower:
                                if data_comp_lower in agent_comp_lower or agent_comp_lower in data_comp_lower:
                                    is_matched = True
                                    break
                            if not is_matched:
                                unmatched_insurance_companies.add(data_comp)
                        
                        # Get all senior agents
                        senior_agents = [a for a in agent_allocations if a['is_senior']]
                        
                        # Step 2: Group data by insurance carrier and priority
                        data_by_insurance_priority = {}
                        unmatched_data_by_priority = {}
                        matched_data_by_insurance_priority = {}
                        
                        for idx, row in processed_df.iterrows():
                            insurance_carrier = str(row[insurance_carrier_col]).strip() if pd.notna(row[insurance_carrier_col]) else 'Unknown'
                            priority = row.get('Priority Status', 'Unknown')
                            
                            if insurance_carrier.lower() == 'unknown' or not insurance_carrier:
                                insurance_carrier = 'Unknown'
                            
                            # Separate unmatched and matched insurance companies
                            # Unknown insurance is always unmatched (senior only)
                            # First Priority is always senior only (for both matched and unmatched)
                            is_unmatched = (insurance_carrier in unmatched_insurance_companies or 
                                          insurance_carrier == 'Unknown')
                            
                            if is_unmatched:
                                # Store unmatched insurance companies separately (highest priority)
                                if insurance_carrier not in unmatched_data_by_priority:
                                    unmatched_data_by_priority[insurance_carrier] = {}
                                if priority not in unmatched_data_by_priority[insurance_carrier]:
                                    unmatched_data_by_priority[insurance_carrier][priority] = []
                                unmatched_data_by_priority[insurance_carrier][priority].append(idx)
                            else:
                                # Store matched insurance companies normally
                                if insurance_carrier not in matched_data_by_insurance_priority:
                                    matched_data_by_insurance_priority[insurance_carrier] = {}
                                if priority not in matched_data_by_insurance_priority[insurance_carrier]:
                                    matched_data_by_insurance_priority[insurance_carrier][priority] = []
                                matched_data_by_insurance_priority[insurance_carrier][priority].append(idx)
                            
                            # Also keep full data structure for reference
                            if insurance_carrier not in data_by_insurance_priority:
                                data_by_insurance_priority[insurance_carrier] = {}
                            if priority not in data_by_insurance_priority[insurance_carrier]:
                                data_by_insurance_priority[insurance_carrier][priority] = []
                            data_by_insurance_priority[insurance_carrier][priority].append(idx)
                        
                        # Initialize INS and Toolkit group tracking (used across all allocation steps)
                        ins_group_allocations = {}  # {agent_name: count}
                        toolkit_group_allocations = {}  # {agent_name: count}
                        ins_group_companies = set(DD_INS_GROUP)
                        toolkit_group_companies = set(DD_TOOLKIT_GROUP)
                        
                        # Identify agents with INS or Toolkit groups
                        # After expansion, DD INS/INS becomes the actual companies, so we check if agent has any DD_INS_GROUP companies
                        agents_with_ins = []
                        agents_with_toolkit = []
                        
                        # Convert to sets for faster lookup
                        ins_group_set = set([c.upper() for c in DD_INS_GROUP])
                        toolkit_group_set = set([c.upper() for c in DD_TOOLKIT_GROUP])
                        
                        for agent in agent_allocations:
                            insurance_list = agent.get('insurance_companies', [])
                            agent_id = agent.get('id', agent.get('name', 'Unknown'))
                            agent_name = agent.get('name', 'Unknown')
                            
                            if insurance_list:
                                # Convert to uppercase set for comparison
                                agent_insurance_set = set([c.upper().strip() for c in insurance_list if c and c != 'ALL_COMPANIES'])
                                
                                # Debug: Show first few agents' insurance companies
                                if len(agents_with_ins) + len(agents_with_toolkit) < 5:
                                    pass
                                
                                # Check if agent has any DD_INS_GROUP companies
                                has_ins_group = bool(agent_insurance_set.intersection(ins_group_set))
                                if has_ins_group:
                                    agents_with_ins.append(agent_name)
                                    ins_group_allocations[agent_id] = 0
                                
                                # Check if agent has any DD_TOOLKIT_GROUP companies
                                has_toolkit_group = bool(agent_insurance_set.intersection(toolkit_group_set))
                                if has_toolkit_group:
                                    agents_with_toolkit.append(agent_name)
                                    toolkit_group_allocations[agent_id] = 0
                            else:
                                if len(agents_with_ins) + len(agents_with_toolkit) < 5:
                                    pass
                        
                        
                        # Step 3: FIRST PRIORITY - Allocate First Priority matched work to senior agents FIRST
                        # This takes precedence over unmatched insurance
                        
                        # Check senior agent remaining capacity before Step 3
                        senior_capacity_before = sum(a['capacity'] - a['allocated'] for a in senior_agents)
                        
                        # ONLY process First Priority matched work for senior agents
                        priority = 'First Priority'
                        # Collect all unallocated First Priority matched work across all insurance carriers
                        priority_work = []  # List of (insurance_carrier, row_index) tuples
                        for insurance_carrier, priority_data in matched_data_by_insurance_priority.items():
                            if priority in priority_data:
                                row_indices = priority_data[priority]
                                # Get unallocated indices for this insurance carrier and priority
                                unallocated_indices = [idx for idx in row_indices if idx not in [i for ag in agent_allocations for i in ag['row_indices']]]
                                for idx in unallocated_indices:
                                    priority_work.append((insurance_carrier, idx))
                        
                        if priority_work:
                            pass
                            
                            # Allocate all First Priority matched work to senior agents, maximizing capacity utilization
                            work_idx = 0
                            while work_idx < len(priority_work):
                                # Check if any senior agent has capacity
                                available_seniors = [a for a in senior_agents if (a['capacity'] - a['allocated']) > 0]
                                if not available_seniors:
                                    # No more senior capacity for First Priority
                                    remaining = len(priority_work) - work_idx
                                    break
                                
                                # Sort by remaining capacity (highest first) to fill largest capacity first
                                available_seniors.sort(key=lambda x: x['capacity'] - x['allocated'], reverse=True)
                                
                                # Allocate to senior agents in batches to maximize capacity utilization
                                for senior_agent in available_seniors:
                                    if work_idx >= len(priority_work):
                                        break
                                    
                                    available_capacity = senior_agent['capacity'] - senior_agent['allocated']
                                    if available_capacity <= 0:
                                        continue
                                    
                                    # Calculate how many rows to assign to this agent (use all available capacity)
                                    remaining_work = len(priority_work) - work_idx
                                    rows_to_assign = min(available_capacity, remaining_work)
                                    
                                    if rows_to_assign > 0:
                                        # Allocate batch of rows to this senior agent
                                        agent_id = senior_agent.get('id', senior_agent.get('name', 'Unknown'))
                                        for i in range(rows_to_assign):
                                            insurance_carrier, row_idx = priority_work[work_idx + i]
                                            senior_agent['row_indices'].append(row_idx)
                                            
                                            # Track INS and Toolkit group allocations (case-insensitive)
                                            if agent_id in ins_group_allocations:
                                                insurance_carrier_upper = insurance_carrier.upper().strip()
                                                if any(insurance_carrier_upper == ic.upper().strip() for ic in DD_INS_GROUP):
                                                    ins_group_allocations[agent_id] += 1
                                            if agent_id in toolkit_group_allocations:
                                                insurance_carrier_upper = insurance_carrier.upper().strip()
                                                if any(insurance_carrier_upper == ic.upper().strip() for ic in DD_TOOLKIT_GROUP):
                                                    toolkit_group_allocations[agent_id] += 1
                                        
                                        senior_agent['allocated'] += rows_to_assign
                                        work_idx += rows_to_assign
                                        
                                        # Log the allocation
                                
                                # Log progress
                                if work_idx % 50 == 0 and work_idx < len(priority_work):
                                    pass
                            
                        else:
                            pass
                        
                        # Note: Second/Third Priority matched work will be allocated to non-seniors in Step 5
                        senior_capacity_after = sum(a['capacity'] - a['allocated'] for a in senior_agents)
                        
                        # Step 4: Allocate unmatched insurance companies to senior agents (after First Priority matched work)
                        if unmatched_insurance_companies and senior_agents:
                            for insurance_carrier, priority_data in unmatched_data_by_priority.items():
                                # Process by priority order
                                for priority in ['First Priority', 'Second Priority', 'Third Priority']:
                                    if priority in priority_data:
                                        row_indices = priority_data[priority]
                                        
                                        # Only senior agents can handle unmatched insurance
                                        available_senior_agents = [a for a in senior_agents if (a['capacity'] - a['allocated']) > 0]
                                        
                                        if available_senior_agents:
                                            # Distribute unmatched insurance rows among senior agents by priority
                                            # Sort by remaining capacity (highest first)
                                            available_senior_agents.sort(key=lambda x: x['capacity'] - x['allocated'], reverse=True)
                                            
                                            # Allocate to senior agents up to their capacity
                                            row_idx = 0
                                            for senior_agent in available_senior_agents:
                                                if row_idx >= len(row_indices):
                                                    break
                                                
                                                available_capacity = senior_agent['capacity'] - senior_agent['allocated']
                                                if available_capacity > 0:
                                                    rows_to_assign = min(available_capacity, len(row_indices) - row_idx)
                                                    if rows_to_assign > 0:
                                                        agent_id = senior_agent.get('id', senior_agent.get('name', 'Unknown'))
                                                        # Track INS and Toolkit group allocations (case-insensitive)
                                                        insurance_carrier_upper = insurance_carrier.upper().strip()
                                                        for assigned_idx in range(row_idx, row_idx + rows_to_assign):
                                                            if agent_id in ins_group_allocations:
                                                                if any(insurance_carrier_upper == ic.upper().strip() for ic in DD_INS_GROUP):
                                                                    ins_group_allocations[agent_id] += 1
                                                            if agent_id in toolkit_group_allocations:
                                                                if any(insurance_carrier_upper == ic.upper().strip() for ic in DD_TOOLKIT_GROUP):
                                                                    toolkit_group_allocations[agent_id] += 1
                                                        
                                                        senior_agent['row_indices'].extend(row_indices[row_idx:row_idx + rows_to_assign])
                                                        senior_agent['allocated'] += rows_to_assign
                                                        row_idx += rows_to_assign
                                            
                                            # If there are remaining unmatched rows that couldn't fit in senior capacity
                                            # they will be handled later or logged
                                            if row_idx < len(row_indices):
                                                pass
                        
                        # Step 5: Allocate remaining matched insurance companies to capable agents (normal allocation)
                        
                        for insurance_carrier, priority_data in matched_data_by_insurance_priority.items():
                            # Process First Priority first (senior agents get priority)
                            for priority in ['First Priority', 'Second Priority', 'Third Priority']:
                                if priority in priority_data:
                                    row_indices = priority_data[priority]
                                    
                                    # Filter out already allocated rows
                                    unallocated_row_indices = [idx for idx in row_indices if idx not in [i for ag in agent_allocations for i in ag['row_indices']]]
                                    
                                    if not unallocated_row_indices:
                                        continue
                                    
                                    # For First Priority and Unknown insurance, ONLY consider senior agents
                                    # For Second/Third Priority, EXCLUDE senior agents (they should only get First Priority)
                                    if priority == 'First Priority' or insurance_carrier == 'Unknown':
                                        # First Priority and Unknown: ONLY senior agents
                                        agents_to_check = [a for a in agent_allocations if a['is_senior']]
                                        if not agents_to_check:
                                            continue
                                    else:
                                        # Second/Third Priority: EXCLUDE senior agents - they should only handle First Priority work
                                        agents_to_check = [a for a in agent_allocations if not a['is_senior']]
                                        if not agents_to_check:
                                            continue
                                    
                                    # Find agents who can work with this insurance company
                                    capable_agents = []
                                    for agent in agents_to_check:
                                        # Skip if agent is at capacity
                                        if agent['capacity'] - agent['allocated'] <= 0:
                                            continue
                                        
                                        # Check if agent can work with this insurance company
                                        can_work = False
                                        
                                        # Senior agents can work with any insurance company
                                        if agent['is_senior']:
                                            can_work = True
                                        elif not agent['insurance_companies']:  # If no specific companies listed, can work with any
                                            can_work = True
                                        else:
                                            # Check if insurance carrier matches any of the agent's working companies
                                            for comp in agent['insurance_companies']:
                                                if (insurance_carrier.lower() in comp.lower() or 
                                                    comp.lower() in insurance_carrier.lower() or
                                                    insurance_carrier == comp):
                                                    can_work = True
                                                    break
                                        
                                        # Check if agent needs training for this insurance company
                                        needs_training = False
                                        if agent['insurance_needs_training']:
                                            for training_comp in agent['insurance_needs_training']:
                                                if (insurance_carrier.lower() in training_comp.lower() or 
                                                    training_comp.lower() in insurance_carrier.lower() or
                                                    insurance_carrier == training_comp):
                                                    needs_training = True
                                                    break
                                        
                                        # Agent is capable only if they can work AND don't need training
                                        if can_work and not needs_training:
                                            capable_agents.append(agent)
                                    
                                    if capable_agents:
                                        # For First Priority and Unknown, verify we only have seniors
                                        if priority == 'First Priority' or insurance_carrier == 'Unknown':
                                            # Double-check: filter to only seniors with capacity
                                            available_senior = [a for a in capable_agents if a['is_senior'] and (a['capacity'] - a['allocated']) > 0]
                                            if available_senior:
                                                capable_agents = available_senior
                                            else:
                                                # No senior capacity available - skip allocation (keep unassigned)
                                                continue
                                        
                                        # Distribute rows among capable agents
                                        rows_per_agent = len(unallocated_row_indices) // len(capable_agents)
                                        remaining_rows = len(unallocated_row_indices) % len(capable_agents)
                                        
                                        row_idx = 0
                                        for i, agent in enumerate(capable_agents):
                                            # Calculate how many rows this agent should get
                                            agent_rows = rows_per_agent
                                            if i < remaining_rows:
                                                agent_rows += 1
                                            
                                            # Ensure we don't exceed agent's capacity
                                            available_capacity = agent['capacity'] - agent['allocated']
                                            actual_rows = min(agent_rows, available_capacity, len(unallocated_row_indices) - row_idx)
                                            
                                            if actual_rows > 0:
                                                # Assign specific row indices to this agent
                                                agent['row_indices'].extend(unallocated_row_indices[row_idx:row_idx + actual_rows])
                                                agent['allocated'] += actual_rows
                                                
                                                # Track INS and Toolkit group allocations (case-insensitive)
                                                agent_id = agent.get('id', agent.get('name', 'Unknown'))
                                                insurance_carrier_upper = insurance_carrier.upper().strip()
                                                if agent_id in ins_group_allocations:
                                                    # Check if this insurance carrier is in DD_INS_GROUP
                                                    if any(insurance_carrier_upper == ic.upper().strip() for ic in DD_INS_GROUP):
                                                        ins_group_allocations[agent_id] += actual_rows
                                                if agent_id in toolkit_group_allocations:
                                                    # Check if this insurance carrier is in DD_TOOLKIT_GROUP
                                                    if any(insurance_carrier_upper == ic.upper().strip() for ic in DD_TOOLKIT_GROUP):
                                                        toolkit_group_allocations[agent_id] += actual_rows
                                                
                                                row_idx += actual_rows
                    else:
                        # Fallback: if no insurance carrier column, use simple capacity-based allocation
                        row_index = 0
                        for agent in agent_allocations:
                            if row_index >= total_rows:
                                break
                            available_capacity = agent['capacity']
                            actual_allocation = min(available_capacity, total_rows - row_index)
                            if actual_allocation > 0:
                                agent['row_indices'] = list(range(row_index, row_index + actual_allocation))
                                agent['allocated'] = actual_allocation
                                row_index += actual_allocation
                    
                    # Sort agents by name for display
                    agent_allocations.sort(key=lambda x: x['name'])
                    
                    # Calculate total allocated rows
                    total_allocated = sum(agent['allocated'] for agent in agent_allocations)
                    
                    # Print INS and Toolkit group allocation summary
                    # Ensure dictionaries exist (they should be initialized earlier)
                    if 'ins_group_allocations' not in locals():
                        ins_group_allocations = {}
                    if 'toolkit_group_allocations' not in locals():
                        toolkit_group_allocations = {}
                    
                    if ins_group_allocations:
                        total_ins = sum(ins_group_allocations.values())
                        # Create mapping from agent_id to agent_name for display
                        agent_id_to_name = {a.get('id', a.get('name')): a.get('name') for a in agent_allocations}
                        for agent_id, count in sorted(ins_group_allocations.items()):
                            agent_name = agent_id_to_name.get(agent_id, agent_id)
                            pass
                        if total_ins == 0:
                            pass
                    else:
                        pass
                    
                    if toolkit_group_allocations:
                        total_toolkit = sum(toolkit_group_allocations.values())
                        # Create mapping from agent_id to agent_name for display
                        agent_id_to_name = {a.get('id', a.get('name')): a.get('name') for a in agent_allocations}
                        for agent_id, count in sorted(toolkit_group_allocations.items()):
                            agent_name = agent_id_to_name.get(agent_id, agent_id)
                            pass
                        if total_toolkit == 0:
                            pass
                    else:
                        pass
                    
                    # Add Agent Name column to processed_df based on allocation
                    # Initialize Agent Name column if it doesn't exist
                    if 'Agent Name' not in processed_df.columns:
                        processed_df['Agent Name'] = ''
                    
                    # Set agent name for each allocated row
                    for agent in agent_allocations:
                        agent_name = agent['name']
                        row_indices = agent.get('row_indices', [])
                        if row_indices:
                            # Filter to only valid indices within the dataframe
                            valid_indices = [idx for idx in row_indices if idx < len(processed_df)]
                            if valid_indices:
                                # Set agent name for all rows allocated to this agent
                                processed_df.loc[valid_indices, 'Agent Name'] = agent_name
                    
                    # Store agent allocations data globally for individual downloads
                    agent_allocations_data = agent_allocations
                    
                    # Also store for reminder system
                    global agent_allocations_for_reminders
                    agent_allocations_for_reminders = agent_allocations
                    
                    # Calculate allocation statistics
                    total_allocated = sum(a['allocated'] for a in agent_allocations)
                    agents_with_work = len([a for a in agent_allocations if a['allocated'] > 0])
                    
                    # Get unmatched insurance companies info (if it exists from allocation process)
                    unmatched_info = ""
                    if insurance_carrier_col and unmatched_insurance_companies:
                        unmatched_info = f"\n🔴 Unmatched Insurance Companies ({len(unmatched_insurance_companies)}): {', '.join(sorted(list(unmatched_insurance_companies))[:5])}{'...' if len(unmatched_insurance_companies) > 5 else ''}\n   ⚠️ These companies were assigned ONLY to senior agents with highest priority."
                    
                    agent_summary = f"""
👥 Agent Allocation Summary (Capability-Based):
- Total Agents: {total_agents}
- Agents with Work: {agents_with_work}
- Total Rows to Allocate: {total_rows}
- Total Allocated: {total_allocated}
- Remaining Unallocated: {total_rows - total_allocated}
- Insurance Matching: {'Enabled' if insurance_carrier_col else 'Disabled'}
{unmatched_info}

📋 Agent Allocation Details:
"""
                    for i, agent in enumerate(agent_allocations):
                        insurance_info = ""
                        senior_info = " (Senior Agent)" if agent['is_senior'] else ""
                        
                        if agent['is_senior']:
                            insurance_info = " (Can work: Any insurance company)"
                        elif agent['insurance_companies']:
                            insurance_info = f" (Can work: {', '.join(agent['insurance_companies'][:2])}{'...' if len(agent['insurance_companies']) > 2 else ''})"
                        
                        if agent['insurance_needs_training']:
                            training_info = f" (Needs training: {', '.join(agent['insurance_needs_training'][:2])}{'...' if len(agent['insurance_needs_training']) > 2 else ''})"
                            insurance_info += training_info
                        
                        agent_summary += f"  {i+1}. {agent['name']}: {agent['allocated']}/{agent['capacity']} rows{senior_info}{insurance_info}\n"
                    
                    # Calculate priority distribution based on actual allocations
                    total_allocated = sum(a['allocated'] for a in agent_allocations)
                    if total_allocated > 0:
                        agent_summary += f"""
📊 Priority Distribution (Based on Actual Allocations):
- First Priority: {first_priority_count} rows total
- Second Priority: {second_priority_count} rows total  
- Third Priority: {third_priority_count} rows total

⚠️ Note: Priority distribution will be proportional to each agent's allocated capacity.
"""
                    else:
                        agent_summary += "\n⚠️ No rows could be allocated due to capacity constraints."
                        
                elif not agent_name_col:
                    agent_summary = "\n⚠️ Agent Name column not found in allocation file."
                elif not counts_col:
                    agent_summary = "\n⚠️ TFD column not found in allocation file."
                
                # Add information about insurance matching
                if insurance_carrier_col and insurance_working_col:
                    training_info = f" and '{insurance_needs_training_col}'" if insurance_needs_training_col else ""
                    agent_summary += f"\n✅ Insurance capability matching enabled using '{insurance_working_col}'{training_info} and '{insurance_carrier_col}' columns."
                elif insurance_carrier_col and not insurance_working_col:
                    agent_summary += f"\n⚠️ Insurance carrier column '{insurance_carrier_col}' found, but 'Insurance List' column not found in allocation file."
                elif not insurance_carrier_col and insurance_working_col:
                    agent_summary += f"\n⚠️ 'Insurance List' column found, but 'Dental Primary Ins Carr' column not found in data file."
                else:
                    agent_summary += f"\nℹ️ Insurance capability matching disabled - using simple capacity-based allocation."
                
                # Add information about training filtering
                if insurance_needs_training_col:
                    agent_summary += f"\n🎓 Training-based filtering enabled - agents will not be assigned work for insurance companies they need training for."
                
                # Add information about senior agents
                senior_count = sum(1 for agent in agent_allocations if agent['is_senior'])
                if senior_count > 0:
                    unmatched_note = f" Unmatched insurance companies ({len(unmatched_insurance_companies)}) are assigned ONLY to senior agents with highest priority." if unmatched_insurance_companies else ""
                    agent_summary += f"\n👑 Senior agents detected: {senior_count} - Senior agents can work with any insurance company and get priority for First Priority cases.{unmatched_note}"
            except Exception as e:
                agent_summary = f"\n⚠️ Error processing agent allocation: {str(e)}"
        
        # Generate result message
        first_priority_dates_list = sorted(list(first_priority_dates))
        second_priority_dates_list = sorted(list(second_priority_dates))
        third_priority_dates_list = sorted(list(third_priority_dates_set))
        first_priority_dates_str = ', '.join(first_priority_dates_list) if first_priority_dates_list else 'None'
        second_priority_dates_str = ', '.join(second_priority_dates_list) if second_priority_dates_list else 'None'
        third_priority_dates_str = ', '.join(third_priority_dates_list) if third_priority_dates_list else 'None'
        
        result_message = f"""✅ Priority processing completed successfully!

📊 Processing Statistics:
- Total rows processed: {total_rows}
- First Priority: {first_priority_count} rows
- Second Priority: {second_priority_count} rows
- Third Priority: {third_priority_count} rows
- Invalid dates: {invalid_dates} rows

📅 Selected First Priority Dates: {first_priority_dates_str}
📅 Selected Second Priority Dates: {second_priority_dates_str}
📅 Third Priority Dates: {third_priority_dates_str}

📋 Updated column: 'Priority Status'
📅 Based on column: '{appointment_date_col}'{agent_summary}

💾 Ready to download the processed result file!"""
        
        return result_message, processed_df
        
    except Exception as e:
        return f"❌ Error during processing: {str(e)}", None

@app.route('/')
@login_required
def index():
    global allocation_data, data_file_data, allocation_filename, data_filename, processing_result
    global agent_processing_result, agent_allocations_data
    
    # Get current user
    user = get_user_by_username(session.get('user_id'))
    
    # Load agent work files if user is an agent
    agent_work_files = None
    if user and user.role == 'agent':
        agent_work_files = get_agent_work_files(user.id)
    
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    # Load all agent work files for admin view
    all_agent_work_files = None
    if user and user.role == 'admin':
        all_agent_work_files = get_all_agent_work_files()
    
    return render_template_string(HTML_TEMPLATE, 
                                allocation_data=allocation_data, 
                                data_file_data=data_file_data,
                                allocation_filename=allocation_filename,
                                data_filename=data_filename,
                                processing_result=processing_result,
                                agent_processing_result=agent_processing_result,
                                agent_allocations_data=agent_allocations_data,
                                agent_work_files=agent_work_files,
                                all_agent_work_files=all_agent_work_files,
                                current_time=current_time)

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')
        
        # Try database authentication first
        user = get_user_by_username(username)
        if user and user.check_password(password):
            # Update last login
            user.last_login = datetime.utcnow()
            db.session.commit()
            
            # Create database session
            session_data = {
                'user_id': user.username,
                'user_role': user.role,
                'user_name': user.name,
                'user_email': user.email
            }
            db_session = create_user_session(user.id, session_data)
            
            # Set Flask session
            session['db_session_id'] = db_session.id
            session.update(session_data)
            
            return redirect(url_for('dashboard'))
        else:
            flash('Invalid username or password. Please try again.', 'error')
    
    return render_template_string(LOGIN_TEMPLATE, GOOGLE_CLIENT_ID=GOOGLE_CLIENT_ID)

@app.route('/google-login')
def google_login():
    """Initiate Google OAuth login"""
    if not GOOGLE_CLIENT_ID:
        flash('Google OAuth is not configured. Please contact administrator to set up Google OAuth for agent login.', 'error')
        return redirect(url_for('login'))
    
    # Get Google OAuth configuration
    google_provider_cfg = get_google_provider_cfg()
    if not google_provider_cfg:
        flash('Unable to connect to Google OAuth service. Please check your internet connection and try again.', 'error')
        return redirect(url_for('login'))
    
    authorization_endpoint = google_provider_cfg["authorization_endpoint"]
    
    # Create request URI
    request_uri = f"{authorization_endpoint}?client_id={GOOGLE_CLIENT_ID}&redirect_uri={request.url_root}callback&scope=openid email profile&response_type=code"
    
    return redirect(request_uri)

@app.route('/callback')
def callback():
    """Handle Google OAuth callback"""
    if not GOOGLE_CLIENT_ID or not GOOGLE_CLIENT_SECRET:
        flash('Google OAuth is not configured. Please contact administrator.', 'error')
        return redirect(url_for('login'))
    
    # Get authorization code from the request
    code = request.args.get("code")
    if not code:
        flash('Authorization failed. Please try again.', 'error')
        return redirect(url_for('login'))
    
    try:
        # Get Google OAuth configuration
        google_provider_cfg = get_google_provider_cfg()
        if not google_provider_cfg:
            flash('Unable to connect to Google OAuth service. Please try again later.', 'error')
            return redirect(url_for('login'))
        
        token_endpoint = google_provider_cfg["token_endpoint"]
        
        # Exchange code for token
        token_data = {
            'code': code,
            'client_id': GOOGLE_CLIENT_ID,
            'client_secret': GOOGLE_CLIENT_SECRET,
            'redirect_uri': request.base_url,
            'grant_type': 'authorization_code'
        }
        
        token_response = req.post(
            token_endpoint,
            data=token_data,
            headers={'Content-Type': 'application/x-www-form-urlencoded'}
        )
        
        # Parse the tokens
        if token_response.status_code != 200:
            flash('Failed to exchange authorization code for token. Please try again.', 'error')
            return redirect(url_for('login'))
            
        tokens = token_response.json()
        
        if 'id_token' not in tokens:
            flash('No ID token received from Google. Please try again.', 'error')
            return redirect(url_for('login'))
        
        # Verify the token
        google_user_info = verify_google_token(tokens['id_token'])
        
        if not google_user_info:
            flash('Token verification failed. Please try again.', 'error')
            return redirect(url_for('login'))
        
        # Get or create user
        user = get_or_create_google_user(google_user_info)
        
        if not user.is_active:
            flash('Your account is inactive. Please contact administrator.', 'error')
            return redirect(url_for('login'))
        
        # Update last login
        user.last_login = datetime.utcnow()
        db.session.commit()
        
        # Create database session
        session_data = {
            'user_id': user.email,  # Use email as user_id for OAuth users
            'user_role': user.role,
            'user_name': user.name,
            'user_email': user.email
        }
        db_session = create_user_session(user.id, session_data)
        
        # Set Flask session
        session['db_session_id'] = db_session.id
        session.update(session_data)
        
        return redirect(url_for('dashboard'))
        
    except Exception as e:
        flash('Authentication failed. Please try again.', 'error')
        return redirect(url_for('login'))

@app.route('/logout')
def logout():
    # Clean up database session
    db_session_id = session.get('db_session_id')
    if db_session_id:
        delete_user_session(db_session_id)
    
    # Clear Flask session
    session.clear()
    flash('You have been logged out successfully.', 'success')
    return redirect(url_for('login'))

@app.route('/dashboard')
@login_required
def dashboard():
    return redirect(url_for('index'))


@app.route('/upload_allocation', methods=['POST'])
@admin_required
def upload_allocation_file():
    global allocation_data, allocation_filename, processing_result
    
    if 'file' not in request.files:
        flash('No file provided', 'error')
        return redirect('/')
    
    file = request.files['file']
    if file.filename == '':
        flash('No file selected', 'error')
        return redirect('/')
    
    try:
        # Save uploaded file temporarily
        filename = secure_filename(file.filename)
        file.save(filename)
        
        # Load Excel file
        allocation_data = pd.read_excel(filename, sheet_name=None)
        
        # Focus on "main" sheet if it exists, otherwise use all sheets
        sheets_to_process = {}
        if 'main' in allocation_data:
            sheets_to_process['main'] = allocation_data['main']
        else:
            sheets_to_process = allocation_data
        
        # Format insurance company names in "Insurance List" column for better allocation matching
        for sheet_name, df in sheets_to_process.items():
            # Find the Insurance List column (case-insensitive)
            insurance_working_col = None
            for col in df.columns:
                if 'insurance' in col.lower() and 'list' in col.lower():
                    insurance_working_col = col
                    break
            
            if insurance_working_col:
                # Format each value in Insurance List column (which may contain multiple companies separated by ; or ,)
                def format_insurance_list(value):
                    if pd.isna(value):
                        return value
                    value_str = str(value)
                    # Split by common delimiters
                    companies = [comp.strip() for comp in re.split(r'[;,\|]', value_str) if comp.strip()]
                    # Format each company name, but preserve "senior" keyword and group names for expansion
                    formatted_companies = []
                    for comp in companies:
                        comp_lower = comp.lower()
                        if 'senior' in comp_lower:
                            formatted_companies.append(comp)  # Keep senior as-is
                        elif (comp_lower == 'dd ins' or comp_lower == 'ins' or 
                              comp_lower == 'dd toolkit' or comp_lower == 'dd toolkits' or 
                              comp_lower == 'dd'):
                            # Keep group names as-is for later expansion
                            formatted_companies.append(comp)
                        else:
                            formatted = format_insurance_company_name(comp)
                            formatted_companies.append(formatted)
                    # Join back with semicolon
                    return '; '.join(formatted_companies)
                
                # First format the insurance names
                df[insurance_working_col] = df[insurance_working_col].apply(format_insurance_list)
                
                # Then expand insurance groups (DD INS/INS and DD Toolkit/Toolkits/DD)
                df[insurance_working_col] = df[insurance_working_col].apply(expand_insurance_groups)
                
                if 'main' in allocation_data:
                    allocation_data['main'] = df
                else:
                    allocation_data[sheet_name] = df
        
        allocation_filename = filename
        
        # Update allocation_data to only include processed sheets
        if 'main' in allocation_data:
            allocation_data = {'main': allocation_data['main']}
        
        processing_result = f"✅ Allocation file uploaded successfully! Loaded {len(allocation_data)} sheet(s): {', '.join(list(allocation_data.keys()))}"
        flash(f'Allocation file uploaded successfully! Loaded {len(allocation_data)} sheet(s): {", ".join(list(allocation_data.keys()))}', 'success')
        
        # Clean up uploaded file
        if os.path.exists(filename):
            os.remove(filename)
        
        return redirect('/')
        
    except Exception as e:
        processing_result = f"❌ Error uploading allocation file: {str(e)}"
        flash(f'Error uploading allocation file: {str(e)}', 'error')
        # Clean up uploaded file on error
        if 'filename' in locals() and os.path.exists(filename):
            os.remove(filename)
        return redirect('/')

@app.route('/upload_data', methods=['POST'])
@admin_required
def upload_data_file():
    global data_file_data, data_filename, processing_result
    
    if 'file' not in request.files:
        flash('No file provided', 'error')
        return redirect('/')
    
    file = request.files['file']
    if file.filename == '':
        flash('No file selected', 'error')
        return redirect('/')
    
    try:
        # Reset tracking for new file
        global _formatted_insurance_names, _formatted_insurance_details
        _formatted_insurance_names = set()
        _formatted_insurance_details = []
        
        # Save uploaded file temporarily
        filename = secure_filename(file.filename)
        file.save(filename)
        
        # Load Excel file
        data_file_data = pd.read_excel(filename, sheet_name=None)
        
        # Format insurance company names in "Dental Primary Ins Carr" column for better allocation
        for sheet_name, df in data_file_data.items():
            # Find the insurance carrier column (case-insensitive)
            insurance_col = None
            for col in df.columns:
                if 'dental' in col.lower() and 'primary' in col.lower() and 'ins' in col.lower() and 'carr' in col.lower():
                    insurance_col = col
                    break
            
            if insurance_col:
                data_file_data[sheet_name] = format_insurance_column_in_dataframe(df.copy(), insurance_col)
        
        data_filename = filename
        
        processing_result = f"✅ Data file uploaded successfully! Loaded {len(data_file_data)} sheets: {', '.join(list(data_file_data.keys()))}"
        flash(f'Data file uploaded successfully! Loaded {len(data_file_data)} sheets: {", ".join(list(data_file_data.keys()))}', 'success')
        
        # Print formatted insurance companies list
        print_formatted_insurance_companies()
        
        # Clean up uploaded file
        if os.path.exists(filename):
            os.remove(filename)
        
        return redirect('/')
        
    except Exception as e:
        processing_result = f"❌ Error uploading data file: {str(e)}"
        flash(f'Error uploading data file: {str(e)}', 'error')
        # Clean up uploaded file on error
        if 'filename' in locals() and os.path.exists(filename):
            os.remove(filename)
        return redirect('/')


@app.route('/process_files', methods=['POST'])
@admin_required
def process_files():
    global allocation_data, data_file_data, processing_result, agent_processing_result, agent_allocations_data
    
    if not data_file_data:
        processing_result = "❌ Error: Please upload data file first"
        return render_template_string(HTML_TEMPLATE, 
                                    allocation_data=allocation_data, 
                                    data_file_data=data_file_data,
                                    allocation_filename=allocation_filename,
                                    data_filename=data_filename,
                                    processing_result=processing_result,
                                    agent_processing_result=agent_processing_result,
                                    agent_allocations_data=agent_allocations_data)
    
    try:
        # Get the first sheet from data file
        data_df = list(data_file_data.values())[0]
        
        # Get selected appointment dates from calendar
        appointment_dates = request.form.getlist('appointment_dates')
        appointment_dates_second = request.form.getlist('appointment_dates_second')
        receive_dates = request.form.getlist('receive_dates')
        debug_count = request.form.get('debug_selected_count', '0')
        debug_count_second = request.form.get('debug_selected_count_second', '0')
        
        # Process the data file with selected dates and allocation data
        result_message, processed_df = process_allocation_files_with_dates(allocation_data, data_df, [], '', appointment_dates, appointment_dates_second, receive_dates)
        
        if processed_df is not None:
            # Store the result for download
            processing_result = result_message
            # Update the data_file_data with the processed result
            data_file_data[list(data_file_data.keys())[0]] = processed_df
        else:
            processing_result = result_message
        
        return render_template_string(HTML_TEMPLATE, 
                                    allocation_data=allocation_data, 
                                    data_file_data=data_file_data,
                                    allocation_filename=allocation_filename,
                                    data_filename=data_filename,
                                    processing_result=processing_result,
                                    agent_processing_result=agent_processing_result,
                                    agent_allocations_data=agent_allocations_data)
        
    except Exception as e:
        processing_result = f"❌ Error processing data file: {str(e)}"
        return render_template_string(HTML_TEMPLATE, 
                                    allocation_data=allocation_data, 
                                    data_file_data=data_file_data,
                                    allocation_filename=allocation_filename,
                                    data_filename=data_filename,
                                    processing_result=processing_result,
                                    agent_processing_result=agent_processing_result,
                                    agent_allocations_data=agent_allocations_data)

@app.route('/download_result', methods=['POST'])
@admin_required
def download_result():
    global data_file_data, data_filename
    
    if not data_file_data:
        return jsonify({'error': 'No data to download'}), 400
    
    filename = request.form.get('filename', '').strip()
    if not filename:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"processed_data_{timestamp}.xlsx"
    
    try:
        # Create a temporary file
        temp_fd, temp_path = tempfile.mkstemp(suffix='.xlsx')
        
        try:
            with pd.ExcelWriter(temp_path, engine='openpyxl') as writer:
                for sheet_name, df in data_file_data.items():
                    # Create a copy of the dataframe to avoid modifying the original
                    df_copy = df.copy()
                    
                    # Find appointment date columns and ensure they're formatted as dates without time
                    for col in df_copy.columns:
                        if 'appointment' in col.lower() and 'date' in col.lower():
                            # Convert to datetime and then format as date string
                            df_copy[col] = pd.to_datetime(df_copy[col], errors='coerce').dt.strftime('%Y-%m-%d')
                    
                    df_copy.to_excel(writer, sheet_name=sheet_name, index=False)
            
            return send_file(temp_path, as_attachment=True, download_name=filename)
            
        finally:
            # Clean up temporary file
            os.close(temp_fd)
            if os.path.exists(temp_path):
                os.unlink(temp_path)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/upload_work_file', methods=['POST'])
@agent_required
def upload_work_file():
    """Upload agent work file with data changes"""
    if 'file' not in request.files:
        return jsonify({'success': False, 'message': 'No file provided'}), 400
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({'success': False, 'message': 'No file selected'}), 400
    
    notes = request.form.get('notes', '')
    
    try:
        # Get current agent
        user_id = session.get('user_id')
        if not user_id:
            return jsonify({'success': False, 'message': 'User not found'}), 400
        
        # Try to find user by ID first, then by email/google_id
        user = User.query.filter_by(id=user_id, is_active=True).first()
        if not user:
            # If not found by ID, try by email (for Google OAuth users)
            user = User.query.filter_by(email=user_id, is_active=True).first()
        if not user:
            # If still not found, try by google_id
            user = User.query.filter_by(google_id=user_id, is_active=True).first()
            
        if not user:
            return jsonify({'success': False, 'message': 'User not found'}), 400
        
        # Save uploaded file
        filename = secure_filename(file.filename)
        file.save(filename)
        
        # Load and process Excel file
        try:
            file_data = pd.read_excel(filename, sheet_name=None)
            
            # Clear all existing agent work files before saving new one
            existing_files = AgentWorkFile.query.filter_by(agent_id=user.id).all()
            for existing_file in existing_files:
                db.session.delete(existing_file)
            db.session.commit()
            
            # Save new file to database
            work_file = save_agent_work_file(
                agent_id=user.id,
                filename=filename,
                file_data=file_data,
                notes=notes
            )
            
            # Clean up uploaded file
            if os.path.exists(filename):
                os.remove(filename)
            
            return jsonify({'success': True, 'message': f'Work file uploaded successfully: {filename} (Previous files cleared)'})
        
        except Exception as e:
            # Clean up uploaded file on error
            if os.path.exists(filename):
                os.remove(filename)
            return jsonify({'success': False, 'message': f'Error processing Excel file: {str(e)}'}), 500
        
    except Exception as e:
        return jsonify({'success': False, 'message': f'Error uploading work file: {str(e)}'}), 500

@app.route('/upload_status', methods=['POST'])
@agent_required
def upload_status_file():
    """Legacy route - redirect to new work file upload"""
    return redirect(url_for('upload_work_file'))

@app.route('/consolidate_agent_files', methods=['POST'])
@admin_required
def consolidate_agent_files():
    """Consolidate all agent work files into one Excel file"""
    try:
        # Get all agent work files
        work_files = get_all_agent_work_files()
        
        if not work_files:
            flash('No agent work files found to consolidate', 'warning')
            return redirect('/')
        
        # Create Excel buffer
        excel_buffer = io.BytesIO()
        
        with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
            # Create summary sheet with updated columns
            summary_data = []
            for work_file in work_files:
                file_data = work_file.get_file_data()
                total_assigned_count = 0
                completed_count = 0
                
                # Calculate counts from file data
                if file_data:
                    if isinstance(file_data, dict):
                        # Multiple sheets - count rows from all sheets except Summary
                        for sheet_name, sheet_data in file_data.items():
                            # Skip Summary sheet
                            if sheet_name.lower() == 'summary':
                                continue
                                
                            if isinstance(sheet_data, pd.DataFrame):
                                # Total assigned count = all rows (excluding header)
                                total_assigned_count += len(sheet_data)
                                
                                # Count completed (non-Workable remarks)
                                if 'Remark' in sheet_data.columns:
                                    # Filter out NaN values and count non-Workable entries
                                    remark_data = sheet_data['Remark'].dropna()
                                    completed_count += len(remark_data[remark_data.str.lower() != 'workable'])
                                elif 'remark' in sheet_data.columns:
                                    remark_data = sheet_data['remark'].dropna()
                                    completed_count += len(remark_data[remark_data.str.lower() != 'workable'])
                                elif 'remarks' in sheet_data.columns:
                                    remark_data = sheet_data['remarks'].dropna()
                                    completed_count += len(remark_data[remark_data.str.lower() != 'workable'])
                    elif isinstance(file_data, pd.DataFrame):
                        # Single DataFrame
                        # Total assigned count = all rows (excluding header)
                        total_assigned_count = len(file_data)
                        
                        # Count completed (non-Workable remarks)
                        if 'Remark' in file_data.columns:
                            remark_data = file_data['Remark'].dropna()
                            completed_count = len(remark_data[remark_data.str.lower() != 'workable'])
                        elif 'remark' in file_data.columns:
                            remark_data = file_data['remark'].dropna()
                            completed_count = len(remark_data[remark_data.str.lower() != 'workable'])
                        elif 'remarks' in file_data.columns:
                            remark_data = file_data['remarks'].dropna()
                            completed_count = len(remark_data[remark_data.str.lower() != 'workable'])
                
                summary_data.append({
                    'Agent': work_file.agent.name,
                    'Total Assigned Count': total_assigned_count,
                    'Completed Count': completed_count
                })
            
            summary_df = pd.DataFrame(summary_data)
            summary_df.to_excel(writer, sheet_name='Summary', index=False)
            
            # Combine all agent data into one sheet
            all_agent_data = []
            for work_file in work_files:
                file_data = work_file.get_file_data()
                if file_data:
                    if isinstance(file_data, dict):
                        # Multiple sheets - combine them (excluding Summary sheets)
                        for sheet_name, sheet_data in file_data.items():
                            # Skip Summary sheet
                            if sheet_name.lower() == 'summary':
                                continue
                                
                            if isinstance(sheet_data, pd.DataFrame):
                                sheet_data_copy = sheet_data.copy()
                                sheet_data_copy['Agent'] = work_file.agent.name
                                sheet_data_copy['Source_Sheet'] = sheet_name
                                all_agent_data.append(sheet_data_copy)
                    elif isinstance(file_data, pd.DataFrame):
                        # Single DataFrame
                        file_data_copy = file_data.copy()
                        file_data_copy['Agent'] = work_file.agent.name
                        all_agent_data.append(file_data_copy)
            
            # Create combined sheet with all agent data
            if all_agent_data:
                combined_df = pd.concat(all_agent_data, ignore_index=True)
                combined_df.to_excel(writer, sheet_name='All Agent Data', index=False)
            else:
                # Fallback if no data found
                simple_df = pd.DataFrame([{'Message': 'No data available from any agent'}])
                simple_df.to_excel(writer, sheet_name='All Agent Data', index=False)
        
        excel_buffer.seek(0)
        
        # Mark files as consolidated
        for work_file in work_files:
            work_file.status = 'consolidated'
        db.session.commit()
        
        # Return file for download
        filename = f"consolidated_agent_files_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        return send_file(
            excel_buffer,
            as_attachment=True,
            download_name=filename,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
        
    except Exception as e:
        flash(f'Error consolidating agent files: {str(e)}', 'error')
        return redirect('/')

@app.route('/get_appointment_dates')
@login_required
def get_appointment_dates():
    global data_file_data
    
    if not data_file_data:
        return jsonify({'error': 'No data file uploaded'}), 400
    
    try:
        # Get the first sheet from data file
        data_df = list(data_file_data.values())[0]
        
        # Find the appointment date column
        appointment_date_col = None
        for col in data_df.columns:
            if 'appointment' in col.lower() and 'date' in col.lower():
                appointment_date_col = col
                break
        
        if appointment_date_col is None:
            return jsonify({'error': 'Appointment Date column not found'}), 400
        
        # Get unique appointment dates with row counts
        appointment_dates = data_df[appointment_date_col].dropna().unique()
        
        # Convert to string format and count rows for each date
        date_data = []
        for date in appointment_dates:
            if hasattr(date, 'date'):
                date_str = date.date().strftime('%Y-%m-%d')
            else:
                date_str = str(date)
            
            # Count rows for this specific date
            if hasattr(date, 'date'):
                row_count = len(data_df[data_df[appointment_date_col].dt.date == date.date()])
            else:
                row_count = len(data_df[data_df[appointment_date_col] == date])
            
            date_data.append({
                'date': date_str,
                'row_count': row_count
            })
        
        # Sort by date
        date_data.sort(key=lambda x: x['date'])
        
        return jsonify({
            'appointment_dates': [item['date'] for item in date_data],
            'appointment_dates_with_counts': date_data,
            'column_name': appointment_date_col
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/get_receive_dates', methods=['GET'])
@admin_required
def get_receive_dates():
    """Get unique receive dates from data file, optionally filtered by appointment dates"""
    global data_file_data
    
    if not data_file_data:
        return jsonify({'error': 'No data file uploaded'}), 400
    
    try:
        # Get the first sheet from data file
        data_df = list(data_file_data.values())[0]
        
        # Find the receive date column
        receive_date_col = None
        for col in data_df.columns:
            if 'receive' in col.lower() and 'date' in col.lower():
                receive_date_col = col
                break
        
        if receive_date_col is None:
            return jsonify({'error': 'Receive Date column not found'}), 400
        
        # Get appointment dates from query parameters
        appointment_dates = request.args.getlist('appointment_dates')
        
        # Filter data based on selected appointment dates if provided
        filtered_df = data_df
        if appointment_dates:
            # Find the appointment date column
            appointment_date_col = None
            for col in data_df.columns:
                if 'appointment' in col.lower() and 'date' in col.lower():
                    appointment_date_col = col
                    break
            
            if appointment_date_col:
                # Convert appointment dates to the same format as in the dataframe
                appointment_dates_formatted = []
                for date_str in appointment_dates:
                    try:
                        # Try to parse the date string and convert to the format used in dataframe
                        from datetime import datetime
                        parsed_date = datetime.strptime(date_str, '%Y-%m-%d').date()
                        appointment_dates_formatted.append(parsed_date)
                    except:
                        # If parsing fails, try to match as string
                        appointment_dates_formatted.append(date_str)
                
                # Filter rows where appointment date matches any of the selected dates
                mask = data_df[appointment_date_col].isin(appointment_dates_formatted)
                filtered_df = data_df[mask]
        
        # Get unique receive dates from filtered data
        receive_dates = filtered_df[receive_date_col].dropna().unique()
        
        # Convert to string format and sort
        date_strings = []
        for date in receive_dates:
            if hasattr(date, 'date'):
                date_str = date.date().strftime('%Y-%m-%d')
            else:
                date_str = str(date)
            date_strings.append(date_str)
        
        date_strings.sort()
        
        return jsonify({
            'receive_dates': date_strings,
            'column_name': receive_date_col,
            'filtered_by_appointment_dates': len(appointment_dates) > 0 if appointment_dates else False
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500



@app.route('/get_agent_allocation', methods=['POST'])
@admin_required
def get_agent_allocation():
    global data_file_data, agent_allocations_data
    
    if not data_file_data or not agent_allocations_data:
        return jsonify({'error': 'No data available'}), 400
    
    agent_id = request.json.get('agent_id')
    agent_name = request.json.get('agent_name')
    
    if not agent_id and not agent_name:
        return jsonify({'error': 'No agent specified (agent_id or agent_name required)'}), 400
    
    try:
        # Find the agent in allocations data
        agent_info = None
        
        # First try to find by agent_id if provided (most reliable)
        if agent_id:
            for agent in agent_allocations_data:
                if agent.get('id') == agent_id:
                    agent_info = agent
                    break
        
        # If not found by ID and name is provided, try by name
        if not agent_info and agent_name:
            matching_agents = [agent for agent in agent_allocations_data if agent.get('name') == agent_name]
            if len(matching_agents) == 1:
                agent_info = matching_agents[0]
            elif len(matching_agents) > 1:
                # Multiple agents with same name - require agent_id
                return jsonify({
                    'error': f'Multiple agents found with name "{agent_name}". Please use agent_id instead.',
                    'agents': [{'id': a.get('id'), 'name': a.get('name')} for a in matching_agents]
                }), 400
        
        if not agent_info:
            return jsonify({'error': 'Agent not found'}), 404
        
        # Get the processed data
        processed_df = list(data_file_data.values())[0]
        
        # Get the specific rows allocated to this agent
        agent_rows = agent_info['allocated']
        row_indices = agent_info.get('row_indices', [])
        
        # Create a subset of data for this agent using specific row indices
        if row_indices and len(row_indices) > 0 and len(processed_df) > max(row_indices):
            agent_df = processed_df.iloc[row_indices].copy()
        else:
            # Fallback: if row_indices not available, use first N rows
            if len(processed_df) >= agent_rows:
                agent_df = processed_df.head(agent_rows).copy()
            else:
                agent_df = processed_df.copy()
        
        # Add serial number column
        agent_df_with_sr = agent_df.copy()
        agent_df_with_sr.insert(0, 'Sr No', range(1, len(agent_df_with_sr) + 1))
        
        # Convert dataframe to HTML table
        html_table = agent_df_with_sr.to_html(classes='modal-table', table_id='agentDataTable', escape=False, index=False)
        
        # Calculate statistics
        total_rows = len(agent_df)
        first_priority = len(agent_df[agent_df['Priority Status'] == 'First Priority']) if 'Priority Status' in agent_df.columns else 0
        second_priority = len(agent_df[agent_df['Priority Status'] == 'Second Priority']) if 'Priority Status' in agent_df.columns else 0
        third_priority = len(agent_df[agent_df['Priority Status'] == 'Third Priority']) if 'Priority Status' in agent_df.columns else 0
        
        return jsonify({
            'success': True,
            'agent_id': agent_info.get('id'),
            'agent_name': agent_info.get('name'),
            'html_table': html_table,
            'stats': {
                'total_rows': total_rows,
                'capacity': agent_info['capacity'],
                'first_priority': first_priority,
                'second_priority': second_priority,
                'third_priority': third_priority
            }
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/download_agent_file', methods=['POST'])
@admin_required
def download_agent_file():
    global data_file_data, agent_allocations_data
    
    if not data_file_data or not agent_allocations_data:
        return jsonify({'error': 'No data available for download'}), 400
    
    agent_id = request.form.get('agent_id')
    agent_name = request.form.get('agent_name')
    
    if not agent_id and not agent_name:
        return jsonify({'error': 'No agent specified (agent_id or agent_name required)'}), 400
    
    # Find the agent
    agent_info = None
    if agent_id:
        for agent in agent_allocations_data:
            if agent.get('id') == agent_id:
                agent_info = agent
                break
    if not agent_info and agent_name:
        matching_agents = [agent for agent in agent_allocations_data if agent.get('name') == agent_name]
        if len(matching_agents) == 1:
            agent_info = matching_agents[0]
        elif len(matching_agents) > 1:
            return jsonify({
                'error': f'Multiple agents found with name "{agent_name}". Please use agent_id instead.',
                'agents': [{'id': a.get('id'), 'name': a.get('name')} for a in matching_agents]
            }), 400
        
        if not agent_info:
            return jsonify({'error': 'Agent not found'}), 404
        
    agent_name = agent_info.get('name', 'Unknown')
    
    # Generate filename with agent name and today's date
    from datetime import datetime
    today = datetime.now().strftime("%Y-%m-%d")
    filename = f"{agent_name}_{today}.xlsx"
    
    try:
        # Get the processed data
        processed_df = list(data_file_data.values())[0]
        
        # Get the specific rows allocated to this agent
        agent_rows = agent_info['allocated']
        row_indices = agent_info.get('row_indices', [])
        
        # Create a subset of data for this agent using specific row indices
        if row_indices and len(row_indices) > 0 and len(processed_df) > max(row_indices):
            agent_df = processed_df.iloc[row_indices].copy()
        else:
            # Fallback: if row_indices not available, use first N rows
            if len(processed_df) >= agent_rows:
                agent_df = processed_df.head(agent_rows).copy()
            else:
                agent_df = processed_df.copy()
        
        # Add agent information to the dataframe
        agent_df['Agent Name'] = agent_name
        agent_df['Allocated Rows'] = agent_rows
        agent_df['Agent Capacity'] = agent_info['capacity']
        
        # Create a temporary file
        temp_fd, temp_path = tempfile.mkstemp(suffix='.xlsx')
        
        try:
            with pd.ExcelWriter(temp_path, engine='openpyxl') as writer:
                # Create a copy of the dataframe to avoid modifying the original
                agent_df_copy = agent_df.copy()
                
                # Find appointment date columns and ensure they're formatted as dates without time
                for col in agent_df_copy.columns:
                    if 'appointment' in col.lower() and 'date' in col.lower():
                        # Convert to datetime and then format as date string
                        agent_df_copy[col] = pd.to_datetime(agent_df_copy[col], errors='coerce').dt.strftime('%Y-%m-%d')
                
                agent_df_copy.to_excel(writer, sheet_name=f'{agent_name}_Allocation', index=False)
                
                # Add a summary sheet
                summary_data = {
                    'Metric': ['Agent Name', 'Total Allocated Rows', 'Agent Capacity', 'First Priority Rows', 'Second Priority Rows', 'Third Priority Rows'],
                    'Value': [
                        agent_name,
                        agent_rows,
                        agent_info['capacity'],
                        len(agent_df[agent_df['Priority Status'] == 'First Priority']) if 'Priority Status' in agent_df.columns else 0,
                        len(agent_df[agent_df['Priority Status'] == 'Second Priority']) if 'Priority Status' in agent_df.columns else 0,
                        len(agent_df[agent_df['Priority Status'] == 'Third Priority']) if 'Priority Status' in agent_df.columns else 0
                    ]
                }
                summary_df = pd.DataFrame(summary_data)
                summary_df.to_excel(writer, sheet_name='Summary', index=False)
            
            return send_file(temp_path, as_attachment=True, download_name=filename)
            
        finally:
            # Clean up temporary file
            os.close(temp_fd)
            if os.path.exists(temp_path):
                os.unlink(temp_path)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/send_approval_email', methods=['POST'])
@admin_required
def send_approval_email():
    try:
        data = request.get_json()
        agent_id = data.get('agent_id')
        agent_name = data.get('agent_name')
        
        if (not agent_id and not agent_name) or not agent_allocations_data:
            return jsonify({'success': False, 'message': 'Agent ID or name required'})
        
        # Find the agent in the allocation data
        agent_info = None
        if agent_id:
            for agent in agent_allocations_data:
                if agent.get('id') == agent_id:
                    agent_info = agent
                    break
        if not agent_info and agent_name:
            matching_agents = [agent for agent in agent_allocations_data if agent.get('name') == agent_name]
            if len(matching_agents) == 1:
                agent_info = matching_agents[0]
            elif len(matching_agents) > 1:
                return jsonify({
                    'success': False,
                    'message': f'Multiple agents found with name "{agent_name}". Please use agent_id instead.',
                    'agents': [{'id': a.get('id'), 'name': a.get('name')} for a in matching_agents]
                })
        
        if not agent_info:
            return jsonify({'success': False, 'message': 'Agent not found'})
        
        # Get agent's email from allocation data
        agent_email = agent_info.get('email')
        if not agent_email:
            return jsonify({'success': False, 'message': 'Agent email not found'})
        
        # Get allocation summary
        summary = get_allocation_summary(agent_name, agent_info)
        
        # Create Excel file with agent's allocated data
        excel_buffer = create_agent_excel_file(agent_name, agent_info)
        
        # Format insurance companies list
        insurance_list = ', '.join(sorted(summary['insurance_companies'])) if summary['insurance_companies'] else 'None'
        
        # Format first priority deadline
        deadline_text = ''
        if summary['first_priority_deadline']:
            deadline_text = summary['first_priority_deadline'].strftime('%Y-%m-%d at %I:%M %p')
        else:
            deadline_text = 'N/A (No First Priority work assigned)'
        
        # Send email
        msg = Message(
            subject=f'Your Work Allocation - {agent_name}',
            recipients=[agent_email],
            body=f'''
Dear {agent_name},

Your work allocation has been approved and is attached to this email.

📊 ALLOCATION SUMMARY:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
• Total Allocated: {summary['total_allocated']} rows
• Your Capacity: {agent_info['capacity']} rows
• Allocation Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

📋 PRIORITY BREAKDOWN:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
• First Priority: {summary['first_priority_count']} rows
• Second Priority: {summary['second_priority_count']} rows
• Third Priority: {summary['third_priority_count']} rows
• Unknown/Other: {summary['unknown_priority_count']} rows

🏥 INSURANCE COMPANIES ({len(summary['insurance_companies'])} unique):
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
{insurance_list}

⏰ FIRST PRIORITY DEADLINE:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
{deadline_text}

Please find your allocated data in the attached Excel file.

Best regards,
Allocation Management System
            ''',
            html=f'''
            <h2>Work Allocation Approved</h2>
            <p>Dear <strong>{agent_name}</strong>,</p>
            <p>Your work allocation has been approved and is attached to this email.</p>
            
            <div style="background-color: #f5f5f5; padding: 15px; border-radius: 5px; margin: 20px 0;">
                <h3 style="margin-top: 0; color: #333;">📊 Allocation Summary</h3>
                <ul style="list-style: none; padding-left: 0;">
                    <li><strong>Total Allocated:</strong> {summary['total_allocated']} rows</li>
                <li><strong>Your Capacity:</strong> {agent_info['capacity']} rows</li>
                <li><strong>Allocation Date:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</li>
            </ul>
            </div>
            
            <div style="background-color: #e8f4f8; padding: 15px; border-radius: 5px; margin: 20px 0;">
                <h3 style="margin-top: 0; color: #2c5282;">📋 Priority Breakdown</h3>
                <ul style="list-style: none; padding-left: 0;">
                    <li><strong>First Priority:</strong> {summary['first_priority_count']} rows</li>
                    <li><strong>Second Priority:</strong> {summary['second_priority_count']} rows</li>
                    <li><strong>Third Priority:</strong> {summary['third_priority_count']} rows</li>
                    <li><strong>Unknown/Other:</strong> {summary['unknown_priority_count']} rows</li>
                </ul>
            </div>
            
            <div style="background-color: #fff3cd; padding: 15px; border-radius: 5px; margin: 20px 0;">
                <h3 style="margin-top: 0; color: #856404;">🏥 Insurance Companies ({len(summary['insurance_companies'])} unique)</h3>
                <p style="word-wrap: break-word;">{insurance_list}</p>
            </div>
            
            <div style="background-color: #f8d7da; padding: 15px; border-radius: 5px; margin: 20px 0; border-left: 4px solid #dc3545;">
                <h3 style="margin-top: 0; color: #721c24;">⏰ First Priority Completion Deadline</h3>
                <p style="font-size: 18px; font-weight: bold; color: #721c24; margin: 10px 0;">{deadline_text}</p>
                <p style="font-size: 12px; color: #856404;">Please ensure all First Priority work is completed by this deadline.</p>
            </div>
            
            <p>Please find your allocated data in the attached Excel file.</p>
            
            <p>Best regards,<br>
            Allocation Management System</p>
            '''
        )
        
        # Attach Excel file
        msg.attach(
            filename=f'{agent_name}_allocation_{datetime.now().strftime("%Y%m%d")}.xlsx',
            content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            data=excel_buffer.getvalue()
        )
        
        mail.send(msg)
        
        return jsonify({'success': True, 'message': f'Approval email sent to {agent_email}'})
        
    except Exception as e:
        return jsonify({'success': False, 'message': f'Error sending email: {str(e)}'})

@app.route('/approve_all_allocations', methods=['POST'])
@admin_required
def approve_all_allocations():
    """Approve all agent allocations and send emails to all agents"""
    try:
        if not agent_allocations_data:
            return jsonify({'success': False, 'message': 'No allocation data found'})
        
        successful_sends = []
        failed_sends = []
        
        # Loop through all agents and send approval emails
        for agent in agent_allocations_data:
            agent_name = agent.get('name')
            agent_email = agent.get('email')
            allocated = agent.get('allocated', 0)
            
            # Skip agents with no email or no allocated rows
            if not agent_email:
                failed_sends.append(f"{agent_name}: No email address")
                continue
            
            if allocated == 0:
                # Skip agents with no allocations (no need to send email)
                continue
            
            try:
                # Get allocation summary
                summary = get_allocation_summary(agent_name, agent)
                
                # Create Excel file with agent's allocated data
                excel_buffer = create_agent_excel_file(agent_name, agent)
                
                # Format insurance companies list
                insurance_list = ', '.join(sorted(summary['insurance_companies'])) if summary['insurance_companies'] else 'None'
                
                # Format first priority deadline
                deadline_text = ''
                if summary['first_priority_deadline']:
                    deadline_text = summary['first_priority_deadline'].strftime('%Y-%m-%d at %I:%M %p')
                else:
                    deadline_text = 'N/A (No First Priority work assigned)'
                
                # Send email
                msg = Message(
                    subject=f'Your Work Allocation - {agent_name}',
                    recipients=[agent_email],
                    body=f'''
Dear {agent_name},

Your work allocation has been approved and is attached to this email.

📊 ALLOCATION SUMMARY:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
• Total Allocated: {summary['total_allocated']} rows
• Your Capacity: {agent['capacity']} rows
• Allocation Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

📋 PRIORITY BREAKDOWN:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
• First Priority: {summary['first_priority_count']} rows
• Second Priority: {summary['second_priority_count']} rows
• Third Priority: {summary['third_priority_count']} rows
• Unknown/Other: {summary['unknown_priority_count']} rows

🏥 INSURANCE COMPANIES ({len(summary['insurance_companies'])} unique):
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
{insurance_list}

⏰ FIRST PRIORITY DEADLINE:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
{deadline_text}

Please find your allocated data in the attached Excel file.

Best regards,
Allocation Management System
                    ''',
                    html=f'''
                    <h2>Work Allocation Approved</h2>
                    <p>Dear <strong>{agent_name}</strong>,</p>
                    <p>Your work allocation has been approved and is attached to this email.</p>
                    
                    <div style="background-color: #f5f5f5; padding: 15px; border-radius: 5px; margin: 20px 0;">
                        <h3 style="margin-top: 0; color: #333;">📊 Allocation Summary</h3>
                        <ul style="list-style: none; padding-left: 0;">
                            <li><strong>Total Allocated:</strong> {summary['total_allocated']} rows</li>
                            <li><strong>Your Capacity:</strong> {agent['capacity']} rows</li>
                            <li><strong>Allocation Date:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</li>
                        </ul>
                    </div>
                    
                    <div style="background-color: #e8f4f8; padding: 15px; border-radius: 5px; margin: 20px 0;">
                        <h3 style="margin-top: 0; color: #2c5282;">📋 Priority Breakdown</h3>
                        <ul style="list-style: none; padding-left: 0;">
                            <li><strong>First Priority:</strong> {summary['first_priority_count']} rows</li>
                            <li><strong>Second Priority:</strong> {summary['second_priority_count']} rows</li>
                            <li><strong>Third Priority:</strong> {summary['third_priority_count']} rows</li>
                            <li><strong>Unknown/Other:</strong> {summary['unknown_priority_count']} rows</li>
                        </ul>
                    </div>
                    
                    <div style="background-color: #fff3cd; padding: 15px; border-radius: 5px; margin: 20px 0;">
                        <h3 style="margin-top: 0; color: #856404;">🏥 Insurance Companies ({len(summary['insurance_companies'])} unique)</h3>
                        <p style="word-wrap: break-word;">{insurance_list}</p>
                    </div>
                    
                    <div style="background-color: #f8d7da; padding: 15px; border-radius: 5px; margin: 20px 0; border-left: 4px solid #dc3545;">
                        <h3 style="margin-top: 0; color: #721c24;">⏰ First Priority Completion Deadline</h3>
                        <p style="font-size: 18px; font-weight: bold; color: #721c24; margin: 10px 0;">{deadline_text}</p>
                        <p style="font-size: 12px; color: #856404;">Please ensure all First Priority work is completed by this deadline.</p>
                    </div>
                    
                    <p>Please find your allocated data in the attached Excel file.</p>
                    
                    <p>Best regards,<br>
                    Allocation Management System</p>
                    '''
                )
                
                # Attach Excel file
                msg.attach(
                    filename=f'{agent_name}_allocation_{datetime.now().strftime("%Y%m%d")}.xlsx',
                    content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                    data=excel_buffer.getvalue()
                )
                
                mail.send(msg)
                successful_sends.append(f"{agent_name} ({agent_email})")
                
            except Exception as e:
                failed_sends.append(f"{agent_name}: {str(e)}")
        
        # Prepare response message
        total_agents = len(agent_allocations_data)
        agents_with_allocation = sum(1 for a in agent_allocations_data if a.get('allocated', 0) > 0)
        successful_count = len(successful_sends)
        failed_count = len(failed_sends)
        
        if successful_count > 0:
            message = f"Successfully sent approval emails to {successful_count} agent(s): {', '.join([s.split(' (')[0] for s in successful_sends])}"
            if failed_count > 0:
                message += f". {failed_count} agent(s) failed: {', '.join(failed_sends)}"
        else:
            message = f"No emails sent. Errors: {', '.join(failed_sends)}" if failed_sends else "No agents with allocations to approve."
        
        return jsonify({
            'success': successful_count > 0,
            'message': message,
            'details': {
                'total_agents': total_agents,
                'agents_with_allocation': agents_with_allocation,
                'successful': successful_count,
                'failed': failed_count,
                'successful_list': successful_sends,
                'failed_list': failed_sends
            }
        })
        
    except Exception as e:
        return jsonify({'success': False, 'message': f'Error approving all allocations: {str(e)}'})

@app.route('/view_shift_times', methods=['GET'])
@admin_required
def view_shift_times():
    """Admin endpoint to view all agents' shift information for verification"""
    global agent_allocations_data, allocation_data
    
    shift_info = []
    
    # First try to get from agent_allocations_data (after processing)
    if agent_allocations_data:
        try:
            for agent in agent_allocations_data:
                shift_start = agent.get('shift_start_time', 'Not set')
                shift_original = agent.get('shift_time_original', 'Not set')
                shift_group = agent.get('shift_group')
                
                # Format shift group name
                group_name = 'Not set'
                if shift_group == 1:
                    group_name = 'Day Shift'
                elif shift_group == 2:
                    group_name = 'Afternoon Shift'
                elif shift_group == 3:
                    group_name = 'Night Shift'
                
                # Format shift start time for display
                start_time_display = shift_start if shift_start else 'Not parsed'
                if shift_start:
                    try:
                        hour, minute = map(int, shift_start.split(':'))
                        if hour < 12:
                            start_time_display = f"{shift_start} ({hour}:{minute:02d} AM)"
                        elif hour == 12:
                            start_time_display = f"{shift_start} (12:00 PM)"
                        else:
                            start_time_display = f"{shift_start} ({hour-12}:{minute:02d} PM)"
                    except:
                        pass
                
                shift_info.append({
                    'agent_id': agent.get('id'),
                    'agent_name': agent.get('name'),
                    'email': agent.get('email', 'Not set'),
                    'shift_time_original': shift_original,
                    'shift_start_time_parsed': shift_start,
                    'shift_start_time_display': start_time_display,
                    'shift_group': shift_group,
                    'shift_group_name': group_name,
                    'capacity': agent.get('capacity', 0),
                    'allocated': agent.get('allocated', 0)
                })
            
            # Sort by shift start time
            shift_info.sort(key=lambda x: (
                x['shift_start_time_parsed'] if x['shift_start_time_parsed'] and x['shift_start_time_parsed'] != 'Not parsed' else '99:99',
                x['agent_name']
            ))
            
            return jsonify({
                'success': True,
                'total_agents': len(shift_info),
                'agents': shift_info,
                'source': 'processed'
            })
        except Exception as e:
            return jsonify({'error': f'Error retrieving shift information: {str(e)}'}), 500
    
    # If no processed data, try to extract from raw allocation_data
    if allocation_data:
        try:
            # Get the main sheet
            agent_df = None
            if 'main' in allocation_data:
                agent_df = allocation_data['main']
            elif len(allocation_data) > 0:
                agent_df = list(allocation_data.values())[0]
            
            if agent_df is not None:
                # Find columns
                agent_name_col = None
                agent_id_col = None
                shift_time_col = None
                shift_group_col = None
                email_col = None
                counts_col = None
                
                for col in agent_df.columns:
                    col_lower = col.lower()
                    if 'agent' in col_lower and 'name' in col_lower:
                        agent_name_col = col
                    elif col_lower == 'id':
                        agent_id_col = col
                    elif 'shift' in col_lower and 'time' in col_lower:
                        shift_time_col = col
                    elif 'shift' in col_lower and 'group' in col_lower:
                        shift_group_col = col
                    elif 'email' in col_lower and 'id' in col_lower:
                        email_col = col
                    elif col_lower == 'tfd':
                        counts_col = col
                
                if agent_name_col:
                    # Parse shift times from raw data
                    for _, row in agent_df.iterrows():
                        agent_name = str(row[agent_name_col]).strip() if pd.notna(row[agent_name_col]) else 'Unknown'
                        
                        # Get agent ID
                        if agent_id_col and pd.notna(row[agent_id_col]):
                            agent_id = str(row[agent_id_col]).strip()
                        else:
                            agent_id = f"{agent_name}_{row.name}"
                        
                        # Get shift time
                        shift_original = None
                        if shift_time_col and pd.notna(row[shift_time_col]):
                            shift_original = str(row[shift_time_col]).strip()
                        
                        # Get shift group
                        shift_group = None
                        if shift_group_col and pd.notna(row[shift_group_col]):
                            try:
                                shift_group = int(float(str(row[shift_group_col]).strip()))
                            except:
                                pass
                        
                        # Parse shift start time (using same logic as in process_allocation_files_with_dates)
                        shift_start = None
                        shift_start_display = 'Not parsed'
                        
                        if shift_original:
                            try:
                                from datetime import time as dt_time
                                import re
                                
                                if '-' in shift_original:
                                    parts = shift_original.split('-')
                                    if len(parts) >= 2:
                                        start_time_str = parts[0].strip()
                                        end_time_str = parts[1].strip()
                                        
                                        has_end_am = 'am' in end_time_str.lower()
                                        has_end_pm = 'pm' in end_time_str.lower()
                                        has_start_am = 'am' in start_time_str.lower()
                                        has_start_pm = 'pm' in start_time_str.lower()
                                        
                                        start_match = re.search(r'(\d{1,2})', start_time_str)
                                        if start_match:
                                            hour = int(start_match.group(1))
                                            minute = 0
                                            
                                            if has_start_am:
                                                if hour == 12:
                                                    hour = 0
                                            elif has_start_pm:
                                                if hour != 12:
                                                    hour += 12
                                            else:
                                                end_match = re.search(r'(\d{1,2})', end_time_str)
                                                if end_match:
                                                    end_hour_12 = int(end_match.group(1))
                                                    
                                                    if shift_group == 1:
                                                        pass  # Keep as AM
                                                    elif shift_group == 2:
                                                        if hour >= end_hour_12:
                                                            pass
                                                        else:
                                                            if hour != 12:
                                                                hour += 12
                                                    elif shift_group == 3:
                                                        if has_end_am:
                                                            if hour >= end_hour_12:
                                                                if hour != 12:
                                                                    hour += 12
                                                            else:
                                                                if hour == 12:
                                                                    hour = 0
                                                    else:
                                                        if has_end_am:
                                                            if hour >= end_hour_12:
                                                                if hour != 12:
                                                                    hour += 12
                                                            else:
                                                                if hour == 12:
                                                                    hour = 0
                                                        elif has_end_pm:
                                                            if hour >= end_hour_12 and hour < 12:
                                                                pass
                                                            elif hour < end_hour_12:
                                                                if hour != 12:
                                                                    hour += 12
                                            
                                            minute_match = re.search(r':(\d{2})', start_time_str)
                                            if minute_match:
                                                minute = int(minute_match.group(1))
                                            
                                            shift_start = dt_time(hour % 24, minute)
                                            shift_start_str = shift_start.strftime('%H:%M')
                                            
                                            # Format display
                                            if hour < 12:
                                                shift_start_display = f"{shift_start_str} ({hour}:{minute:02d} AM)"
                                            elif hour == 12:
                                                shift_start_display = f"{shift_start_str} (12:00 PM)"
                                            else:
                                                shift_start_display = f"{shift_start_str} ({(hour-12)}:{minute:02d} PM)"
                            except Exception as e:
                                pass
                        
                        # Format shift group name
                        group_name = 'Not set'
                        if shift_group == 1:
                            group_name = 'Day Shift'
                        elif shift_group == 2:
                            group_name = 'Afternoon Shift'
                        elif shift_group == 3:
                            group_name = 'Night Shift'
                        
                        # Get other info
                        agent_email = ''
                        if email_col and pd.notna(row[email_col]):
                            agent_email = str(row[email_col]).strip()
                        
                        capacity = 0
                        if counts_col and pd.notna(row[counts_col]):
                            try:
                                capacity = int(float(str(row[counts_col]).replace(',', '')))
                            except:
                                pass
                        
                        shift_info.append({
                            'agent_id': agent_id,
                            'agent_name': agent_name,
                            'email': agent_email or 'Not set',
                            'shift_time_original': shift_original or 'Not set',
                            'shift_start_time_parsed': shift_start.strftime('%H:%M') if shift_start else 'Not parsed',
                            'shift_start_time_display': shift_start_display,
                            'shift_group': shift_group,
                            'shift_group_name': group_name,
                            'capacity': capacity,
                            'allocated': 0
                        })
                
                # Sort by shift start time
                shift_info.sort(key=lambda x: (
                    x['shift_start_time_parsed'] if x['shift_start_time_parsed'] and x['shift_start_time_parsed'] != 'Not parsed' else '99:99',
                    x['agent_name']
                ))
                
                return jsonify({
                    'success': True,
                    'total_agents': len(shift_info),
                    'agents': shift_info,
                    'source': 'raw_upload',
                    'message': 'Showing shift times from uploaded staff details (file not yet processed)'
                })
        except Exception as e:
            return jsonify({'error': f'Error extracting shift information from uploaded file: {str(e)}'}), 500
    
    return jsonify({'error': 'No allocation data available. Please upload staff details file first.'}), 400

def send_reminder_email(agent_info):
    """Send a reminder email to an agent prompting them to upload their work"""
    try:
        agent_name = agent_info.get('name', 'Agent')
        agent_email = agent_info.get('email')
        allocated = agent_info.get('allocated', 0)
        
        if not agent_email:
            return False, "No email address"
        
        if allocated == 0:
            return False, "No allocated work to remind about"
        
        # Get allocation summary
        summary = get_allocation_summary(agent_name, agent_info)
        
        # Format insurance companies list
        insurance_list = ', '.join(sorted(summary['insurance_companies'])) if summary['insurance_companies'] else 'None'
        
        # Send reminder email
        msg = Message(
            subject=f'Reminder: Please Upload Your Work - {agent_name}',
            recipients=[agent_email],
            body=f'''
Dear {agent_name},

This is a friendly reminder to upload your completed work.

📊 YOUR CURRENT ALLOCATION:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
• Total Allocated: {summary['total_allocated']} rows
• First Priority: {summary['first_priority_count']} rows
• Second Priority: {summary['second_priority_count']} rows
• Third Priority: {summary['third_priority_count']} rows

🏥 INSURANCE COMPANIES:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
{insurance_list}

⏰ Please log into the system and upload your completed work.

Best regards,
Allocation Management System
            ''',
            html=f'''
            <div style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto; padding: 20px;">
                <h2 style="color: #333;">📧 Work Upload Reminder</h2>
                <p>Dear <strong>{agent_name}</strong>,</p>
                <p>This is a friendly reminder to upload your completed work.</p>
                
                <div style="background-color: #e7f3ff; padding: 15px; border-radius: 5px; margin: 20px 0;">
                    <h3 style="margin-top: 0; color: #0056b3;">📊 Your Current Allocation</h3>
                    <ul style="margin: 10px 0; padding-left: 20px;">
                        <li>Total Allocated: <strong>{summary['total_allocated']} rows</strong></li>
                        <li>First Priority: <strong>{summary['first_priority_count']} rows</strong></li>
                        <li>Second Priority: <strong>{summary['second_priority_count']} rows</strong></li>
                        <li>Third Priority: <strong>{summary['third_priority_count']} rows</strong></li>
                    </ul>
                </div>
                
                <div style="background-color: #fff3cd; padding: 15px; border-radius: 5px; margin: 20px 0;">
                    <h3 style="margin-top: 0; color: #856404;">🏥 Insurance Companies</h3>
                    <p style="word-wrap: break-word;">{insurance_list}</p>
                </div>
                
                <div style="background-color: #d4edda; padding: 15px; border-radius: 5px; margin: 20px 0; border-left: 4px solid #28a745;">
                    <p style="font-size: 16px; font-weight: bold; color: #155724; margin: 10px 0;">⏰ Please log into the system and upload your completed work.</p>
                </div>
                
                <p>Best regards,<br>
                Allocation Management System</p>
            </div>
            '''
        )
        
        mail.send(msg)
        return True, f"Reminder sent to {agent_email}"
        
    except Exception as e:
        return False, str(e)

def check_and_send_reminders():
    """Check which agents need reminders and send them every 2 hours from shift start time"""
    global agent_allocations_for_reminders
    
    if not agent_allocations_for_reminders:
        return
    
    current_time = datetime.now()
    current_hour = current_time.hour
    current_minute = current_time.minute
    
    successful_reminders = []
    failed_reminders = []
    
    for agent in agent_allocations_for_reminders:
        shift_start_time_str = agent.get('shift_start_time')
        agent_email = agent.get('email')
        allocated = agent.get('allocated', 0)
        
        # Skip if no shift start time, email, or allocated work
        if not shift_start_time_str or not agent_email or allocated == 0:
            continue
        
        try:
            # Parse shift start time (format: HH:MM)
            shift_hour, shift_minute = map(int, shift_start_time_str.split(':'))
            shift_start_today = current_time.replace(hour=shift_hour, minute=shift_minute, second=0, microsecond=0)
            
            # If shift hasn't started yet today, skip
            if shift_start_today > current_time:
                continue
            
            # Calculate hours since shift started today
            hours_since_start = (current_time - shift_start_today).total_seconds() / 3600
            
            # Calculate which reminder interval we're at (0, 2, 4, 6, 8, etc. hours)
            reminder_interval = 2  # hours
            interval_number = int(hours_since_start // reminder_interval)
            next_interval_time = shift_start_today + timedelta(hours=interval_number * reminder_interval)
            next_interval_time_plus_one = next_interval_time + timedelta(hours=reminder_interval)
            
            # Check if current time is within 5 minutes before or after a reminder interval
            tolerance_minutes = 5
            time_diff = abs((current_time - next_interval_time).total_seconds() / 60)
            
            if time_diff <= tolerance_minutes:
                # We're at a reminder interval. Check if we haven't sent one recently
                last_reminder_key = f"last_reminder_{agent.get('id')}"
                if not hasattr(app, '_reminder_tracker'):
                    app._reminder_tracker = {}
                
                last_reminder_time = app._reminder_tracker.get(last_reminder_key)
                
                if last_reminder_time:
                    minutes_since_last = (current_time - last_reminder_time).total_seconds() / 60
                    if minutes_since_last < 100:  # Don't send if sent within last 100 minutes
                        continue
                
                # Send reminder
                success, message = send_reminder_email(agent)
                if success:
                    successful_reminders.append(f"{agent.get('name')} ({agent_email})")
                    if not hasattr(app, '_reminder_tracker'):
                        app._reminder_tracker = {}
                    app._reminder_tracker[last_reminder_key] = current_time
                else:
                    failed_reminders.append(f"{agent.get('name')}: {message}")
        
        except Exception as e:
            failed_reminders.append(f"{agent.get('name')}: {str(e)}")
    
    # Log reminder results (optional - you can remove this if you don't want logging)
    if successful_reminders or failed_reminders:
        print(f"[Reminder System] Sent {len(successful_reminders)} reminders, {len(failed_reminders)} failed")
        if failed_reminders:
            print(f"[Reminder System] Failed: {', '.join(failed_reminders[:5])}")  # Show first 5 failures

def get_allocation_summary(agent_name, agent_info):
    """Get detailed allocation summary for an agent"""
    global data_file_data
    
    summary = {
        'total_allocated': agent_info.get('allocated', 0),
        'capacity': agent_info.get('capacity', 0),
        'first_priority_count': 0,
        'second_priority_count': 0,
        'third_priority_count': 0,
        'unknown_priority_count': 0,
        'insurance_companies': set(),
        'first_priority_deadline': None
    }
    
    # Get the agent's allocated rows
    row_indices = agent_info.get('row_indices', [])
    
    if row_indices and data_file_data:
        # Get the main data
        if isinstance(data_file_data, dict):
            first_sheet_name = list(data_file_data.keys())[0]
            main_df = data_file_data[first_sheet_name]
        else:
            main_df = data_file_data
        
        if len(main_df) > 0:
            # Get allocated rows
            allocated_df = main_df.iloc[row_indices].copy()
            
            # Count by priority
            if 'Priority Status' in allocated_df.columns:
                summary['first_priority_count'] = len(allocated_df[allocated_df['Priority Status'] == 'First Priority'])
                summary['second_priority_count'] = len(allocated_df[allocated_df['Priority Status'] == 'Second Priority'])
                summary['third_priority_count'] = len(allocated_df[allocated_df['Priority Status'] == 'Third Priority'])
                unknown_mask = allocated_df['Priority Status'].isin(['', 'Unknown', None]) | allocated_df['Priority Status'].isna()
                summary['unknown_priority_count'] = len(allocated_df[unknown_mask])
            
            # Get unique insurance companies
            insurance_col = None
            for col in allocated_df.columns:
                if 'dental' in col.lower() and 'primary' in col.lower() and 'ins' in col.lower():
                    insurance_col = col
                    break
            
            if insurance_col:
                insurance_companies = allocated_df[insurance_col].dropna().unique()
                summary['insurance_companies'] = set([str(ic).strip() for ic in insurance_companies if str(ic).strip() and str(ic).strip().lower() != 'unknown'])
            
            # Calculate First Priority deadline (2nd business day end of day)
            if summary['first_priority_count'] > 0:
                from datetime import datetime, time
                today = datetime.now().date()
                second_business_day = get_nth_business_day(today, 2)
                # Set deadline to end of business day (5:00 PM) on 2nd business day
                summary['first_priority_deadline'] = datetime.combine(second_business_day, time(17, 0))
    
    return summary

def create_agent_excel_file(agent_name, agent_info):
    """Create Excel file with agent's allocated data"""
    try:
        # Get the agent's allocated row indices
        row_indices = agent_info.get('row_indices', [])
        
        if not row_indices or data_file_data is None:
            # If no specific rows or no data, create empty DataFrame
            allocated_df = pd.DataFrame({'Message': ['No data allocated to this agent']})
        else:
            # data_file_data is a dictionary, get the first sheet (main data)
            if isinstance(data_file_data, dict):
                # Get the first sheet from the dictionary
                first_sheet_name = list(data_file_data.keys())[0]
                main_df = data_file_data[first_sheet_name]
            else:
                # If it's already a DataFrame
                main_df = data_file_data
            
            # Get the actual allocated rows from the processed data using row indices
            allocated_df = main_df.iloc[row_indices].copy()
        
        # Create Excel buffer
        excel_buffer = io.BytesIO()
        
        with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
            # Write main data
            allocated_df.to_excel(writer, sheet_name='Allocated Data', index=False)
            
            # Create summary sheet
            summary_data = {
                'Agent Name': [agent_name],
                'Total Allocated': [agent_info['allocated']],
                'Capacity': [agent_info['capacity']],
                'Allocation Date': [datetime.now().strftime('%Y-%m-%d %H:%M:%S')],
                'Status': ['Approved']
            }
            summary_df = pd.DataFrame(summary_data)
            summary_df.to_excel(writer, sheet_name='Summary', index=False)
        
        excel_buffer.seek(0)
        return excel_buffer
        
    except Exception as e:
        # Return empty Excel file as fallback
        excel_buffer = io.BytesIO()
        empty_df = pd.DataFrame({'Message': ['No data available']})
        empty_df.to_excel(excel_buffer, index=False)
        excel_buffer.seek(0)
        return excel_buffer

@app.route('/reset_app', methods=['POST'])
@admin_required
def reset_app():
    global allocation_data, data_file_data, allocation_filename, data_filename, processing_result
    global agent_allocations_data
    
    try:
        # Clear all agent work files from database
        AgentWorkFile.query.delete()
        
        # Clear all allocations from database
        Allocation.query.delete()
        
        # Reset all global variables
        allocation_data = None
        data_file_data = None
        allocation_filename = None
        data_filename = None
        processing_result = "🔄 Application reset successfully! All files, data, and agent consolidation files have been cleared."
        agent_allocations_data = None
        
        # Commit database changes
        db.session.commit()
        
        return redirect('/')
        
    except Exception as e:
        db.session.rollback()
        processing_result = f"❌ Error resetting application: {str(e)}"
        return redirect('/')

if __name__ == '__main__':
    import os
    import threading
    import time
    
    # Initialize database
    init_database()
    
    # Load insurance name mapping at startup
    load_insurance_name_mapping()
    
    # Start session cleanup thread
    def cleanup_sessions_periodically():
        while True:
            try:
                with app.app_context():
                    cleanup_expired_sessions()
                time.sleep(3600)  # Clean up every hour
            except Exception as e:
                time.sleep(3600)
    
    cleanup_thread = threading.Thread(target=cleanup_sessions_periodically, daemon=True)
    cleanup_thread.start()
    
    # Set up scheduler for reminder emails
    scheduler = BackgroundScheduler()
    scheduler.add_job(
        func=lambda: check_and_send_reminders(),
        trigger=IntervalTrigger(hours=2),
        id='reminder_check',
        name='Check and send reminder emails every 2 hours',
        replace_existing=True
    )
    scheduler.start()
    print("✅ Reminder email scheduler started - checking every 2 hours")
    
    port = int(os.environ.get('PORT', 5003))
    # Always enable debug + auto-reload for local dev unless explicitly disabled
    debug = True if os.environ.get('DISABLE_DEBUG') != '1' else False
    
    try:
        app.run(debug=debug, host='0.0.0.0', port=port, use_reloader=debug)
    finally:
        # Shutdown scheduler when app stops
        if scheduler.running:
            scheduler.shutdown()
