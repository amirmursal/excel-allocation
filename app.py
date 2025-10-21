#!/usr/bin/env python3
"""
Excel Allocation System - Web Application
Admin can upload allocation and data files, Agent can upload status files
"""

from flask import Flask, render_template_string, request, jsonify, send_file, redirect
import pandas as pd
import os
from datetime import datetime
from werkzeug.utils import secure_filename
import tempfile

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max file size

# Global variables to store session data
allocation_data = None
data_file_data = None
allocation_filename = None
data_filename = None
processing_result = None

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
    </style>
    <link href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0/css/all.min.css" rel="stylesheet">
</head>
<body>
    <div class="container">
        <div class="header">
            <h1><i class="fas fa-file-excel"></i> Excel Allocation System</h1>
            <p>Upload and process Excel files for allocation management</p>
            
            <div class="role-selector">
                <button class="role-btn active" onclick="switchRole('admin')">
                    <i class="fas fa-user-shield"></i> Admin
                </button>
                <button class="role-btn" onclick="switchRole('agent')">
                    <i class="fas fa-user"></i> Agent
                </button>
            </div>
        </div>

        <div class="content">
            <!-- Admin Panel -->
            <div id="admin-panel" class="panel active">
                <div class="upload-grid">
                    <div class="upload-card">
                        <form action="/upload_allocation" method="post" enctype="multipart/form-data" id="allocation-form">
                            <div class="form-group">
                                <input type="file" id="allocation_file" name="file" accept=".xlsx,.xls" required>
                            </div>
                            <button type="submit" id="allocation-btn">📤 Upload Agent Allocation Details</button>
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
                        </div>
                        
                        <!-- Second Priority Panel -->
                        <div id="second-priority-panel" class="priority-panel" style="display: none;">
                            
                            
                            <!-- Calendar Input for Second Priority Dates -->
                            <div class="form-group">
                                <div id="calendar_container_second" style="border: 1px solid #ddd; padding: 15px; background: white; border-radius: 8px; margin: 10px 0;"></div>
                                <div id="selected_dates_info_second" style="background: #f8f9fa; padding: 10px; border-radius: 5px; border: 1px solid #e9ecef;">
                                    <strong>Selected Second Priority Dates:</strong> <span id="selected_count_second">0</span> <span id="selected_text_second">dates selected</span>
                                    <div id="selected_dates_list_second" style="margin-top: 5px; font-size: 12px; color: #666;"></div>
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
                    <h3>💾 Download Processed File</h3>
                    <p>Download your Excel file with updated Priority Status assignments.</p>
                    <form action="/download_result" method="post">
                        <div class="form-group">
                            <label for="output_filename">Output filename (optional):</label>
                            <input type="text" id="output_filename" name="filename" 
                                   placeholder="processed_allocation_data.xlsx" style="width: 100%; padding: 8px; border: 1px solid #ddd; border-radius: 4px;">
                        </div>
                        <button type="submit" class="process-btn" style="background: linear-gradient(135deg, #3498db, #2980b9);">
                            <i class="fas fa-download"></i> Download Processed File
                        </button>
                    </form>
                </div>
                {% endif %}

                <!-- Reset Section -->
                <div class="section">
                    <h3>🔄 Reset Application</h3>
                    <p>Clear all uploaded files and reset the application to start fresh.</p>
                    <form action="/reset_app" method="post" onsubmit="return confirm('Are you sure you want to reset the application? This will clear all uploaded files and data.')">
                        <button type="submit" class="reset-btn">🗑️ Reset Application</button>
                    </form>
                </div>
            </div>

            <!-- Agent Panel -->
            <div id="agent-panel" class="panel">
                <div class="coming-soon">
                    <i class="fas fa-tools"></i>
                    <h3>Under Development</h3>
                    <p>Agent functionality will be available soon</p>
                </div>
            </div>
        </div>
    </div>

    <script>
        function switchRole(role) {
            // Update button states
            document.querySelectorAll('.role-btn').forEach(btn => btn.classList.remove('active'));
            event.target.classList.add('active');
            
            // Show/hide panels
            document.querySelectorAll('.panel').forEach(panel => panel.classList.remove('active'));
            document.getElementById(role + '-panel').classList.add('active');
        }
        
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
            }
        }

        // Form submission with loading states - with null checks
        const allocationForm = document.getElementById('allocation-form');
        if (allocationForm) {
            allocationForm.addEventListener('submit', function() {
                const btn = document.getElementById('allocation-btn');
                if (btn) {
                    btn.disabled = true;
                    btn.textContent = 'Uploading...';
                }
            });
        }

        const dataForm = document.getElementById('data-form');
        if (dataForm) {
            dataForm.addEventListener('submit', function() {
                const btn = document.getElementById('data-btn');
                if (btn) {
                    btn.disabled = true;
                    btn.textContent = 'Uploading...';
                }
            });
        }

        const processForm = document.getElementById('process-form');
        if (processForm) {
            processForm.addEventListener('submit', function(e) {
                e.preventDefault();
                processFiles();
            });
        }
        
        // Populate date fields when page loads
        document.addEventListener('DOMContentLoaded', function() {
            // Load appointment dates from uploaded file
            loadAppointmentDates();
            
            // Also try to load calendar after a short delay to ensure page is fully loaded
            setTimeout(() => {
                loadAppointmentDates();
            }, 1000);
        });
        
        // Global variables for calendar
        let currentDate = new Date();
        let appointmentDates = new Set();
        let selectedDates = new Set();
        let selectedSecondDates = new Set();
        
        function loadAppointmentDates() {
            const calendarContainer = document.getElementById('calendar_container');
            if (!calendarContainer) return;
            
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
                        return;
                    }
                    
                    const dates = data.appointment_dates;
                    const columnName = data.column_name;
                    
                    if (!dates || dates.length === 0) {
                        calendarContainer.innerHTML = '<p style="color: #666; font-style: italic; text-align: center; padding: 20px;">No appointment dates found in the file.</p>';
                        return;
                    }
                    
                    
                    // Store appointment dates
                    appointmentDates = new Set(dates);
                    // Directly show checkbox list (no calendar view)
                    showFallbackDateList(dates, columnName);
                    updateSelectedDatesInfo();
                })
                .catch(error => {
                    calendarContainer.innerHTML = `<p style="color: #e74c3c; text-align: center; padding: 20px;">Error loading appointment dates: ${error.message}</p>`;
                });
        }
        
        function loadAppointmentDatesSecond() {
            const calendarContainer = document.getElementById('calendar_container_second');
            if (!calendarContainer) return;
            
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
                        return;
                    }
                    
                    const dates = data.appointment_dates;
                    const columnName = data.column_name;
                    
                    if (!dates || dates.length === 0) {
                        calendarContainer.innerHTML = '<p style="color: #666; font-style: italic; text-align: center; padding: 20px;">No appointment dates found in the file.</p>';
                        return;
                    }
                    
                    
                    // Store appointment dates
                    appointmentDates = new Set(dates);
                    // Directly show checkbox list (no calendar view)
                    showFallbackDateListSecond(dates, columnName);
                    updateSelectedDatesInfoSecond();
                })
                .catch(error => {
                    calendarContainer.innerHTML = `<p style="color: #e74c3c; text-align: center; padding: 20px;">Error loading appointment dates: ${error.message}</p>`;
                });
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
        
        function showFallbackDateList(dates, columnName) {
            const calendarContainer = document.getElementById('calendar_container');
            if (!calendarContainer) return;
            
            let html = `
                <div style="text-align: center; margin-bottom: 20px;">
                    <p>Click on dates to select them for First Priority:</p>
                </div>
                <div style="display: grid; grid-template-columns: repeat(auto-fill, minmax(200px, 1fr)); gap: 10px; max-height: 300px; overflow-y: auto;">
            `;
            
            dates.forEach((date, index) => {
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
                
                if (isSelectedInFirst) {
                    itemStyle = 'display: flex; align-items: center; padding: 10px; border: 2px solid #4caf50; border-radius: 8px; background: #4caf50; color: white; cursor: pointer; transition: all 0.3s;';
                    textStyle = 'font-weight: bold; font-size: 16px; color: white;';
                    dayStyle = 'color: rgba(255,255,255,0.8); font-size: 14px;';
                } else if (isDisabled) {
                    itemStyle = 'display: flex; align-items: center; padding: 10px; border: 2px solid #f39c12; border-radius: 8px; background: #f39c12; color: white; cursor: not-allowed; opacity: 0.7; transition: all 0.3s;';
                    textStyle = 'font-weight: bold; font-size: 16px; color: white;';
                    dayStyle = 'color: rgba(255,255,255,0.8); font-size: 14px;';
                }
                
                html += `
                    <div style="${itemStyle}"
                         onclick="${isDisabled ? '' : `toggleDate('${date}')`}" 
                         id="date_${index}">
                        <input type="checkbox" id="checkbox_${index}" data-date="${date}" style="margin-right: 10px; transform: scale(1.2);" ${isDisabled ? 'disabled' : ''}>
                        <div>
                            <div style="${textStyle}">${formattedDate}${isDisabled ? ' (Second Priority)' : ''}</div>
                            <div style="${dayStyle}">${dayName}</div>
                        </div>
                    </div>
                `;
            });
            
            html += '</div>';
            calendarContainer.innerHTML = html;
            // Sync checkboxes to current selection
            syncFallbackCheckboxes();
        }
        
        function showFallbackDateListSecond(dates, columnName) {
            const calendarContainer = document.getElementById('calendar_container_second');
            if (!calendarContainer) return;
            
            let html = `
                <div style="text-align: center; margin-bottom: 20px;">
                    <p>Click on dates to select them for Second Priority:</p>
                </div>
                <div style="display: grid; grid-template-columns: repeat(auto-fill, minmax(200px, 1fr)); gap: 10px; max-height: 300px; overflow-y: auto;">
            `;
            
            dates.forEach((date, index) => {
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
                
                if (isSelectedInSecond) {
                    itemStyle = 'display: flex; align-items: center; padding: 10px; border: 2px solid #f39c12; border-radius: 8px; background: #f39c12; color: white; cursor: pointer; transition: all 0.3s;';
                    textStyle = 'font-weight: bold; font-size: 16px; color: white;';
                    dayStyle = 'color: rgba(255,255,255,0.8); font-size: 14px;';
                } else if (isDisabled) {
                    itemStyle = 'display: flex; align-items: center; padding: 10px; border: 2px solid #4caf50; border-radius: 8px; background: #4caf50; color: white; cursor: not-allowed; opacity: 0.7; transition: all 0.3s;';
                    textStyle = 'font-weight: bold; font-size: 16px; color: white;';
                    dayStyle = 'color: rgba(255,255,255,0.8); font-size: 14px;';
                }
                
                html += `
                    <div style="${itemStyle}"
                         onclick="${isDisabled ? '' : `toggleDateSecond('${date}')`}" 
                         id="date_second_${index}">
                        <input type="checkbox" id="checkbox_second_${index}" data-date="${date}" style="margin-right: 10px; transform: scale(1.2);" ${isDisabled ? 'disabled' : ''}>
                        <div>
                            <div style="${textStyle}">${formattedDate}${isDisabled ? ' (First Priority)' : ''}</div>
                            <div style="${dayStyle}">${dayName}</div>
                        </div>
                    </div>
                `;
            });
            
            html += '</div>';
            calendarContainer.innerHTML = html;
            // Sync checkboxes to current selection
            syncFallbackCheckboxesSecond();
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
                console.error('Progress elements not found');
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
                console.error('Error:', error);
            });
        }
    </script>
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
        
        # Convert appointment date column to datetime
        try:
            processed_df[appointment_date_col] = pd.to_datetime(processed_df[appointment_date_col], errors='coerce')
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

def process_allocation_files_with_dates(allocation_df, data_df, selected_dates, custom_dates, appointment_dates, appointment_dates_second=None):
    """Process data file with priority assignment based on selected dates"""
    try:
        from datetime import datetime, timedelta
        import pandas as pd
        
        # Use data_df as the main file to process
        processed_df = data_df.copy()
        
        # Find the appointment date column
        appointment_date_col = None
        for col in processed_df.columns:
            if 'appointment' in col.lower() and 'date' in col.lower():
                appointment_date_col = col
                break
        
        if appointment_date_col is None:
            return f"❌ Error: 'Appointment Date' column not found in data file.\nAvailable columns: {list(processed_df.columns)}", None
        
        # Keep original appointment date format - don't convert to datetime
        # We'll work with string dates to preserve original format (MM/DD/YY)
        
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
            original_appointment_value = appointment_date
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
                processed_df.at[idx, 'Priority Status'] = 'First Priority'
                first_priority_count += 1
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
📅 Based on column: '{appointment_date_col}'

💾 Ready to download the processed result file!"""
        
        return result_message, processed_df
        
    except Exception as e:
        return f"❌ Error during processing: {str(e)}", None

@app.route('/')
def index():
    global allocation_data, data_file_data, allocation_filename, data_filename, processing_result
    return render_template_string(HTML_TEMPLATE, 
                                allocation_data=allocation_data, 
                                data_file_data=data_file_data,
                                allocation_filename=allocation_filename,
                                data_filename=data_filename,
                                processing_result=processing_result)

@app.route('/upload_allocation', methods=['POST'])
def upload_allocation_file():
    global allocation_data, allocation_filename, processing_result
    
    if 'file' not in request.files:
        processing_result = "❌ Error: No file provided"
        return redirect('/')
    
    file = request.files['file']
    if file.filename == '':
        processing_result = "❌ Error: No file selected"
        return redirect('/')
    
    try:
        # Save uploaded file
        filename = secure_filename(file.filename)
        file.save(filename)
        
        # Load Excel file
        allocation_data = pd.read_excel(filename, sheet_name=None)
        allocation_filename = filename
        
        processing_result = f"✅ Allocation file uploaded successfully! Loaded {len(allocation_data)} sheets: {', '.join(list(allocation_data.keys()))}"
        return redirect('/')
        
    except Exception as e:
        processing_result = f"❌ Error uploading allocation file: {str(e)}"
        return redirect('/')

@app.route('/upload_data', methods=['POST'])
def upload_data_file():
    global data_file_data, data_filename, processing_result
    
    if 'file' not in request.files:
        processing_result = "❌ Error: No file provided"
        return redirect('/')
    
    file = request.files['file']
    if file.filename == '':
        processing_result = "❌ Error: No file selected"
        return redirect('/')
    
    try:
        # Save uploaded file
        filename = secure_filename(file.filename)
        file.save(filename)
        
        # Load Excel file
        data_file_data = pd.read_excel(filename, sheet_name=None)
        data_filename = filename
        
        processing_result = f"✅ Data file uploaded successfully! Loaded {len(data_file_data)} sheets: {', '.join(list(data_file_data.keys()))}"
        return redirect('/')
        
    except Exception as e:
        processing_result = f"❌ Error uploading data file: {str(e)}"
        return redirect('/')

@app.route('/process_files', methods=['POST'])
def process_files():
    global allocation_data, data_file_data, processing_result
    
    if not data_file_data:
        processing_result = "❌ Error: Please upload data file first"
        return render_template_string(HTML_TEMPLATE, 
                                    allocation_data=allocation_data, 
                                    data_file_data=data_file_data,
                                    allocation_filename=allocation_filename,
                                    data_filename=data_filename,
                                    processing_result=processing_result)
    
    try:
        # Get the first sheet from data file
        data_df = list(data_file_data.values())[0]
        
        # Get selected appointment dates from calendar
        appointment_dates = request.form.getlist('appointment_dates')
        appointment_dates_second = request.form.getlist('appointment_dates_second')
        debug_count = request.form.get('debug_selected_count', '0')
        debug_count_second = request.form.get('debug_selected_count_second', '0')
        
        # Process the data file with selected dates
        result_message, processed_df = process_allocation_files_with_dates(None, data_df, [], '', appointment_dates, appointment_dates_second)
        
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
                                    processing_result=processing_result)
        
    except Exception as e:
        processing_result = f"❌ Error processing data file: {str(e)}"
        return render_template_string(HTML_TEMPLATE, 
                                    allocation_data=allocation_data, 
                                    data_file_data=data_file_data,
                                    allocation_filename=allocation_filename,
                                    data_filename=data_filename,
                                    processing_result=processing_result)

@app.route('/download_result', methods=['POST'])
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
                    df.to_excel(writer, sheet_name=sheet_name, index=False)
            
            return send_file(temp_path, as_attachment=True, download_name=filename)
            
        finally:
            # Clean up temporary file
            os.close(temp_fd)
            if os.path.exists(temp_path):
                os.unlink(temp_path)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/get_appointment_dates')
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
        
        # Get unique appointment dates
        appointment_dates = data_df[appointment_date_col].dropna().unique()
        
        # Convert to string format and sort
        date_strings = []
        for date in appointment_dates:
            if hasattr(date, 'date'):
                date_str = date.date().strftime('%Y-%m-%d')
            else:
                date_str = str(date)
            date_strings.append(date_str)
        
        date_strings.sort()
        
        return jsonify({
            'appointment_dates': date_strings,
            'column_name': appointment_date_col
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/reset_app', methods=['POST'])
def reset_app():
    global allocation_data, data_file_data, allocation_filename, data_filename, processing_result
    
    try:
        # Reset all global variables
        allocation_data = None
        data_file_data = None
        allocation_filename = None
        data_filename = None
        processing_result = "🔄 Application reset successfully! All files and data have been cleared."
        
        return redirect('/')
        
    except Exception as e:
        processing_result = f"❌ Error resetting application: {str(e)}"
        return redirect('/')

if __name__ == '__main__':
    import os
    port = int(os.environ.get('PORT', 5003))
    # Always enable debug + auto-reload for local dev unless explicitly disabled
    debug = True if os.environ.get('DISABLE_DEBUG') != '1' else False
    
    app.run(debug=debug, host='0.0.0.0', port=port, use_reloader=debug)
