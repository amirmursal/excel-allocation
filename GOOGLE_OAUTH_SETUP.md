# Google OAuth Setup Guide

This guide will help you set up Google OAuth authentication for your Excel Allocation System.

## Prerequisites

- A Google account
- Access to Google Cloud Console

## Step 1: Create Google Cloud Project

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create a new project or select an existing one
3. Enable the Google+ API (if not already enabled)

## Step 2: Configure OAuth Consent Screen

1. In the Google Cloud Console, go to "APIs & Services" > "OAuth consent screen"
2. Choose "External" user type (unless you have a Google Workspace account)
3. Fill in the required information:
   - App name: "Excel Allocation System"
   - User support email: Your email
   - Developer contact information: Your email
4. Add your domain to authorized domains (if you have one)
5. Save and continue

## Step 3: Create OAuth 2.0 Credentials

1. Go to "APIs & Services" > "Credentials"
2. Click "Create Credentials" > "OAuth 2.0 Client IDs"
3. Choose "Web application" as the application type
4. Set the name: "Excel Allocation System"
5. Add authorized redirect URIs:
   - For local development: `http://localhost:5003/callback`
   - For production: `https://yourdomain.com/callback`
6. Click "Create"
7. Copy the Client ID and Client Secret

## Step 4: Configure Environment Variables

Create a `.env` file in your project root (if it doesn't exist) and add:

```env
GOOGLE_CLIENT_ID=your_google_client_id_here
GOOGLE_CLIENT_SECRET=your_google_client_secret_here
```

## Step 5: Install Dependencies

Install the required packages:

```bash
pip install -r requirements.txt
```

## Step 6: Database Migration

Since we've added new fields to the User model, you need to create and run a migration:

```bash
flask db init  # Only if this is the first migration
flask db migrate -m "Add Google OAuth fields"
flask db upgrade
```

## Step 7: Test the Integration

1. Start your application:

   ```bash
   python app.py
   ```

2. Go to `http://localhost:5003`
3. Click "Login with Google"
4. Complete the OAuth flow
5. You should be logged in as a new agent user

## User Management

### Automatic User Creation

- New users logging in with Google OAuth will be automatically created as "agent" role
- Existing users can be linked to their Google accounts

### Admin Access

- To give admin access to a Google OAuth user, update their role in the database:
  ```sql
  UPDATE users SET role = 'admin' WHERE email = 'user@example.com';
  ```

### User Roles

- **Admin**: Can upload files, process data, and manage the system
- **Agent**: Can upload work files and view their own data

## Security Notes

1. Keep your Google Client Secret secure
2. Use HTTPS in production
3. Regularly review OAuth consent screen settings
4. Monitor user access in Google Cloud Console

## Troubleshooting

### Common Issues

1. **"OAuth is not configured" error**

   - Check that GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET are set in your environment

2. **"Token verification failed" error**

   - Ensure your redirect URI matches exactly what's configured in Google Cloud Console
   - Check that the Google+ API is enabled

3. **"Authorization failed" error**
   - Verify the OAuth consent screen is properly configured
   - Check that the user's email domain is authorized (if using domain restrictions)

### Testing with Different Accounts

- Test with different Gmail accounts to ensure the OAuth flow works correctly
- Verify that new users are created with the correct default role
- Test that existing users can link their Google accounts

## Production Deployment

For production deployment:

1. Update the redirect URI in Google Cloud Console to your production domain
2. Ensure your production environment has the correct environment variables
3. Use HTTPS for all OAuth redirects
4. Consider implementing additional security measures like rate limiting

## Support

If you encounter issues:

1. Check the application logs for detailed error messages
2. Verify your Google Cloud Console configuration
3. Test with a simple OAuth flow first
4. Ensure all dependencies are properly installed
