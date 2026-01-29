// routes/auth.js - Authentication routes
const express = require('express');
const { body, validationResult } = require('express-validator');
const User = require('../models/User');
const { AuthService, loginLimiter, requireAdmin } = require('../auth');
const { cryptoUtil } = require('../utils/crypto'); // Import crypto utilities

const router = express.Router();

// Validation rules for login (updated to accept handle or email)
const loginValidation = [
  body('login')
    .trim()
    .notEmpty()
    .withMessage('User handle or email is required')
    .isLength({ min: 3, max: 100 })
    .withMessage('Login must be between 3 and 100 characters'),
  body('password')
    .isLength({ min: 6 })
    .withMessage('Password must be at least 6 characters long')
];

// POST /auth/login - Admin login (updated to accept handle or email)
router.post('/login', loginLimiter, loginValidation, async (req, res) => {
  try {
    // Check for validation errors
    const errors = validationResult(req);
    if (!errors.isEmpty()) {
      return res.status(400).json({ 
        error: 'Validation failed', 
        details: errors.array() 
      });
    }

    const { login, password } = req.body;

    // Use crypto utility to find user by handle or email
    const user = await cryptoUtil.findUserByLoginCredential(login);
    
    if (!user) {
      return res.status(401).json({ 
        error: 'Invalid login credentials' 
      });
    }

    // Check if user is admin
    if (!user.is_admin) {
      return res.status(403).json({ 
        error: 'Access denied. Admin privileges required.' 
      });
    }

    // Verify password (your passwords are already bcrypt hashed)
    const isPasswordValid = await AuthService.comparePassword(password, user.user_password);
    if (!isPasswordValid) {
      return res.status(401).json({ 
        error: 'Invalid login credentials' 
      });
    }

    // Update last active date
    await user.updateLastActive();

    // Generate token
    const token = AuthService.generateToken(user);

    // Log successful login (for audit purposes)
    console.log(`Admin login successful: ${user.user_handle} (${user.display_name}) at ${new Date().toISOString()}`);

    // Return success response
    res.json({
      message: 'Login successful',
      token,
      user: {
        id: user._id,
        user_handle: user.user_handle,
        display_name: user.display_name,
        user_firstname: user.user_firstname,
        user_lastname: user.user_lastname,
        is_admin: user.is_admin,
        last_active_date: user.last_active_date
      }
    });

  } catch (error) {
    console.error('Login error:', error);
    res.status(500).json({ 
      error: 'Server error during login' 
    });
  }
});

// Helper function to decrypt email (you'll need to implement this based on your encryption)
async function decryptEmail(encryptedEmail) {
  try {
    // This is a placeholder - you'll need to implement based on your encryption method
    // It looks like you're using Fernet encryption based on the format
    
    // Example implementation if using cryptography library:
    // const crypto = require('crypto');
    // const key = process.env.ENCRYPTION_KEY;
    // return decrypt(encryptedEmail, key);
    
    // For now, return null to skip encrypted email search
    // You can implement this based on your specific encryption setup
    return null;
  } catch (error) {
    return null;
  }
}

// POST /auth/refresh - Refresh token (updated for your schema)
router.post('/refresh', requireAdmin, async (req, res) => {
  try {
    // Generate new token for current user
    const token = AuthService.generateToken(req.user);
    
    res.json({
      message: 'Token refreshed successfully',
      token,
      user: {
        id: req.user._id,
        user_handle: req.user.user_handle,
        display_name: req.user.display_name,
        user_firstname: req.user.user_firstname,
        user_lastname: req.user.user_lastname,
        is_admin: req.user.is_admin
      }
    });
  } catch (error) {
    console.error('Token refresh error:', error);
    res.status(500).json({ 
      error: 'Server error during token refresh' 
    });
  }
});

// POST /auth/logout - Logout (updated for your schema)
router.post('/logout', (req, res) => {
  // With JWT, logout is primarily handled client-side by removing the token
  // But we can log the action for audit purposes
  if (req.headers.authorization) {
    try {
      const token = req.headers.authorization.split(' ')[1];
      const decoded = AuthService.verifyToken(token);
      console.log(`Admin logout: ${decoded.user_handle || decoded.id} at ${new Date().toISOString()}`);
    } catch (error) {
      // Token might be invalid, but that's okay for logout
    }
  }

  res.json({ message: 'Logged out successfully' });
});

// GET /auth/me - Get current user info (updated for your schema)
router.get('/me', requireAdmin, (req, res) => {
  res.json({
    user: {
      id: req.user._id,
      user_handle: req.user.user_handle,
      display_name: req.user.display_name,
      user_firstname: req.user.user_firstname,
      user_lastname: req.user.user_lastname,
      user_bio: req.user.user_bio,
      user_genres: req.user.user_genres,
      date_joined: req.user.date_joined,
      last_active_date: req.user.last_active_date,
      is_admin: req.user.is_admin,
      current_reading_goal: req.user.getCurrentReadingGoal(),
      badges_count: req.user.user_badges.length,
      clubs_count: req.user.user_clubs.length
    }
  });
});

// POST /auth/change-password - Change admin password (updated for your schema)
router.post('/change-password', requireAdmin, [
  body('currentPassword')
    .notEmpty()
    .withMessage('Current password is required'),
  body('newPassword')
    .isLength({ min: 8 })
    .matches(/^(?=.*[a-z])(?=.*[A-Z])(?=.*\d)(?=.*[@$!%*?&])[A-Za-z\d@$!%*?&]/)
    .withMessage('New password must be at least 8 characters and include uppercase, lowercase, number, and special character')
], async (req, res) => {
  try {
    const errors = validationResult(req);
    if (!errors.isEmpty()) {
      return res.status(400).json({ 
        error: 'Validation failed', 
        details: errors.array() 
      });
    }

    const { currentPassword, newPassword } = req.body;
    const user = await User.findById(req.user._id);

    // Verify current password (using your field name)
    const isCurrentPasswordValid = await AuthService.comparePassword(currentPassword, user.user_password);
    if (!isCurrentPasswordValid) {
      return res.status(400).json({ 
        error: 'Current password is incorrect' 
      });
    }

    // Hash and save new password
    const hashedNewPassword = await AuthService.hashPassword(newPassword);
    await User.findByIdAndUpdate(req.user._id, { 
      user_password: hashedNewPassword,
      last_active_date: new Date().toISOString().split('T')[0] // Update last active
    });

    console.log(`Password changed for admin: ${user.user_handle} (${user.display_name}) at ${new Date().toISOString()}`);

    res.json({ message: 'Password changed successfully' });

  } catch (error) {
    console.error('Change password error:', error);
    res.status(500).json({ 
      error: 'Server error during password change' 
    });
  }
});

module.exports = router;