require('dotenv').config();

const express = require('express');
const cors = require('cors');
const http = require('http');
const { Server } = require('socket.io');
const { createClient } = require('@supabase/supabase-js');

const app = express();
const server = http.createServer(app);

// ==========================================
// ENVIRONMENT VARIABLES
// ==========================================

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_KEY;

const PORT = process.env.PORT || 10000;

const CORS_ORIGIN =
  process.env.CORS_ORIGIN || 'https://equacards.netlify.app';

// ==========================================
// SUPABASE
// ==========================================

const supabase = createClient(
  SUPABASE_URL,
  SUPABASE_KEY
);

// ==========================================
// CORS
// ==========================================

app.use(
  cors({
    origin: [
      'https://equacards.netlify.app',
      'https://equacards.pages.dev'
    ],
    methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
    allowedHeaders: ['Content-Type', 'Authorization'],
    credentials: true
  })
);

app.use(express.json());

// ==========================================
// SOCKET.IO
// ==========================================

const io = new Server(server, {
  cors: {
    origin: [
      'https://equacards.netlify.app',
      'https://equacards.pages.dev'
    ],
    methods: ['GET', 'POST'],
    credentials: true
  },
  transports: ['websocket', 'polling']
});

// ==========================================
// SOCKET CONNECTION
// ==========================================

io.on('connection', (socket) => {
  console.log('====================================');
  console.log('Socket.IO client connected');
  console.log('Socket ID:', socket.id);
  console.log('====================================');

  socket.on('disconnect', (reason) => {
    console.log(
      `Socket disconnected: ${socket.id} | Reason: ${reason}`
    );
  });

  socket.on('error', (error) => {
    console.error('Socket error:', error);
  });
});

// ==========================================
// GET /
// ==========================================

app.get('/', (req, res) => {
  res.json({
    status: 'EquaCards Backend Running',
    socketio: 'enabled',
    timestamp: new Date().toISOString()
  });
});

// ==========================================
// GET /leaderboard
// ==========================================

app.get('/leaderboard', async (req, res) => {
  try {
    const { data, error } = await supabase
      .from('scores')
      .select('*')
      .order('score', { ascending: false });

    if (error) {
      throw error;
    }

    res.json(data);
  } catch (err) {
    console.error('GET /leaderboard error:', err);

    res.status(500).json({
      success: false,
      error: err.message
    });
  }
});

// ==========================================
// GET /users
// ==========================================

app.get('/users', async (req, res) => {
  try {
    const { data, error } = await supabase
      .from('users')
      .select('*');

    if (error) {
      throw error;
    }

    res.json(data);
  } catch (err) {
    console.error('GET /users error:', err);

    res.status(500).json({
      success: false,
      error: err.message
    });
  }
});

// ==========================================
// GET /matches
// ==========================================

app.get('/matches', async (req, res) => {
  try {
    const { data, error } = await supabase
      .from('matches')
      .select('*');

    if (error) {
      throw error;
    }

    res.json(data);
  } catch (err) {
    console.error('GET /matches error:', err);

    res.status(500).json({
      success: false,
      error: err.message
    });
  }
});

// ==========================================
// POST /scores
// ==========================================

app.post('/scores', async (req, res) => {
  try {
    const { username, score } = req.body;

    if (!username) {
      return res.status(400).json({
        success: false,
        error: 'Username is required'
      });
    }

    const { data, error } = await supabase
      .from('scores')
      .upsert(
        {
          username: username,
          score: score
        },
        {
          onConflict: 'username'
        }
      )
      .select();

    if (error) {
      throw error;
    }

    res.status(200).json({
      success: true,
      data
    });

  } catch (err) {
    console.error('POST /scores error:', err);

    res.status(500).json({
      success: false,
      error: err.message
    });
  }
});

// ==========================================
// POST /users
// ==========================================

app.post('/users', async (req, res) => {
  try {
    const { data, error } = await supabase
      .from('users')
      .insert([req.body])
      .select();

    if (error) {
      throw error;
    }

    res.status(201).json(data);
  } catch (err) {
    console.error('POST /users error:', err);

    res.status(500).json({
      success: false,
      error: err.message
    });
  }
});

// ==========================================
// POST /matches
// ==========================================

app.post('/matches', async (req, res) => {
  try {
    const { data, error } = await supabase
      .from('matches')
      .insert([req.body])
      .select();

    if (error) {
      throw error;
    }

    res.status(201).json(data);
  } catch (err) {
    console.error('POST /matches error:', err);

    res.status(500).json({
      success: false,
      error: err.message
    });
  }
});

// ==========================================
// START SERVER
// ==========================================

server.listen(PORT, '0.0.0.0', () => {
  console.log('====================================');
  console.log('EquaCards Backend Started');
  console.log(`HTTP Port: ${PORT}`);
  console.log('Socket.IO: ENABLED');
  console.log('====================================');
});
