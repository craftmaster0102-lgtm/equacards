require('dotenv').config();
const express = require('express');
const cors = require('cors');
const { createClient } = require('@supabase/supabase-js');

const app = express();

// Environment variables
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_KEY;
const PORT = process.env.PORT || 3000;
const CORS_ORIGIN = process.env.CORS_ORIGIN || 'https://equacards.netlify.app';

// Supabase client
const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);

// Middleware
app.use(cors({
  origin: CORS_ORIGIN,
  methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
  allowedHeaders: ['Content-Type', 'Authorization']
}));
app.use(express.json());

// Routes

// GET /
app.get('/', (req, res) => {
  res.json({
    status: 'EquaCards Backend Running'
  });
});

// GET /leaderboard
app.get('/leaderboard', async (req, res) => {
  try {
    const { data, error } = await supabase
      .from('scores')
      .select('*')
      .order('score', { ascending: false });

    if (error) throw error;

    res.json(data);
  } catch (err) {
    res.status(500).json({
      error: err.message
    });
  }
});

// GET /users
app.get('/users', async (req, res) => {
  try {
    const { data, error } = await supabase
      .from('users')
      .select('*');

    if (error) throw error;

    res.json(data);
  } catch (err) {
    res.status(500).json({
      error: err.message
    });
  }
});

// GET /matches
app.get('/matches', async (req, res) => {
  try {
    const { data, error } = await supabase
      .from('matches')
      .select('*');

    if (error) throw error;

    res.json(data);
  } catch (err) {
    res.status(500).json({
      error: err.message
    });
  }
});

// POST /scores
app.post('/scores', async (req, res) => {
  try {
    const { data, error } = await supabase
      .from('scores')
      .insert([req.body])
      .select();

    if (error) throw error;

    res.status(201).json(data);
  } catch (err) {
    res.status(500).json({
      error: err.message
    });
  }
});

// POST /users
app.post('/users', async (req, res) => {
  try {
    const { data, error } = await supabase
      .from('users')
      .insert([req.body])
      .select();

    if (error) throw error;

    res.status(201).json(data);
  } catch (err) {
    res.status(500).json({
      error: err.message
    });
  }
});

// POST /matches
app.post('/matches', async (req, res) => {
  try {
    const { data, error } = await supabase
      .from('matches')
      .insert([req.body])
      .select();

    if (error) throw error;

    res.status(201).json(data);
  } catch (err) {
    res.status(500).json({
      error: err.message
    });
  }
});
server.listen(PORT, () => {
    console.log(`EquaCards Backend Running on ${PORT}`);
});
io.on("connection", (socket) => {
    console.log("Player Connected:", socket.id);

    socket.on("disconnect", () => {
        console.log("Player Disconnected:", socket.id);
    });
});
const http = require("http");
const { Server } = require("socket.io");

const server = http.createServer(app);

const io = new Server(server, {
  cors: {
    origin: [
      "https://equacards.netlify.app",
      "http://localhost:5500",
      "http://127.0.0.1:5500"
    ],
    methods: ["GET", "POST"]
  }
});
// Start server
app.listen(PORT, () => {
  console.log(`EquaCards server running on port ${PORT}`);
});
