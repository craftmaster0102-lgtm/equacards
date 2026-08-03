require('dotenv').config();
const express = require('express');
const cors = require('cors');
const http = require('http');
const { Server } = require('socket.io');
const { createClient } = require('@supabase/supabase-js');

const app = express();
const server = http.createServer(app);
// Environment variables
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_PUBLISHABLE_KEY = process.env.SUPABASE_PUBLISHABLE_KEY;
const PORT = process.env.PORT || 3000;
const CORS_ORIGIN = process.env.CORS_ORIGIN || 'https://9f4aca6c.equacards.pages.dev';

// Supabase client
const supabase = createClient(SUPABASE_URL, SUPABASE_PUBLISHABLE_KEY);

// Middleware
app.use(cors({
  origin: [
    "https://9f4aca6c.equacards.pages.dev",
    "http://localhost:5500",
    "http://127.0.0.1:5500"
  ],
  credentials: true,
  methods: ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
  allowedHeaders: ["Content-Type", "Authorization"]
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
const io = new Server(server, {
  cors: {
    origin: [
      "https://9f4aca6c.equacards.pages.dev",
      "http://localhost:5500",
      "http://127.0.0.1:5500"
    ],
    methods: ["GET", "POST"],
    credentials: true
  }
});
const rooms = {};
io.on("connection", (socket) => {
socket.on("createRoom", ({ playerName }) => {

    let roomId = Math.random().toString(36).substring(2, 8).toUpperCase();

    while (rooms[roomId]) {
        roomId = Math.random().toString(36).substring(2, 8).toUpperCase();
    }

    rooms[roomId] = {
        players: [{
            id: socket.id,
            name: playerName
        }]
    };

    socket.join(roomId);

    socket.emit("roomCreated", {
        roomId
    });

    console.log("Room Created:", roomId);

});
socket.on("joinRoom", ({ roomId, playerName }) => {

    roomId = roomId.toUpperCase();

    if (!rooms[roomId]) {
        socket.emit("joinError", "Room not found");
        return;
    }

    if (rooms[roomId].players.length >= 2) {
        socket.emit("joinError", "Room full");
        return;
    }

    rooms[roomId].players.push({
        id: socket.id,
        name: playerName
    });

    socket.join(roomId);

    io.to(roomId).emit("playerJoined", {
        roomId,
        players: rooms[roomId].players
    });

    console.log(playerName, "joined", roomId);

});
socket.on("disconnect", () => {

    console.log("Disconnected:", socket.id);

    for (const roomId in rooms) {

        rooms[roomId].players =
            rooms[roomId].players.filter(p => p.id !== socket.id);

        if (rooms[roomId].players.length === 0) {

            delete rooms[roomId];

        } else {

            io.to(roomId).emit("playerLeft");

        }
    }

});
  console.log("Player Connected:", socket.id);

});

server.listen(PORT, () => {
  console.log(`EquaCards Backend Running on ${PORT}`);
});
