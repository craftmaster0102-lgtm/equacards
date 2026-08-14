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
  process.env.CORS_ORIGIN ||
  'https://equacard.netlify.app,https://www.equacard.netlify.app,https://equacards.netlify.app,https://www.equacards.netlify.app,https://equacards.pages.dev,http://localhost:3000,http://localhost:5173,http://localhost:8080,http://127.0.0.1:3000,http://127.0.0.1:5173,http://127.0.0.1:8080';

const ALLOWED_ORIGINS = CORS_ORIGIN
  .split(',')
  .map((origin) => origin.trim())
  .filter(Boolean);

function isAllowedOrigin(origin) {
  if (!origin) return true;

  const hostname = new URL(origin).hostname;
  const allowed = ALLOWED_ORIGINS.includes(origin);
  const netlifyHost = hostname.endsWith('.netlify.app') || hostname.endsWith('.pages.dev');
  const localHost = ['localhost', '127.0.0.1'].includes(hostname);

  return allowed || netlifyHost || localHost;
}

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

const corsOptions = {
  origin: function(origin, callback) {
    if (isAllowedOrigin(origin)) {
      callback(null, true);
    } else {
      callback(new Error('Not allowed by CORS'));
    }
  },
  methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
  allowedHeaders: ['Content-Type', 'Authorization'],
  credentials: true
};

app.use(cors(corsOptions));
app.options(/.*/, cors(corsOptions));
app.use(express.json());

// ==========================================
// SOCKET.IO
// ==========================================

const io = new Server(server, {
  cors: {
    origin: function(origin, callback) {
      if (isAllowedOrigin(origin)) {
        callback(null, true);
      } else {
        callback(new Error('Not allowed by CORS'));
      }
    },
    methods: ['GET', 'POST'],
    credentials: true
  },
  transports: ['websocket', 'polling'],
  pingInterval: 25000,
  pingTimeout: 60000
});

// ==========================================
// MATCH ROOM MANAGER
// ==========================================

const matchRooms = {}; // { roomId: { host: socketId, hostName, opponent: socketId, opponentName, createdAt } }

function generateRoomCode() {
  return Math.random().toString(36).substring(2, 8).toUpperCase();
}

// ==========================================
// SOCKET CONNECTION
// ==========================================

io.on('connection', (socket) => {
  console.log('====================================');
  console.log('Socket.IO client connected');
  console.log('Socket ID:', socket.id);
  console.log('====================================');

  // ==========================================
  // CREATE ROOM
  // ==========================================
  socket.on('createRoom', async (data) => {
    try {
      const { playerName } = data;
      let roomCode = generateRoomCode();

      // Ensure unique room code
      while (matchRooms[roomCode]) {
        roomCode = generateRoomCode();
      }

      // Store room in memory
      matchRooms[roomCode] = {
        roomId: roomCode,
        host: socket.id,
        hostName: playerName,
        opponent: null,
        opponentName: null,
        status: 'waiting',
        createdAt: new Date().toISOString()
      };

      // Store match in Supabase
      const { data: matchData, error: matchError } = await supabase
        .from('matches')
        .insert([{
          room_id: roomCode,
          host_name: playerName,
          opponent_name: null,
          status: 'waiting',
          created_at: new Date().toISOString()
        }])
        .select();

      if (matchError) {
        console.error('Supabase insert error:', matchError);
        socket.emit('createRoomError', 'Failed to create room');
        return;
      }

      // Join the socket to a room
      socket.join(roomCode);

      console.log(`[MATCH] Room created: ${roomCode} by ${playerName} (${socket.id})`);

      // Send room code to host
      socket.emit('roomCreated', {
        roomId: roomCode,
        playerName: playerName,
        status: 'waiting'
      });

    } catch (err) {
      console.error('CREATE ROOM error:', err);
      socket.emit('createRoomError', err.message);
    }
  });

  // ==========================================
  // JOIN ROOM
  // ==========================================
  socket.on('joinRoom', async (data) => {
    try {
      const { roomId, playerName } = data;

      // Check if room exists
      if (!matchRooms[roomId]) {
        console.log(`[MATCH] Failed: Room ${roomId} does not exist`);
        socket.emit('joinError', 'Invalid room code');
        return;
      }

      const room = matchRooms[roomId];

      // Check if room is full
      if (room.opponent) {
        console.log(`[MATCH] Failed: Room ${roomId} is full`);
        socket.emit('joinError', 'Room is full');
        return;
      }

      // Add opponent to room
      room.opponent = socket.id;
      room.opponentName = playerName;
      room.status = 'ready';

      // Update Supabase
      const { error: updateError } = await supabase
        .from('matches')
        .update({
          opponent_name: playerName,
          status: 'ready'
        })
        .eq('room_id', roomId);

      if (updateError) {
        console.error('Supabase update error:', updateError);
      }

      // Join the socket to the room
      socket.join(roomId);

      console.log(`[MATCH] Player joined: ${roomId} | Player: ${playerName} (${socket.id})`);

      // Notify opponent that someone joined
      io.to(roomId).emit('opponentJoined', {
        roomId: roomId,
        players: [
          { id: room.host, name: room.hostName },
          { id: room.opponent, name: room.opponentName }
        ]
      });

      // Send join confirmation to joining player
      socket.emit('roomJoined', {
        roomId: roomId,
        players: [
          { id: room.host, name: room.hostName },
          { id: room.opponent, name: room.opponentName }
        ]
      });

    } catch (err) {
      console.error('JOIN ROOM error:', err);
      socket.emit('joinError', err.message);
    }
  });

  // ==========================================
  // GAME START (READY)
  // ==========================================
  socket.on('playerReady', (data) => {
    const { roomId } = data;
    const room = matchRooms[roomId];

    if (room) {
      console.log(`[MATCH] Player ready in ${roomId}: ${socket.id}`);
      io.to(roomId).emit('playerReadyAck', { playerId: socket.id });
    }
  });

  // ==========================================
  // GAME STATE SYNC (for multiplayer gameplay)
  // ==========================================
  socket.on('gameStateUpdate', (data) => {
    const { roomId, gameState } = data;
    const room = matchRooms[roomId];

    if (room) {
      // Broadcast game state to opponent
      socket.to(roomId).emit('opponentGameState', {
        playerId: socket.id,
        gameState: gameState
      });
    }
  });

  // ==========================================
  // DISCONNECT
  // ==========================================
  socket.on('disconnect', (reason) => {
    console.log(
      `Socket disconnected: ${socket.id} | Reason: ${reason}`
    );

    // Find and clean up match rooms
    for (const roomId in matchRooms) {
      const room = matchRooms[roomId];
      if (room.host === socket.id || room.opponent === socket.id) {
        console.log(`[MATCH] Cleaning up room ${roomId} due to disconnect`);

        // Notify remaining player
        io.to(roomId).emit('playerLeft', { playerId: socket.id });

        // Delete room after a delay
        setTimeout(() => {
          delete matchRooms[roomId];
        }, 5000);
      }
    }
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

app.get(['/leaderboard', '/api/leaderboard'], async (req, res) => {
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

app.get(['/users', '/api/users'], async (req, res) => {
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

app.get(['/matches', '/api/matches'], async (req, res) => {
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

app.post(['/scores', '/api/scores'], async (req, res) => {
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

app.post(['/users', '/api/users'], async (req, res) => {
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

app.post(['/matches', '/api/matches'], async (req, res) => {
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
