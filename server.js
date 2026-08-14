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
const SUPABASE_KEY = process.env.SUPABASE_KEY || process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_PUBLISHABLE_KEY;

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

const supabase = SUPABASE_URL && SUPABASE_KEY
  ? createClient(SUPABASE_URL, SUPABASE_KEY)
  : null;

if (!supabase) {
  console.warn('[CONFIG] Supabase is not configured. Set SUPABASE_KEY or SUPABASE_SERVICE_ROLE_KEY in the backend environment.');
}

async function updateMatchRecord(roomId, updates) {
  if (!supabase) return { ok: true, skipped: true };

  try {
    const { error } = await supabase
      .from('matches')
      .update(updates)
      .eq('room_id', roomId);

    if (error) {
      console.warn('[MATCH] Supabase update skipped:', error.message);
      return { ok: false, error };
    }

    return { ok: true, skipped: false };
  } catch (err) {
    console.warn('[MATCH] Supabase update failed:', err.message);
    return { ok: false, error: err };
  }
}

async function createMatchRecord(roomCode, playerName) {
  if (!supabase) return { ok: true, skipped: true };

  try {
    const { error } = await supabase
      .from('matches')
      .insert([{
        room_id: roomCode,
        host_name: playerName,
        opponent_name: null,
        status: 'waiting',
        created_at: new Date().toISOString()
      }]);

    if (error) {
      console.warn('[MATCH] Supabase insert skipped:', error.message);
      return { ok: false, error };
    }

    return { ok: true, skipped: false };
  } catch (err) {
    console.warn('[MATCH] Supabase insert failed:', err.message);
    return { ok: false, error: err };
  }
}

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

function evaluateExpressionValue(expression) {
  const sanitized = (expression || '').replace(/\s+/g, '');
  if (!sanitized) return null;

  const safeExpression = sanitized.replace(/[^0-9+\-*/().]/g, '');
  if (safeExpression !== sanitized) return null;

  try {
    const value = Function(`"use strict"; return (${safeExpression});`)();
    return Number.isFinite(value) ? Math.round(value) : null;
  } catch (err) {
    return null;
  }
}

function generateRoundState(level = 1) {
  const safeLevel = Math.max(1, Math.min(4, Number(level) || 1));
  const numCount = safeLevel === 1 ? 3 : (safeLevel < 4 ? 4 : 5);
  const maxNum = safeLevel === 1 ? 20 : (safeLevel === 2 ? 30 : (safeLevel === 3 ? 50 : 100));
  const ops = safeLevel === 1 ? ['+', '-'] : (safeLevel === 2 ? ['+', '-', '*'] : ['+', '-', '*', '/']);

  const numbers = Array.from({ length: numCount }, () => Math.floor(Math.random() * maxNum) + 1);
  let target = numbers.reduce((a, b) => a + b, 0);

  for (let attempt = 0; attempt < 50; attempt++) {
    const shuffled = [...numbers].sort(() => Math.random() - 0.5);
    let val = shuffled[0];
    for (let i = 0; i < numCount - 1; i++) {
      const op = ops[Math.floor(Math.random() * ops.length)];
      const next = shuffled[i + 1];
      if (op === '+') val += next;
      else if (op === '-') val -= next;
      else if (op === '*') val *= next;
      else if (op === '/') val = next !== 0 ? Math.floor(val / next) : val;
    }
    if (Math.abs(val) > 0 && Math.abs(val) <= 999) { target = Math.abs(val); break; }
  }

  const cards = [];
  numbers.forEach((n) => cards.push({ type: 'num', value: n }));
  ops.forEach((o) => cards.push({ type: 'op', value: o }));
  if (safeLevel >= 3) {
    cards.push({ type: 'paren', value: '(' });
    cards.push({ type: 'paren', value: ')' });
  }

  return {
    level: safeLevel,
    target,
    numbers: [...numbers],
    operators: [...ops],
    cards: cards.map((card) => ({ ...card })),
    timer: 30
  };
}

function startMatchIfReady(roomId) {
  const room = matchRooms[roomId];
  if (!room) return;

  if (!room.host || !room.opponent) return;
  if (!room.ready[room.host] || !room.ready[room.opponent]) return;
  if (room.status === 'playing' || room.status === 'finished') return;

  room.status = 'playing';
  room.roundState = generateRoundState(room.level || 1);
  room.startedAt = Date.now();

  io.to(roomId).emit('matchStart', {
    roomId,
    players: [
      { id: room.host, name: room.hostName },
      { id: room.opponent, name: room.opponentName }
    ],
    roundState: room.roundState,
    startedAt: room.startedAt
  });
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
        ready: {},
        level: 1,
        roundState: null,
        winnerId: null,
        loserId: null,
        createdAt: new Date().toISOString()
      };

      // Store match in Supabase only if configured; otherwise keep working in-memory.
      const matchRecordResult = await createMatchRecord(roomCode, playerName);
      if (matchRecordResult.ok === false && matchRecordResult.error && !supabase) {
        console.warn('[MATCH] Supabase unavailable; continuing in-memory room creation');
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
      room.ready = {};

      // Update Supabase only if configured; otherwise continue with in-memory room state.
      const updateResult = await updateMatchRecord(roomId, {
        opponent_name: playerName,
        status: 'ready'
      });

      if (updateResult.ok === false && updateResult.error && !supabase) {
        console.warn('[MATCH] Supabase unavailable; continuing in-memory room join');
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

    if (!room) return;

    room.ready[socket.id] = true;
    console.log(`[MATCH] Player ready in ${roomId}: ${socket.id}`);
    io.to(roomId).emit('playerReadyAck', { playerId: socket.id });
    startMatchIfReady(roomId);
  });

  socket.on('leaveMatch', (data) => {
    const roomId = data?.roomId || data;
    const room = matchRooms[roomId];

    if (!room) return;

    const remainingId = room.host === socket.id ? room.opponent : room.host;
    socket.leave(roomId);

    if (room.host === socket.id) {
      room.host = room.opponent || null;
      room.hostName = room.opponentName || null;
    }
    if (room.opponent === socket.id) {
      room.opponent = null;
      room.opponentName = null;
    }
    room.ready = {};
    room.status = room.host ? 'waiting' : 'closed';
    room.winnerId = null;
    room.loserId = null;

    if (remainingId) {
      io.to(remainingId).emit('playerLeft', {
        roomId,
        playerId: socket.id,
        message: 'Opponent left the room'
      });
    }

    if (!room.host) {
      delete matchRooms[roomId];
    }
  });

  socket.on('matchSubmission', (data) => {
    const { roomId, expression } = data || {};
    const room = matchRooms[roomId];

    if (!room || room.status !== 'playing') return;

    const value = evaluateExpressionValue(expression);
    const target = room.roundState?.target;
    if (value === null || target === null || typeof target === 'undefined') return;

    if (value === target) {
      room.status = 'finished';
      room.winnerId = socket.id;
      room.loserId = socket.id === room.host ? room.opponent : room.host;

      io.to(roomId).emit('matchResult', {
        roomId,
        winnerId: room.winnerId,
        loserId: room.loserId,
        winnerName: socket.id === room.host ? room.hostName : room.opponentName,
        loserName: socket.id === room.host ? room.opponentName : room.hostName,
        target,
        expression,
        status: 'finished'
      });
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

        const remainingId = room.host === socket.id ? room.opponent : room.host;
        if (remainingId) {
          io.to(roomId).emit('playerLeft', { playerId: socket.id, roomId, message: 'Opponent left the room' });
        }

        if (room.host === socket.id) {
          room.host = room.opponent || null;
          room.hostName = room.opponentName || null;
        }
        if (room.opponent === socket.id) {
          room.opponent = null;
          room.opponentName = null;
        }
        room.ready = {};
        room.status = room.host ? 'waiting' : 'closed';
        room.winnerId = null;
        room.loserId = null;

        if (!room.host) {
          delete matchRooms[roomId];
        }
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
    if (!supabase) {
      return res.status(503).json({
        success: false,
        error: 'Supabase is not configured. Set SUPABASE_KEY or SUPABASE_SERVICE_ROLE_KEY.'
      });
    }

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
    if (!supabase) {
      return res.status(503).json({
        success: false,
        error: 'Supabase is not configured. Set SUPABASE_KEY or SUPABASE_SERVICE_ROLE_KEY.'
      });
    }

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
    if (!supabase) {
      return res.status(503).json({
        success: false,
        error: 'Supabase is not configured. Set SUPABASE_KEY or SUPABASE_SERVICE_ROLE_KEY.'
      });
    }

    const { username, score } = req.body;

    if (!username) {
      return res.status(400).json({
        success: false,
        error: 'Username is required'
      });
    }

    const payload = {
      username,
      score: Number(score) || 0,
      created_at: new Date().toISOString()
    };

    let { data, error } = await supabase
      .from('scores')
      .upsert(payload, { onConflict: 'username' })
      .select();

    if (error && error.code === '42501') {
      console.warn('[SUPABASE] RLS blocked upsert. Trying insert fallback.');
      ({ data, error } = await supabase
        .from('scores')
        .insert([payload])
        .select());
    }

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
    if (!supabase) {
      return res.status(503).json({
        success: false,
        error: 'Supabase is not configured. Set SUPABASE_KEY or SUPABASE_SERVICE_ROLE_KEY.'
      });
    }

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
    if (!supabase) {
      return res.status(503).json({
        success: false,
        error: 'Supabase is not configured. Set SUPABASE_KEY or SUPABASE_SERVICE_ROLE_KEY.'
      });
    }

    const payload = {
      room_id: req.body.room_id || req.body.roomId || null,
      host_name: req.body.host_name || req.body.hostName || null,
      opponent_name: req.body.opponent_name || req.body.opponentName || null,
      winner_name: req.body.winner_name || req.body.winnerName || null,
      loser_name: req.body.loser_name || req.body.loserName || null,
      status: req.body.status || 'finished',
      created_at: req.body.created_at || req.body.createdAt || new Date().toISOString()
    };

    if (!payload.room_id) {
      return res.status(400).json({
        success: false,
        error: 'room_id is required'
      });
    }

    const { data, error } = await supabase
      .from('matches')
      .insert([payload])
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
