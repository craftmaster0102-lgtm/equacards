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

const matchRooms = {}; // { roomId: { host, hostName, opponent, opponentName, status, currentRound, ... } }
const MATCH_RECONNECT_GRACE_MS = 15000;
const ROUND_TRANSITION_DELAY_MS = 2500;

function generateRoomCode() {
  return Math.random().toString(36).substring(2, 8).toUpperCase();
}

function getRoomPlayerSlot(room, socketId) {
  if (!room) return null;
  if (room.host === socketId) return 'host';
  if (room.opponent === socketId) return 'opponent';
  return null;
}

function normalizeMatchStats() {
  return {
    round1Score: 0,
    round2Score: 0,
    totalScore: 0,
    roundsWon: 0,
    roundsLost: 0,
    matchResult: null,
    connected: true
  };
}

function ensureRoomStats(room) {
  room.matchStats = room.matchStats || {
    host: normalizeMatchStats(),
    opponent: normalizeMatchStats()
  };
  room.matchStats.host = room.matchStats.host || normalizeMatchStats();
  room.matchStats.opponent = room.matchStats.opponent || normalizeMatchStats();
  room.matchStats.host.connected = room.host ? true : false;
  room.matchStats.opponent.connected = room.opponent ? true : false;
}

function buildRoomPlayers(room) {
  ensureRoomStats(room);
  return [
    {
      id: room.host,
      name: room.hostName || 'Player 1',
      score: Number(room.matchStats.host.totalScore || 0),
      round1Score: Number(room.matchStats.host.round1Score || 0),
      round2Score: Number(room.matchStats.host.round2Score || 0),
      roundsWon: Number(room.matchStats.host.roundsWon || 0),
      roundsLost: Number(room.matchStats.host.roundsLost || 0),
      connected: !!room.host,
      slot: 'host'
    },
    {
      id: room.opponent,
      name: room.opponentName || 'Player 2',
      score: Number(room.matchStats.opponent.totalScore || 0),
      round1Score: Number(room.matchStats.opponent.round1Score || 0),
      round2Score: Number(room.matchStats.opponent.round2Score || 0),
      roundsWon: Number(room.matchStats.opponent.roundsWon || 0),
      roundsLost: Number(room.matchStats.opponent.roundsLost || 0),
      connected: !!room.opponent,
      slot: 'opponent'
    }
  ];
}

function buildMatchState(room) {
  ensureRoomStats(room);
  const players = buildRoomPlayers(room);
  return {
    roomId: room.roomId,
    matchId: room.matchId || room.roomId,
    status: room.status,
    currentRound: Number(room.currentRound || 1),
    maxRounds: 2,
    players,
    matchStats: room.matchStats,
    roundState: room.roundState ? { ...room.roundState, timer: room.roundState.timer || 30 } : null
  };
}

function emitMatchState(roomId) {
  const room = matchRooms[roomId];
  if (!room) return;
  const payload = buildMatchState(room);
  io.to(roomId).emit('matchState', payload);
}

function clearRoundTimer(room) {
  if (!room) return;
  if (room.roundTimeout) {
    clearTimeout(room.roundTimeout);
    room.roundTimeout = null;
  }
}

function emitRoundTransition(roomId, roundNumber, roundSummary) {
  const room = matchRooms[roomId];
  if (!room) return;
  room.status = `round${roundNumber}`;
  room.roundState = generateRoundState(room.level || 1);
  room.roundResults = room.roundResults || {};
  room.roundResults[roundNumber] = room.roundResults[roundNumber] || { host: null, opponent: null };
  room.roundTimeout = setTimeout(() => {
    if (!matchRooms[roomId]) return;
    const current = matchRooms[roomId];
    if (!current.roundResults?.[roundNumber]?.host || !current.roundResults?.[roundNumber]?.opponent) {
      current.roundResults = current.roundResults || {};
      current.roundResults[roundNumber] = current.roundResults[roundNumber] || { host: null, opponent: null };
      if (!current.roundResults[roundNumber].host) {
        current.roundResults[roundNumber].host = { correct: false, expression: null, timedOut: true, submittedAt: Date.now() };
      }
      if (!current.roundResults[roundNumber].opponent) {
        current.roundResults[roundNumber].opponent = { correct: false, expression: null, timedOut: true, submittedAt: Date.now() };
      }
    }
    finalizeRound(roomId);
  }, 30000);

  io.to(roomId).emit('roundTransition', {
    roomId,
    matchId: room.matchId || roomId,
    currentRound: roundNumber,
    nextRound: roundNumber + 1,
    roundSummary,
    roundState: room.roundState,
    players: buildRoomPlayers(room),
    matchStats: room.matchStats
  });
  emitMatchState(roomId);
}

function finalizeRound(roomId) {
  const room = matchRooms[roomId];
  if (!room || !room.roundState) return;
  clearRoundTimer(room);

  const roundNumber = Number(room.currentRound || 1);
  const roundSummary = room.roundResults?.[roundNumber] || { host: null, opponent: null };
  const hostResult = roundSummary.host || { correct: false, expression: null };
  const oppResult = roundSummary.opponent || { correct: false, expression: null };

  const hostScore = !!hostResult.correct ? 100 : 0;
  const oppScore = !!oppResult.correct ? 100 : 0;

  const roundKey = roundNumber === 1 ? 'round1Score' : 'round2Score';
  room.matchStats = room.matchStats || { host: normalizeMatchStats(), opponent: normalizeMatchStats() };
  room.matchStats.host[roundKey] = hostScore;
  room.matchStats.opponent[roundKey] = oppScore;
  room.matchStats.host.totalScore = (room.matchStats.host.round1Score || 0) + (room.matchStats.host.round2Score || 0);
  room.matchStats.opponent.totalScore = (room.matchStats.opponent.round1Score || 0) + (room.matchStats.opponent.round2Score || 0);

  if (hostScore > oppScore) {
    room.matchStats.host.roundsWon = (room.matchStats.host.roundsWon || 0) + 1;
    room.matchStats.opponent.roundsLost = (room.matchStats.opponent.roundsLost || 0) + 1;
  } else if (oppScore > hostScore) {
    room.matchStats.opponent.roundsWon = (room.matchStats.opponent.roundsWon || 0) + 1;
    room.matchStats.host.roundsLost = (room.matchStats.host.roundsLost || 0) + 1;
  }

  const roundWinner = hostScore === oppScore ? 'draw' : (hostScore > oppScore ? 'host' : 'opponent');
  room.lastRoundResult = { round: roundNumber, winner: roundWinner, hostScore, oppScore, hostResult, oppResult };

  if (roundNumber === 1) {
    room.currentRound = 2;
    room.status = 'round2';
    room.roundState = generateRoundState(room.level || 1);
    room.roundResults = room.roundResults || {};
    room.roundResults[2] = { host: null, opponent: null };
    room.roundStartedAt = Date.now();
    room.roundTimeout = setTimeout(() => {
      const current = matchRooms[roomId];
      if (!current) return;
      if (!current.roundResults?.[2]?.host) {
        current.roundResults[2].host = { correct: false, expression: null, timedOut: true, submittedAt: Date.now() };
      }
      if (!current.roundResults?.[2]?.opponent) {
        current.roundResults[2].opponent = { correct: false, expression: null, timedOut: true, submittedAt: Date.now() };
      }
      finalizeRound(roomId);
    }, 30000);

    io.to(roomId).emit('roundTransition', {
      roomId,
      matchId: room.matchId || roomId,
      currentRound: 2,
      nextRound: null,
      roundSummary: room.lastRoundResult,
      roundState: room.roundState,
      players: buildRoomPlayers(room),
      matchStats: room.matchStats,
      transitionMessage: 'Round 1 complete. Round 2 starting...'
    });
    emitMatchState(roomId);
    return;
  }

  room.status = 'finished';
  room.matchResult = {
    status: 'completed',
    winnerId: room.matchStats.host.totalScore === room.matchStats.opponent.totalScore
      ? null
      : (room.matchStats.host.totalScore > room.matchStats.opponent.totalScore ? room.host : room.opponent),
    winnerName: room.matchStats.host.totalScore === room.matchStats.opponent.totalScore
      ? 'Draw'
      : (room.matchStats.host.totalScore > room.matchStats.opponent.totalScore ? room.hostName : room.opponentName),
    host: {
      name: room.hostName,
      round1: room.matchStats.host.round1Score,
      round2: room.matchStats.host.round2Score,
      totalScore: room.matchStats.host.totalScore,
      roundsWon: room.matchStats.host.roundsWon,
      roundsLost: room.matchStats.host.roundsLost
    },
    opponent: {
      name: room.opponentName,
      round1: room.matchStats.opponent.round1Score,
      round2: room.matchStats.opponent.round2Score,
      totalScore: room.matchStats.opponent.totalScore,
      roundsWon: room.matchStats.opponent.roundsWon,
      roundsLost: room.matchStats.opponent.roundsLost
    },
    matchId: room.matchId || room.roomId
  };

  io.to(roomId).emit('matchResult', {
    roomId,
    matchId: room.matchId || room.roomId,
    winnerId: room.matchResult.winnerId,
    winnerName: room.matchResult.winnerName,
    loserName: room.matchResult.winnerId === room.host ? room.opponentName : room.hostName,
    status: 'completed',
    roundSummary: room.lastRoundResult,
    matchStats: room.matchStats,
    result: room.matchResult
  });
  emitMatchState(roomId);
}

function startRound(roomId, roundNumber) {
  const room = matchRooms[roomId];
  if (!room) return;
  room.currentRound = roundNumber;
  room.status = `round${roundNumber}`;
  room.roundState = generateRoundState(room.level || 1);
  room.roundResults = room.roundResults || {};
  room.roundResults[roundNumber] = room.roundResults[roundNumber] || { host: null, opponent: null };
  room.roundStartedAt = Date.now();

  clearRoundTimer(room);
  room.roundTimeout = setTimeout(() => {
    const current = matchRooms[roomId];
    if (!current) return;
    if (!current.roundResults?.[roundNumber]?.host) {
      current.roundResults[roundNumber].host = { correct: false, expression: null, timedOut: true, submittedAt: Date.now() };
    }
    if (!current.roundResults?.[roundNumber]?.opponent) {
      current.roundResults[roundNumber].opponent = { correct: false, expression: null, timedOut: true, submittedAt: Date.now() };
    }
    finalizeRound(roomId);
  }, 30000);

  io.to(roomId).emit('roundStart', {
    roomId,
    matchId: room.matchId || roomId,
    currentRound: roundNumber,
    roundState: room.roundState,
    players: buildRoomPlayers(room),
    matchStats: room.matchStats,
    status: room.status
  });
  emitMatchState(roomId);
}

function startMatchIfReady(roomId) {
  const room = matchRooms[roomId];
  if (!room) return;

  if (!room.host || !room.opponent) return;
  if (!room.ready[room.host] || !room.ready[room.opponent]) return;
  if (room.status === 'playing' || room.status === 'finished' || room.status === 'round1' || room.status === 'round2') return;

  room.status = 'countdown';
  room.matchId = room.matchId || `${room.roomId}_${Date.now()}`;
  room.currentRound = 1;
  room.matchStats = {
    host: { ...normalizeMatchStats(), connected: !!room.host },
    opponent: { ...normalizeMatchStats(), connected: !!room.opponent }
  };
  room.roundResults = { 1: { host: null, opponent: null }, 2: { host: null, opponent: null } };
  room.roundStartedAt = Date.now();

  io.to(roomId).emit('matchStart', {
    roomId,
    matchId: room.matchId,
    players: buildRoomPlayers(room),
    currentRound: 1,
    roundState: generateRoundState(room.level || 1),
    startedAt: room.roundStartedAt,
    matchStats: room.matchStats
  });

  setTimeout(() => {
    const current = matchRooms[roomId];
    if (current) {
      startRound(roomId, 1);
    }
  }, 2500);
}

function scheduleReconnectCheck(roomId, slot) {
  const room = matchRooms[roomId];
  if (!room) return;
  room.reconnectTimers = room.reconnectTimers || {};
  if (room.reconnectTimers[slot]) clearTimeout(room.reconnectTimers[slot]);

  room.reconnectTimers[slot] = setTimeout(() => {
    const current = matchRooms[roomId];
    if (!current) return;
    const slotId = slot === 'host' ? current.host : current.opponent;
    if (!slotId && current.status !== 'finished') {
      current.status = 'abandoned';
      current.matchResult = { status: 'abandoned', winnerName: slot === 'host' ? current.opponentName : current.hostName };
      io.to(roomId).emit('matchAbandoned', {
        roomId,
        message: 'Opponent left the match.',
        status: 'abandoned',
        winnerName: current.matchResult.winnerName
      });
    }
  }, MATCH_RECONNECT_GRACE_MS);
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

  // Fallback: if no valid target found after all attempts, generate a simple one
  if (target > 999 || target <= 0) {
    target = numbers[0] + numbers[1]; // Simple sum of first two numbers
    target = Math.max(1, Math.min(999, target)); // Ensure within valid range
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
      const { playerName } = data || {};
      let roomCode = generateRoomCode();

      while (matchRooms[roomCode]) {
        roomCode = generateRoomCode();
      }

      matchRooms[roomCode] = {
        roomId: roomCode,
        host: socket.id,
        hostName: playerName || 'Host',
        opponent: null,
        opponentName: null,
        status: 'waiting',
        ready: {},
        level: 1,
        roundState: null,
        currentRound: 1,
        matchStats: null,
        roundResults: { 1: { host: null, opponent: null }, 2: { host: null, opponent: null } },
        winnerId: null,
        loserId: null,
        createdAt: new Date().toISOString(),
        reconnectTimers: {},
        roundTimeout: null,
        matchId: `${roomCode}_${Date.now()}`
      };

      const matchRecordResult = await createMatchRecord(roomCode, playerName || 'Host');
      if (matchRecordResult.ok === false && matchRecordResult.error && !supabase) {
        console.warn('[MATCH] Supabase unavailable; continuing in-memory room creation');
      }

      socket.join(roomCode);
      console.log(`[MATCH] Room created: ${roomCode} by ${playerName || 'Host'} (${socket.id})`);

      socket.emit('roomCreated', {
        roomId: roomCode,
        playerName: playerName || 'Host',
        status: 'waiting',
        matchId: matchRooms[roomCode].matchId
      });

    } catch (err) {
      console.error('CREATE ROOM error:', err);
      socket.emit('createRoomError', err.message);
    }
  });

  socket.on('joinRoom', async (data) => {
    try {
      const { roomId, playerName } = data || {};

      if (!matchRooms[roomId]) {
        console.log(`[MATCH] Failed: Room ${roomId} does not exist`);
        socket.emit('joinError', 'Invalid room code');
        return;
      }

      const room = matchRooms[roomId];

      if (room.opponent) {
        console.log(`[MATCH] Failed: Room ${roomId} is full`);
        socket.emit('joinError', 'Room is full');
        return;
      }

      room.opponent = socket.id;
      room.opponentName = playerName || 'Guest';
      room.status = 'ready';
      room.ready = {};
      room.matchId = room.matchId || `${room.roomId}_${Date.now()}`;
      room.matchStats = {
        host: { ...normalizeMatchStats(), connected: true },
        opponent: { ...normalizeMatchStats(), connected: true }
      };

      const updateResult = await updateMatchRecord(roomId, {
        opponent_name: room.opponentName,
        status: 'ready'
      });

      if (updateResult.ok === false && updateResult.error && !supabase) {
        console.warn('[MATCH] Supabase unavailable; continuing in-memory room join');
      }

      socket.join(roomId);
      console.log(`[MATCH] Player joined: ${roomId} | Player: ${room.opponentName} (${socket.id})`);

      io.to(roomId).emit('opponentJoined', {
        roomId: roomId,
        matchId: room.matchId,
        players: [
          { id: room.host, name: room.hostName },
          { id: room.opponent, name: room.opponentName }
        ]
      });

      socket.emit('roomJoined', {
        roomId: roomId,
        matchId: room.matchId,
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

  socket.on('playerReady', (data) => {
    const { roomId } = data || {};
    const room = matchRooms[roomId];

    if (!room) return;

    room.ready[socket.id] = true;
    console.log(`[MATCH] Player ready in ${roomId}: ${socket.id}`);
    io.to(roomId).emit('playerReadyAck', { playerId: socket.id, roomId });
    startMatchIfReady(roomId);
  });

  socket.on('togglePause', (data) => {
    const roomId = data?.roomId;
    const room = matchRooms[roomId];
    if (!room) return;

    const isPaused = !!data?.isPaused;
    room.pauseState = room.pauseState || { isPaused: false, by: null };
    room.pauseState.isPaused = isPaused;
    room.pauseState.by = isPaused ? socket.id : null;

    io.to(roomId).emit('matchPauseState', {
      roomId,
      isPaused,
      pausedBy: socket.id,
      matchStats: room.matchStats,
      players: buildRoomPlayers(room)
    });
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
    room.status = room.host ? 'abandoned' : 'cancelled';
    room.winnerId = null;
    room.loserId = null;
    room.roundState = null;
    clearRoundTimer(room);

    if (remainingId) {
      io.to(remainingId).emit('playerLeft', {
        roomId,
        playerId: socket.id,
        message: 'Opponent left the match.',
        status: 'abandoned'
      });
    }

    if (!room.host) {
      delete matchRooms[roomId];
    }
  });

  socket.on('rejoinMatch', (data) => {
    const { roomId, playerName } = data || {};
    const room = matchRooms[roomId];
    if (!room) return;

    const slot = room.hostName === playerName ? 'host' : (room.opponentName === playerName ? 'opponent' : null);
    if (!slot) return;

    room[slot] = socket.id;
    room.ready[socket.id] = true;
    room.playerReconnectGrace = null;
    if (room.reconnectTimers) {
      if (room.reconnectTimers[slot]) {
        clearTimeout(room.reconnectTimers[slot]);
        room.reconnectTimers[slot] = null;
      }
    }

    io.to(roomId).emit('matchState', buildMatchState(room));
    socket.emit('rejoinedMatch', { roomId, matchId: room.matchId, currentRound: room.currentRound || 1, roundState: room.roundState, matchStats: room.matchStats });
  });

  socket.on('matchSubmission', (data) => {
    const { roomId, expression } = data || {};
    const room = matchRooms[roomId];

    if (!room || !['round1', 'round2'].includes(room.status)) return;

    const slot = getRoomPlayerSlot(room, socket.id);
    if (!slot) return;

    const roundNumber = Number(room.currentRound || 1);
    room.roundResults = room.roundResults || { 1: { host: null, opponent: null }, 2: { host: null, opponent: null } };
    room.roundResults[roundNumber] = room.roundResults[roundNumber] || { host: null, opponent: null };

    if (room.roundResults[roundNumber][slot]) {
      return;
    }

    const value = evaluateExpressionValue(expression);
    const target = room.roundState?.target;
    if (value === null || target === null || typeof target === 'undefined') {
      room.roundResults[roundNumber][slot] = { correct: false, expression, submittedAt: Date.now(), invalid: true };
      if (room.roundResults[roundNumber].host && room.roundResults[roundNumber].opponent) {
        finalizeRound(roomId);
      }
      return;
    }

    const correct = value === target;
    room.roundResults[roundNumber][slot] = { correct, expression, submittedAt: Date.now() };

    const playerName = slot === 'host' ? room.hostName : room.opponentName;
    const message = correct ? `${playerName} solved the target!` : `${playerName} submitted an answer.`;

    const bothSubmitted = !!(room.roundResults[roundNumber].host && room.roundResults[roundNumber].opponent);
    const shouldFinalizeNow = correct || bothSubmitted;

    io.to(roomId).emit('roundUpdate', {
      roomId,
      currentRound: roundNumber,
      playerId: socket.id,
      playerName,
      correct,
      message,
      matchStats: room.matchStats,
      roundState: room.roundState,
      players: buildRoomPlayers(room)
    });

    if (shouldFinalizeNow) {
      finalizeRound(roomId);
    }
  });

  socket.on('gameStateUpdate', (data) => {
    const { roomId, gameState } = data || {};
    const room = matchRooms[roomId];

    if (room) {
      socket.to(roomId).emit('opponentGameState', {
        playerId: socket.id,
        gameState: gameState,
        roomId,
        currentRound: room.currentRound,
        matchStats: room.matchStats
      });
    }
  });

  socket.on('disconnect', (reason) => {
    console.log(`Socket disconnected: ${socket.id} | Reason: ${reason}`);

    for (const roomId in matchRooms) {
      const room = matchRooms[roomId];
      if (room.host === socket.id || room.opponent === socket.id) {
        console.log(`[MATCH] Grace period started for ${roomId} due to disconnect`);

        const slot = room.host === socket.id ? 'host' : 'opponent';
        const remainingId = room.host === socket.id ? room.opponent : room.host;
        if (remainingId) {
          io.to(remainingId).emit('opponentDisconnected', {
            roomId,
            playerId: socket.id,
            message: 'Opponent disconnected. Reconnecting...',
            status: 'reconnect_pending'
          });
        }

        if (room.matchStats) {
          room.matchStats[slot].connected = false;
        }

        scheduleReconnectCheck(roomId, slot);
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
      console.error('[SCORES] Supabase not configured');
      return res.status(200).json({
        success: false,
        queuedLocally: true,
        error: 'Supabase is not configured. Set SUPABASE_KEY or SUPABASE_SERVICE_ROLE_KEY.'
      });
    }

    const { username, score } = req.body;

    if (!username) {
      console.warn('[SCORES] Missing username in payload:', req.body);
      return res.status(200).json({
        success: false,
        queuedLocally: true,
        error: 'Username is required'
      });
    }

    const matchId = req.body.match_id || req.body.matchId || null;
    const payload = {
      username,
      score: Number(score) || 0,
      round: req.body.round || 1,
      combo: req.body.combo || 0,
      correct: req.body.correct || 0,
      wrong: req.body.wrong || 0,
      accuracy: req.body.accuracy || 0,
      level: req.body.level || 1,
      email: req.body.email || '',
      created_at: new Date().toISOString()
    };

    if (matchId) {
      payload.match_id = matchId;
    }

    console.log('[SCORES] Inserting score:', payload);

    let { data, error } = await supabase
      .from('scores')
      .upsert([
        payload
      ], { onConflict: 'match_id', ignoreDuplicates: false })
      .select();

    if (error) {
      console.warn('[SCORES] match_id upsert unavailable or failed; retrying without match_id', error.message);
      const fallback = { ...payload };
      delete fallback.match_id;
      const fallBackResult = await supabase
        .from('scores')
        .insert([fallback])
        .select();

      data = fallBackResult.data;
      error = fallBackResult.error;
    }

    if (error) {
      console.error('[SCORES] Supabase error:', error.code, error.message);
      return res.status(200).json({
        success: false,
        queuedLocally: true,
        error: error.message,
        code: error.code
      });
    }

    console.log('[SCORES] Insert successful:', data);
    res.status(200).json({
      success: true,
      data
    });

  } catch (err) {
    console.error('[SCORES] Exception:', err.message, err.stack);

    res.status(200).json({
      success: false,
      queuedLocally: true,
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
      return res.status(200).json({
        success: false,
        queuedLocally: true,
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
      return res.status(200).json({
        success: false,
        queuedLocally: true,
        error: 'room_id is required'
      });
    }

    const { data, error } = await supabase
      .from('matches')
      .insert([payload])
      .select();

    if (error) {
      return res.status(200).json({
        success: false,
        queuedLocally: true,
        error: error.message
      });
    }

    res.status(200).json({
      success: true,
      data
    });
  } catch (err) {
    console.error('POST /matches error:', err);

    res.status(200).json({
      success: false,
      queuedLocally: true,
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
