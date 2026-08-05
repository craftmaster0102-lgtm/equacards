 import express from 'express';
import { createServer } from 'http';
import { Server } from 'socket.io';
import { createClient } from '@supabase/supabase-js';
import helmet from 'helmet';
import compression from 'compression';
import cors from 'cors';
import dotenv from 'dotenv';
import rateLimit from 'express-rate-limit';

dotenv.config();

const PORT = parseInt(process.env.PORT || '3000', 10);
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_PUBLISHABLE_KEY = process.env.SUPABASE_PUBLISHABLE_KEY;
const CORS_ORIGIN = process.env.CORS_ORIGIN || 'https://equacards.netlify.app,http://localhost:5500,http://127.0.0.1:5500';

if (!SUPABASE_URL || !SUPABASE_PUBLISHABLE_KEY) {
  console.error(JSON.stringify({ level: 'fatal', message: 'Missing Supabase environment variables', timestamp: new Date().toISOString() }));
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_PUBLISHABLE_KEY);

const app = express();
const httpServer = createServer(app);

const whitelist = CORS_ORIGIN.split(',').map(o => o.trim()).filter(Boolean);

const corsOptions = {
  origin: (origin, callback) => {
    if (!origin || whitelist.includes(origin)) {
      callback(null, true);
    } else {
      callback(new Error('Not allowed by CORS'));
    }
  },
  credentials: true,
  methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
  allowedHeaders: ['Content-Type', 'Authorization', 'X-Requested-With'],
  optionsSuccessStatus: 200
};

app.use(cors(corsOptions));
const io = new Server(httpServer, {
  cors: corsOptions,
  pingTimeout: 60000,
  pingInterval: 25000,
  transports: ['websocket', 'polling'],
  maxHttpBufferSize: 1e6
});

app.use(helmet());
app.use(compression());
app.use(express.json({ limit: '10kb' }));

const logger = {
  info: (msg, meta = {}) => console.log(JSON.stringify({ level: 'info', message: msg, timestamp: new Date().toISOString(), ...meta })),
  error: (msg, meta = {}) => console.error(JSON.stringify({ level: 'error', message: msg, timestamp: new Date().toISOString(), ...meta })),
  warn: (msg, meta = {}) => console.warn(JSON.stringify({ level: 'warn', message: msg, timestamp: new Date().toISOString(), ...meta }))
};

const globalLimiter = rateLimit({
  windowMs: 15 * 60 * 1000,
  max: 200,
  standardHeaders: true,
  legacyHeaders: false,
  handler: (req, res) => res.status(429).json({ error: 'Too many requests' })
});
app.use(globalLimiter);

const scoreLimiter = rateLimit({
  windowMs: 60 * 1000,
  max: 20,
  standardHeaders: true,
  legacyHeaders: false,
  handler: (req, res) => res.status(429).json({ error: 'Score submission rate limit exceeded' })
});

const roomCreateLimiter = rateLimit({
  windowMs: 60 * 1000,
  max: 10,
  standardHeaders: true,
  legacyHeaders: false,
  handler: (req, res) => res.status(429).json({ error: 'Room creation rate limit exceeded' })
});

const sanitizeString = (str) => {
  if (typeof str !== 'string') return '';
  return str.trim().replace(/[<>]/g, '').slice(0, 200);
};

const isValidEmail = (email) => /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email);

class RoomManager {
  constructor() {
    this.rooms = new Map();
    this.socketToRoom = new Map();
    this.disconnectTimers = new Map();
    this.DISCONNECT_TIMEOUT = 300000;
    this.ROOM_EXPIRY = 3600000;
    this.MAX_ROOMS = 1000;
  }

  generateRoomId() {
    return Math.random().toString(36).substring(2, 8).toUpperCase();
  }

  createRoom(hostId) {
    if (this.rooms.size >= this.MAX_ROOMS) {
      throw new Error('Maximum rooms reached');
    }
    let roomId;
    let attempts = 0;
    do {
      roomId = this.generateRoomId();
      attempts++;
    } while (this.rooms.has(roomId) && attempts < 100);
    if (this.rooms.has(roomId)) throw new Error('Failed to generate unique room ID');

    const room = {
      roomId,
      hostId,
      createdAt: Date.now(),
      updatedAt: Date.now(),
      status: 'waiting',
      players: new Map(),
      scores: new Map(),
      combos: new Map(),
      readyState: new Map(),
      currentRound: 0,
      timer: null,
      winner: null,
      gameState: null,
      matchStartedAt: null,
      chat: []
    };
    this.rooms.set(roomId, room);
    return room;
  }

  getRoom(roomId) {
    return this.rooms.get(roomId) || null;
  }

  joinRoom(roomId, playerId, playerData) {
    const room = this.rooms.get(roomId);
    if (!room) return null;
    if (room.players.size >= 4) return null;
    if (room.status !== 'waiting') return null;

    room.players.set(playerId, {
      id: playerId,
      username: sanitizeString(playerData.username || 'Anonymous'),
      socketId: playerData.socketId,
      connected: true,
      joinedAt: Date.now()
    });
    room.scores.set(playerId, 0);
    room.combos.set(playerId, 0);
    room.readyState.set(playerId, false);
    room.updatedAt = Date.now();
    this.socketToRoom.set(playerData.socketId, roomId);
    return room;
  }

  leaveRoom(roomId, playerId) {
    const room = this.rooms.get(roomId);
    if (!room) return;
    room.players.delete(playerId);
    room.scores.delete(playerId);
    room.combos.delete(playerId);
    room.readyState.delete(playerId);
    if (room.players.size === 0) {
      this.destroyRoom(roomId);
    } else {
      room.updatedAt = Date.now();
      if (room.hostId === playerId) {
        const nextHost = room.players.keys().next().value;
        if (nextHost) room.hostId = nextHost;
      }
    }
  }

  setPlayerReady(roomId, playerId, ready) {
    const room = this.rooms.get(roomId);
    if (!room) return false;
    room.readyState.set(playerId, ready);
    room.updatedAt = Date.now();
    return true;
  }

  startMatch(roomId) {
    const room = this.rooms.get(roomId);
    if (!room) return false;
    room.status = 'playing';
    room.currentRound = 1;
    room.matchStartedAt = Date.now();
    room.updatedAt = Date.now();
    room.gameState = {
      round: 1,
      question: null,
      answers: new Map(),
      penalty: 0,
      accuracy: 0,
      startTime: Date.now()
    };
    return true;
  }

  endMatch(roomId, winnerId) {
    const room = this.rooms.get(roomId);
    if (!room) return false;
    room.status = 'finished';
    room.winner = winnerId;
    room.updatedAt = Date.now();
    return true;
  }

  updateScore(roomId, playerId, score) {
    const room = this.rooms.get(roomId);
    if (!room) return false;
    if (typeof score !== 'number' || score < 0 || score > 999999) return false;
    const current = room.scores.get(playerId) || 0;
    if (score >= current) {
      room.scores.set(playerId, score);
      room.updatedAt = Date.now();
      return true;
    }
    return false;
  }

  updateCombo(roomId, playerId, combo) {
    const room = this.rooms.get(roomId);
    if (!room) return false;
    if (typeof combo !== 'number' || combo < 0) return false;
    room.combos.set(playerId, combo);
    return true;
  }

  handleDisconnect(socketId) {
    const roomId = this.socketToRoom.get(socketId);
    if (!roomId) return null;
    const room = this.rooms.get(roomId);
    if (!room) return null;

    for (const [playerId, player] of room.players) {
      if (player.socketId === socketId) {
        player.connected = false;
        const timerKey = `${roomId}:${playerId}`;
        const existingTimer = this.disconnectTimers.get(timerKey);
        if (existingTimer) clearTimeout(existingTimer);
        const timer = setTimeout(() => {
          this.destroyRoom(roomId);
        }, this.DISCONNECT_TIMEOUT);
        this.disconnectTimers.set(timerKey, timer);
        return { roomId, playerId };
      }
    }
    return null;
  }

  handleReconnect(roomId, playerId, socketId) {
    const room = this.rooms.get(roomId);
    if (!room) return null;
    const player = room.players.get(playerId);
    if (!player) return null;

    const timerKey = `${roomId}:${playerId}`;
    const timer = this.disconnectTimers.get(timerKey);
    if (timer) {
      clearTimeout(timer);
      this.disconnectTimers.delete(timerKey);
    }

    player.socketId = socketId;
    player.connected = true;
    this.socketToRoom.set(socketId, roomId);
    room.updatedAt = Date.now();
    return room;
  }

  destroyRoom(roomId) {
    const room = this.rooms.get(roomId);
    if (!room) return;
    for (const [playerId] of room.players) {
      const timerKey = `${roomId}:${playerId}`;
      const timer = this.disconnectTimers.get(timerKey);
      if (timer) clearTimeout(timer);
      this.disconnectTimers.delete(timerKey);
    }
    for (const [socketId, rid] of this.socketToRoom) {
      if (rid === roomId) this.socketToRoom.delete(socketId);
    }
    this.rooms.delete(roomId);
  }

  cleanup() {
    const now = Date.now();
    const expired = [];
    for (const [roomId, room] of this.rooms) {
      if (room.status === 'waiting' && now - room.createdAt > this.ROOM_EXPIRY) {
        expired.push(roomId);
        continue;
      }
      if (room.players.size === 0) {
        this.destroyRoom(roomId);
      }
    }
    return expired;
  }

  getPublicRoomState(room) {
    return {
      roomId: room.roomId,
      hostId: room.hostId,
      status: room.status,
      players: Array.from(room.players.values()).map(p => ({
        id: p.id,
        username: p.username,
        connected: p.connected
      })),
      scores: Object.fromEntries(room.scores),
      combos: Object.fromEntries(room.combos),
      readyState: Object.fromEntries(room.readyState),
      currentRound: room.currentRound,
      timer: room.timer,
      winner: room.winner,
      gameState: room.gameState ? {
        round: room.gameState.round,
        question: room.gameState.question,
        penalty: room.gameState.penalty,
        accuracy: room.gameState.accuracy
      } : null
    };
  }
}

const roomManager = new RoomManager();

const socketRateLimits = new Map();
io.use((socket, next) => {
  const ip = socket.handshake.address;
  const now = Date.now();
  const window = 1000;
  const max = 30;
  const entry = socketRateLimits.get(ip) || { count: 0, reset: now + window };
  if (now > entry.reset) {
    entry.count = 1;
    entry.reset = now + window;
  } else {
    entry.count++;
  }
  socketRateLimits.set(ip, entry);
  if (entry.count > max) {
    return next(new Error('Rate limit exceeded'));
  }
  next();
});

io.engine.on('connection_error', (err) => {
  logger.error('Socket connection error', { message: err.message, code: err.code });
});

io.on('connection', (socket) => {
  logger.info('Player connected', { socketId: socket.id, ip: socket.handshake.address });

  socket.on('ping', (cb) => {
    if (typeof cb === 'function') cb({ time: Date.now() });
  });

  socket.on('heartbeat', (cb) => {
    if (typeof cb === 'function') cb({ time: Date.now() });
  });

  socket.on('createRoom', (data, callback) => {
    try {
      if (typeof callback !== 'function') return;
      const playerId = sanitizeString(data?.playerId);
      const username = sanitizeString(data?.username);
      if (!playerId || !username) {
        return callback({ success: false, error: 'Invalid player data' });
      }
      const room = roomManager.createRoom(playerId);
      const joined = roomManager.joinRoom(room.roomId, playerId, { username, socketId: socket.id });
      if (!joined) {
        roomManager.destroyRoom(room.roomId);
        return callback({ success: false, error: 'Failed to create room' });
      }
      socket.join(room.roomId);
      logger.info('Room created', { roomId: room.roomId, hostId: playerId });
      callback({ success: true, room: roomManager.getPublicRoomState(room) });
    } catch (err) {
      logger.error('createRoom error', { error: err.message, socketId: socket.id });
      if (typeof callback === 'function') callback({ success: false, error: 'Server error' });
    }
  });

  socket.on('joinRoom', (data, callback) => {
    try {
      if (typeof callback !== 'function') return;
      const roomId = sanitizeString(data?.roomId)?.toUpperCase();
      const playerId = sanitizeString(data?.playerId);
      const username = sanitizeString(data?.username);
      if (!roomId || !playerId || !username) {
        return callback({ success: false, error: 'Invalid data' });
      }
      const room = roomManager.getRoom(roomId);
      if (!room) {
        return callback({ success: false, error: 'Room not found' });
      }
      if (room.status !== 'waiting') {
        return callback({ success: false, error: 'Match already started' });
      }
      const joined = roomManager.joinRoom(roomId, playerId, { username, socketId: socket.id });
      if (!joined) {
        return callback({ success: false, error: 'Room is full or unavailable' });
      }
      socket.join(roomId);
      socket.to(roomId).emit('playerJoined', { playerId, username });
      logger.info('Player joined room', { roomId, playerId });
      callback({ success: true, room: roomManager.getPublicRoomState(room) });
    } catch (err) {
      logger.error('joinRoom error', { error: err.message, socketId: socket.id });
      if (typeof callback === 'function') callback({ success: false, error: 'Server error' });
    }
  });

  socket.on('leaveRoom', (data) => {
    try {
      const roomId = sanitizeString(data?.roomId)?.toUpperCase();
      const playerId = sanitizeString(data?.playerId);
      if (!roomId || !playerId) return;
      roomManager.leaveRoom(roomId, playerId);
      socket.leave(roomId);
      socket.to(roomId).emit('playerLeft', { playerId });
      logger.info('Player left room', { roomId, playerId });
    } catch (err) {
      logger.error('leaveRoom error', { error: err.message, socketId: socket.id });
    }
  });

  socket.on('cancelRoom', (data, callback) => {
    try {
      if (typeof callback !== 'function') return;
      const roomId = sanitizeString(data?.roomId)?.toUpperCase();
      const playerId = sanitizeString(data?.playerId);
      const room = roomManager.getRoom(roomId);
      if (!room || room.hostId !== playerId) {
        return callback({ success: false, error: 'Unauthorized' });
      }
      room.status = 'cancelled';
      io.to(roomId).emit('roomCancelled', { roomId });
      roomManager.destroyRoom(roomId);
      logger.info('Room cancelled', { roomId });
      callback({ success: true });
    } catch (err) {
      logger.error('cancelRoom error', { error: err.message, socketId: socket.id });
      if (typeof callback === 'function') callback({ success: false, error: 'Server error' });
    }
  });

  socket.on('reconnect', (data, callback) => {
    try {
      if (typeof callback !== 'function') return;
      const roomId = sanitizeString(data?.roomId)?.toUpperCase();
      const playerId = sanitizeString(data?.playerId);
      if (!roomId || !playerId) {
        return callback({ success: false, error: 'Invalid data' });
      }
      const room = roomManager.handleReconnect(roomId, playerId, socket.id);
      if (!room) {
        return callback({ success: false, error: 'Reconnection failed' });
      }
      socket.join(roomId);
      logger.info('Player reconnected', { roomId, playerId, socketId: socket.id });
      callback({ success: true, room: roomManager.getPublicRoomState(room) });
      io.to(roomId).emit('playerRejoined', { playerId });
    } catch (err) {
      logger.error('reconnect error', { error: err.message, socketId: socket.id });
      if (typeof callback === 'function') callback({ success: false, error: 'Server error' });
    }
  });

  socket.on('playerReady', (data) => {
    try {
      const roomId = sanitizeString(data?.roomId)?.toUpperCase();
      const playerId = sanitizeString(data?.playerId);
      if (!roomId || !playerId) return;
      const room = roomManager.getRoom(roomId);
      if (!room) return;
      roomManager.setPlayerReady(roomId, playerId, true);
      io.to(roomId).emit('playerReady', { playerId });
      const allReady = room.players.size > 1 && Array.from(room.readyState.values()).every(r => r === true);
      if (allReady && room.status === 'waiting') {
        room.status = 'countdown';
        io.to(roomId).emit('startCountdown', { countdown: 3 });
        setTimeout(() => {
          const started = roomManager.startMatch(roomId);
          if (started) {
            io.to(roomId).emit('matchStarted', { room: roomManager.getPublicRoomState(room) });
            logger.info('Match started', { roomId });
          }
        }, 3000);
      }
    } catch (err) {
      logger.error('playerReady error', { error: err.message, socketId: socket.id });
    }
  });

  socket.on('playerNotReady', (data) => {
    try {
      const roomId = sanitizeString(data?.roomId)?.toUpperCase();
      const playerId = sanitizeString(data?.playerId);
      if (!roomId || !playerId) return;
      roomManager.setPlayerReady(roomId, playerId, false);
      socket.to(roomId).emit('playerNotReady', { playerId });
    } catch (err) {
      logger.error('playerNotReady error', { error: err.message, socketId: socket.id });
    }
  });

  socket.on('submitMove', (data) => {
    try {
      const roomId = sanitizeString(data?.roomId)?.toUpperCase();
      const playerId = sanitizeString(data?.playerId);
      const move = data?.move;
      if (!roomId || !playerId || move === undefined) return;
      const room = roomManager.getRoom(roomId);
      if (!room || room.status !== 'playing') return;
      socket.to(roomId).emit('moveSubmitted', { playerId, move });
    } catch (err) {
      logger.error('submitMove error', { error: err.message, socketId: socket.id });
    }
  });

  socket.on('submitAnswer', (data, callback) => {
    try {
      if (typeof callback !== 'function') return;
      const roomId = sanitizeString(data?.roomId)?.toUpperCase();
      const playerId = sanitizeString(data?.playerId);
      const answer = data?.answer;
      const timeTaken = data?.timeTaken;
      if (!roomId || !playerId || answer === undefined) {
        return callback({ success: false, error: 'Invalid data' });
      }
      const room = roomManager.getRoom(roomId);
      if (!room || room.status !== 'playing') {
        return callback({ success: false, error: 'Match not active' });
      }

      let isCorrect = false;
      if (room.gameState && room.gameState.question && room.gameState.question.answer !== undefined) {
        isCorrect = String(answer).trim().toLowerCase() === String(room.gameState.question.answer).trim().toLowerCase();
      }

      const baseScore = isCorrect ? 100 : 0;
      const timeBonus = isCorrect ? Math.max(0, Math.floor(50 - (timeTaken || 0) / 200)) : 0;
      const scoreDelta = baseScore + timeBonus;
      const currentScore = room.scores.get(playerId) || 0;
      const newScore = currentScore + scoreDelta;

      roomManager.updateScore(roomId, playerId, newScore);
      if (isCorrect) {
        const currentCombo = room.combos.get(playerId) || 0;
        roomManager.updateCombo(roomId, playerId, currentCombo + 1);
      } else {
        roomManager.updateCombo(roomId, playerId, 0);
      }

      callback({ success: true, correct: isCorrect, score: newScore, scoreDelta });
      io.to(roomId).emit('answerResult', { playerId, correct: isCorrect, score: newScore, scoreDelta });
      io.to(roomId).emit('scoreUpdate', { scores: Object.fromEntries(room.scores), combos: Object.fromEntries(room.combos) });
    } catch (err) {
      logger.error('submitAnswer error', { error: err.message, socketId: socket.id });
      if (typeof callback === 'function') callback({ success: false, error: 'Server error' });
    }
  });

  socket.on('scoreUpdate', (data) => {
    try {
      const roomId = sanitizeString(data?.roomId)?.toUpperCase();
      const playerId = sanitizeString(data?.playerId);
      const score = data?.score;
      if (!roomId || !playerId || typeof score !== 'number') return;
      const room = roomManager.getRoom(roomId);
      if (!room || room.status !== 'playing') return;
      const current = room.scores.get(playerId) || 0;
      if (score >= current && score <= current + 1000) {
        roomManager.updateScore(roomId, playerId, score);
        io.to(roomId).emit('scoreUpdate', { scores: Object.fromEntries(room.scores), combos: Object.fromEntries(room.combos) });
      }
    } catch (err) {
      logger.error('scoreUpdate error', { error: err.message, socketId: socket.id });
    }
  });

  socket.on('timerUpdate', (data) => {
    try {
      const roomId = sanitizeString(data?.roomId)?.toUpperCase();
      const timeLeft = data?.timeLeft;
      if (!roomId || typeof timeLeft !== 'number') return;
      const room = roomManager.getRoom(roomId);
      if (!room || room.status !== 'playing') return;
      room.timer = timeLeft;
      socket.to(roomId).emit('timerUpdate', { timeLeft });
    } catch (err) {
      logger.error('timerUpdate error', { error: err.message, socketId: socket.id });
    }
  });

  socket.on('roundUpdate', (data) => {
    try {
      const roomId = sanitizeString(data?.roomId)?.toUpperCase();
      const round = data?.round;
      if (!roomId || typeof round !== 'number' || round < 1) return;
      const room = roomManager.getRoom(roomId);
      if (!room || room.status !== 'playing') return;
      room.currentRound = round;
      if (room.gameState) room.gameState.round = round;
      room.updatedAt = Date.now();
      io.to(roomId).emit('roundUpdate', { round });
    } catch (err) {
      logger.error('roundUpdate error', { error: err.message, socketId: socket.id });
    }
  });

  socket.on('syncGameState', (data) => {
    try {
      const roomId = sanitizeString(data?.roomId)?.toUpperCase();
      const playerId = sanitizeString(data?.playerId);
      const gameState = data?.gameState;
      if (!roomId || !playerId || !gameState) return;
      const room = roomManager.getRoom(roomId);
      if (!room || room.status !== 'playing') return;

      if (gameState.round && typeof gameState.round === 'number') {
        room.currentRound = gameState.round;
      }
      if (gameState.question) {
        if (!room.gameState) room.gameState = {};
        room.gameState.question = gameState.question;
      }
      if (typeof gameState.penalty === 'number') {
        if (!room.gameState) room.gameState = {};
        room.gameState.penalty = gameState.penalty;
      }
      if (typeof gameState.accuracy === 'number') {
        if (!room.gameState) room.gameState = {};
        room.gameState.accuracy = gameState.accuracy;
      }
      room.updatedAt = Date.now();

      io.to(roomId).emit('gameStateSync', {
        round: room.currentRound,
        question: room.gameState ? room.gameState.question : null,
        penalty: room.gameState ? room.gameState.penalty : 0,
        accuracy: room.gameState ? room.gameState.accuracy : 0,
        scores: Object.fromEntries(room.scores),
        combos: Object.fromEntries(room.combos)
      });
    } catch (err) {
      logger.error('syncGameState error', { error: err.message, socketId: socket.id });
    }
  });

  socket.on('requestResync', (data, callback) => {
    try {
      if (typeof callback !== 'function') return;
      const roomId = sanitizeString(data?.roomId)?.toUpperCase();
      const room = roomManager.getRoom(roomId);
      if (!room) {
        return callback({ success: false, error: 'Room not found' });
      }
      callback({
        success: true,
        room: roomManager.getPublicRoomState(room)
      });
    } catch (err) {
      logger.error('requestResync error', { error: err.message, socketId: socket.id });
      if (typeof callback === 'function') callback({ success: false, error: 'Server error' });
    }
  });

  socket.on('matchEnded', async (data) => {
    try {
      const roomId = sanitizeString(data?.roomId)?.toUpperCase();
      const winnerId = sanitizeString(data?.winnerId);
      if (!roomId) return;
      const room = roomManager.getRoom(roomId);
      if (!room) return;

      roomManager.endMatch(roomId, winnerId);

      let highestScore = 0;
      for (const score of room.scores.values()) {
        if (score > highestScore) highestScore = score;
      }

      const duration = room.matchStartedAt ? Math.floor((Date.now() - room.matchStartedAt) / 1000) : 0;
      const players = Array.from(room.players.values()).map(p => ({
        id: p.id,
        username: p.username,
        score: room.scores.get(p.id) || 0
      }));

      const { error } = await supabase.from('matches').insert({
        room_id: roomId,
        players,
        winner: winnerId || null,
        duration,
        highest_score: highestScore,
        status: 'completed',
        created_at: new Date(room.createdAt).toISOString(),
        finished_at: new Date().toISOString()
      });

      if (error) {
        logger.error('Failed to save match', { error: error.message, roomId });
      } else {
        logger.info('Match saved', { roomId, winner: winnerId, duration });
      }

      io.to(roomId).emit('matchEnded', {
        room: roomManager.getPublicRoomState(room),
        winner: winnerId,
        duration
      });
    } catch (err) {
      logger.error('matchEnded error', { error: err.message, socketId: socket.id });
    }
  });

  socket.on('rematch', (data) => {
    try {
      const roomId = sanitizeString(data?.roomId)?.toUpperCase();
      const playerId = sanitizeString(data?.playerId);
      const room = roomManager.getRoom(roomId);
      if (!room || room.hostId !== playerId) return;
      room.status = 'waiting';
      room.currentRound = 0;
      room.winner = null;
      room.gameState = null;
      room.matchStartedAt = null;
      room.timer = null;
      room.scores.clear();
      room.combos.clear();
      room.readyState.clear();
      for (const player of room.players.values()) {
        room.scores.set(player.id, 0);
        room.combos.set(player.id, 0);
        room.readyState.set(player.id, false);
      }
      room.updatedAt = Date.now();
      io.to(roomId).emit('rematchStarted', { room: roomManager.getPublicRoomState(room) });
      logger.info('Rematch started', { roomId });
    } catch (err) {
      logger.error('rematch error', { error: err.message, socketId: socket.id });
    }
  });

  socket.on('chatMessage', (data) => {
    try {
      const roomId = sanitizeString(data?.roomId)?.toUpperCase();
      const playerId = sanitizeString(data?.playerId);
      const message = sanitizeString(data?.message);
      if (!roomId || !playerId || !message) return;
      const room = roomManager.getRoom(roomId);
      if (!room) return;
      const player = room.players.get(playerId);
      if (!player) return;
      const chatData = {
        playerId,
        username: player.username,
        message,
        timestamp: Date.now()
      };
      room.chat.push(chatData);
      if (room.chat.length > 100) room.chat.shift();
      io.to(roomId).emit('chatMessage', chatData);
    } catch (err) {
      logger.error('chatMessage error', { error: err.message, socketId: socket.id });
    }
  });

  socket.on('typing', (data) => {
    try {
      const roomId = sanitizeString(data?.roomId)?.toUpperCase();
      const playerId = sanitizeString(data?.playerId);
      const isTyping = !!data?.isTyping;
      if (!roomId || !playerId) return;
      socket.to(roomId).emit('playerTyping', { playerId, isTyping });
    } catch (err) {
      logger.error('typing error', { error: err.message, socketId: socket.id });
    }
  });

  socket.on('spectatorJoin', (data, callback) => {
    try {
      if (typeof callback !== 'function') return;
      const roomId = sanitizeString(data?.roomId)?.toUpperCase();
      const room = roomManager.getRoom(roomId);
      if (!room) {
        return callback({ success: false, error: 'Room not found' });
      }
      socket.join(roomId);
      callback({ success: true, room: roomManager.getPublicRoomState(room) });
    } catch (err) {
      logger.error('spectatorJoin error', { error: err.message, socketId: socket.id });
      if (typeof callback === 'function') callback({ success: false, error: 'Server error' });
    }
  });

  socket.on('disconnect', (reason) => {
    logger.info('Player disconnected', { socketId: socket.id, reason });
    const dcInfo = roomManager.handleDisconnect(socket.id);
    if (dcInfo) {
      const room = roomManager.getRoom(dcInfo.roomId);
      if (room) {
        io.to(dcInfo.roomId).emit('playerDisconnected', { playerId: dcInfo.playerId });
      }
    }
    roomManager.socketToRoom.delete(socket.id);
  });
});

app.use((req, res, next) => {
  const start = Date.now();
  res.on('finish', () => {
    const duration = Date.now() - start;
    logger.info('API request', {
      method: req.method,
      path: req.path,
      status: res.statusCode,
      duration,
      ip: req.ip
    });
  });
  next();
});

app.get('/', (req, res) => {
  res.json({ status: 'ok', service: 'equacards-backend', timestamp: new Date().toISOString() });
});

app.get('/leaderboard', async (req, res, next) => {
  try {
    const { data, error } = await supabase
      .from('scores')
      .select('*')
      .order('score', { ascending: false })
      .limit(100);
    if (error) throw error;
    res.json(data || []);
  } catch (err) {
    next(err);
  }
});

app.get('/users', async (req, res, next) => {
  try {
    const { data, error } = await supabase.from('users').select('*');
    if (error) throw error;
    res.json(data || []);
  } catch (err) {
    next(err);
  }
});

app.get('/matches', async (req, res, next) => {
  try {
    const { data, error } = await supabase
      .from('matches')
      .select('*')
      .order('created_at', { ascending: false })
      .limit(100);
    if (error) throw error;
    res.json(data || []);
  } catch (err) {
    next(err);
  }
});

async function saveScoreLogic(username, score, email) {
  username = sanitizeString(username);
  if (!username) throw new Error('Username is required');
  if (typeof score !== 'number' || isNaN(score)) throw new Error('Score must be a number');
  if (score < 0) throw new Error('Score cannot be negative');
  if (score > 999999) throw new Error('Score exceeds maximum allowed');

  const { data: existing, error: fetchError } = await supabase
    .from('scores')
    .select('id, score')
    .eq('username', username)
    .single();

  if (fetchError && fetchError.code !== 'PGRST116') throw fetchError;

  if (existing) {
    if (score > existing.score) {
      const { error } = await supabase
        .from('scores')
        .update({ score, updated_at: new Date().toISOString() })
        .eq('id', existing.id);
      if (error) throw error;
      return { updated: true, username, score, previousScore: existing.score };
    }
    return { updated: false, username, score: existing.score, message: 'Existing score is higher or equal' };
  }

  const { error } = await supabase
    .from('scores')
    .insert({
      username,
      score,
      created_at: new Date().toISOString(),
      updated_at: new Date().toISOString()
    });
  if (error) throw error;
  return { created: true, username, score };
}

app.post('/scores', scoreLimiter, async (req, res, next) => {
  try {
    const { username, score, email } = req.body || {};
    const result = await saveScoreLogic(username, score, email);
    res.status(201).json(result);
  } catch (err) {
    next(err);
  }
});

app.post('/save-score', scoreLimiter, async (req, res, next) => {
  try {
    const { username, score, email } = req.body || {};
    const result = await saveScoreLogic(username, score, email);
    res.status(201).json(result);
  } catch (err) {
    next(err);
  }
});

app.post('/saveScore', scoreLimiter, async (req, res, next) => {
  try {
    const { username, score, email } = req.body || {};
    const result = await saveScoreLogic(username, score, email);
    res.status(201).json(result);
  } catch (err) {
    next(err);
  }
});

app.post('/users', async (req, res, next) => {
  try {
    const { username, email } = req.body || {};
    const cleanUsername = sanitizeString(username);
    const cleanEmail = sanitizeString(email);

    if (!cleanUsername) return res.status(400).json({ error: 'Username is required' });
    if (cleanUsername.length < 2 || cleanUsername.length > 30) {
      return res.status(400).json({ error: 'Username must be 2-30 characters' });
    }
    if (cleanEmail && !isValidEmail(cleanEmail)) {
      return res.status(400).json({ error: 'Invalid email format' });
    }

    const { data: existing, error: fetchError } = await supabase
      .from('users')
      .select('id')
      .eq('username', cleanUsername)
      .single();

    if (fetchError && fetchError.code !== 'PGRST116') throw fetchError;

    if (existing) {
      const { error } = await supabase
        .from('users')
        .update({
          email: cleanEmail || null,
          updated_at: new Date().toISOString()
        })
        .eq('id', existing.id);
      if (error) throw error;
      return res.json({ updated: true, username: cleanUsername });
    }

    const { error } = await supabase
      .from('users')
      .insert({
        username: cleanUsername,
        email: cleanEmail || null,
        created_at: new Date().toISOString(),
        updated_at: new Date().toISOString()
      });
    if (error) throw error;
    res.status(201).json({ created: true, username: cleanUsername });
  } catch (err) {
    next(err);
  }
});

app.post('/matches', async (req, res, next) => {
  try {
    const { room_id, players, winner, duration, highest_score, status } = req.body || {};
    if (!room_id || !Array.isArray(players)) {
      return res.status(400).json({ error: 'Invalid match data: room_id and players array required' });
    }
    const { error } = await supabase.from('matches').insert({
      room_id: sanitizeString(room_id),
      players,
      winner: winner || null,
      duration: typeof duration === 'number' ? duration : 0,
      highest_score: typeof highest_score === 'number' ? highest_score : 0,
      status: sanitizeString(status) || 'completed',
      created_at: new Date().toISOString(),
      finished_at: new Date().toISOString()
    });
    if (error) throw error;
    res.status(201).json({ success: true });
  } catch (err) {
    next(err);
  }
});

app.use((req, res) => {
  res.status(404).json({ error: 'Not found' });
});

app.use((err, req, res, next) => {
  logger.error('API error', {
    message: err.message,
    path: req.path,
    method: req.method,
    stack: process.env.NODE_ENV === 'development' ? err.stack : undefined
  });

  if (err instanceof SyntaxError && err.status === 400 && 'body' in err) {
    return res.status(400).json({ error: 'Invalid JSON payload' });
  }

  if (err.message === 'Not allowed by CORS') {
    return res.status(403).json({ error: 'CORS origin not allowed' });
  }

  res.status(err.status || 500).json({
    error: err.message || 'Internal server error'
  });
});

const cleanupInterval = setInterval(() => {
  try {
    const expired = roomManager.cleanup();
    for (const roomId of expired) {
      io.to(roomId).emit('roomExpired', { roomId, reason: 'Room expired due to inactivity' });
      roomManager.destroyRoom(roomId);
      logger.info('Room expired and destroyed', { roomId });
    }
  } catch (err) {
    logger.error('Cleanup error', { message: err.message });
  }
}, 60000);

const shutdown = (signal) => {
  logger.info(`Received ${signal}, shutting down gracefully`);
  clearInterval(cleanupInterval);
  io.close(() => {
    logger.info('Socket.IO server closed');
  });
  httpServer.close(() => {
    logger.info('HTTP server closed');
    process.exit(0);
  });
  setTimeout(() => {
    logger.error('Forced shutdown after timeout');
    process.exit(1);
  }, 30000);
};

process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));

process.on('uncaughtException', (err) => {
  logger.error('Uncaught exception', { message: err.message, stack: err.stack });
  shutdown('uncaughtException');
});

process.on('unhandledRejection', (reason, promise) => {
  logger.error('Unhandled rejection', { reason: String(reason) });
});

httpServer.listen(PORT, () => {
  logger.info('Server started', { port: PORT, env: process.env.NODE_ENV || 'production' });
});
