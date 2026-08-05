// server.js
import 'dotenv/config';
import express from 'express';
import { createServer } from 'http';
import { Server } from 'socket.io';
import { createClient } from '@supabase/supabase-js';
import helmet from 'helmet';
import cors from 'cors';
import compression from 'compression';
import rateLimit from 'express-rate-limit';
import morgan from 'morgan';
import { v4 as uuidv4 } from 'uuid';

// --- Supabase Client ---
const supabaseUrl = process.env.SUPABASE_URL;
const supabaseKey = process.env.SUPABASE_KEY;

if (!supabaseUrl || !supabaseKey) {
  console.error('Missing SUPABASE_URL or SUPABASE_KEY in environment variables.');
  console.error('Please ensure .env file exists and contains SUPABASE_URL and SUPABASE_KEY.');
  process.exit(1);
}

const supabase = createClient(supabaseUrl, supabaseKey);

// --- Express App Setup ---
const app = express();
const httpServer = createServer(app);
const io = new Server(httpServer, {
  cors: {
    origin: 'https://equacards.netlify.app', // Allow requests from your frontend
    methods: ['GET', 'POST'],
    credentials: true
  }
});

const PORT = process.env.PORT || 3000;

// --- Middleware ---
app.use(helmet()); // Secure your app by setting various HTTP headers
app.use(compression()); // Compress response bodies for all requests

const limiter = rateLimit({
  windowMs: 15 * 60 * 1000, // 15 minutes
  max: 100, // Limit each IP to 100 requests per window (here, per 15 minutes)
  standardHeaders: true, // Return rate limit info in the `RateLimit-*` headers
  legacyHeaders: false, // Disable the `X-RateLimit-*` headers
  message: { success: false, error: 'Too many requests, please try again after 15 minutes.' }
});

// Apply the rate limiting middleware to all API requests (excluding Socket.IO)
app.use('/api/', limiter);
app.use(express.json()); // Parse JSON request bodies
app.use(express.urlencoded({ extended: true })); // Parse URL-encoded request bodies
app.use(morgan('combined')); // HTTP request logger

// CORS for Express API endpoints (Socket.IO CORS is configured separately)
app.use(cors({
  origin: 'https://equacards.netlify.app',
  credentials: true
}));

// --- In-memory Match Management ---
const activeMatches = new Map(); // roomCode -> MatchState object
const userSocketMap = new Map(); // username -> socket.id (for reconnection/finding active socket)
const socketUserMap = new Map(); // socket.id -> username

// Match state structure
class MatchState {
  constructor(roomCode, hostSocketId, hostUsername) {
    this.roomCode = roomCode;
    this.host = { id: hostSocketId, username: hostUsername, isReady: false };
    this.guest = null;
    this.status = 'waiting'; // waiting, countdown, playing, ended
    this.players = new Map(); // Map<socketId, { username, isReady, currentScore, lastAnswerTime, ... }>
    this.players.set(hostSocketId, { username: hostUsername, isReady: false, currentScore: 0 });
    this.gameData = {
      round: 0,
      timer: 0,
      equation: '',
      correctAnswer: 0,
      playersScores: {} // username -> score
    };
    this.gameData.playersScores[hostUsername] = 0;
    this.heartbeats = new Map(); // socketId -> lastSeenTimestamp
    this.heartbeats.set(hostSocketId, Date.now()); // Initialize host heartbeat
    this.lastActivity = Date.now(); // For overall room cleanup
    this.countdownInterval = null;
    this.gameInterval = null;
    this.gameRoundTimeout = null;
    this.chatHistory = [];
  }

  getPlayerCount() {
    return this.players.size;
  }

  getPlayersInfo() {
    return Array.from(this.players.values()).map(p => ({
      username: p.username,
      isReady: p.isReady,
      currentScore: p.currentScore
    }));
  }

  isEveryoneReady() {
    if (this.getPlayerCount() < 2) return false;
    for (const player of this.players.values()) {
      if (!player.isReady) return false;
    }
    return true;
  }

  resetGameData() {
    this.gameData = {
      round: 0,
      timer: 0,
      equation: '',
      correctAnswer: 0,
      playersScores: {}
    };
    for (const player of this.players.values()) {
      player.currentScore = 0;
      player.isReady = false; // Reset readiness for rematch
      this.gameData.playersScores[player.username] = 0;
    }
    this.status = 'waiting';
    if (this.countdownInterval) {
      clearInterval(this.countdownInterval);
      this.countdownInterval = null;
    }
    if (this.gameInterval) {
      clearInterval(this.gameInterval);
      this.gameInterval = null;
    }
    if (this.gameRoundTimeout) {
      clearTimeout(this.gameRoundTimeout);
      this.gameRoundTimeout = null;
    }
  }

  // Clears all intervals and timeouts associated with this match
  clearTimers() {
    if (this.countdownInterval) clearInterval(this.countdownInterval);
    if (this.gameInterval) clearInterval(this.gameInterval);
    if (this.gameRoundTimeout) clearTimeout(this.gameRoundTimeout);
    this.countdownInterval = null;
    this.gameInterval = null;
    this.gameRoundTimeout = null;
  }
}

// Heartbeat interval to check for inactive users in matches
setInterval(() => {
  const now = Date.now();
  for (const [roomCode, match] of activeMatches.entries()) {
    // Check players' heartbeats
    for (const [socketId, lastSeen] of match.heartbeats.entries()) {
      if (now - lastSeen > 30 * 1000) { // 30 seconds inactivity
        const username = socketUserMap.get(socketId);
        console.log(`[Heartbeat] Player ${username} (${socketId}) in room ${roomCode} timed out.`);

        // Forcefully disconnect the timed-out socket
        const timedOutSocket = io.sockets.sockets.get(socketId);
        if (timedOutSocket) {
          timedOutSocket.emit('disconnectWarning', { message: 'You have been disconnected due to inactivity.' });
          timedOutSocket.disconnect(true); // true to close the underlying connection
        }
        // Disconnect handler will take care of room cleanup
      }
    }
  }
}, 15 * 1000); // Check every 15 seconds

// Global room cleanup for stale or genuinely abandoned rooms
setInterval(() => {
  const now = Date.now();
  for (const [roomCode, match] of activeMatches.entries()) {
    // If a room has been waiting for more than 5 minutes and is empty or single-player
    if (match.status === 'waiting' && match.getPlayerCount() <= 1 && (now - match.lastActivity > 5 * 60 * 1000)) {
      console.log(`[Room Cleanup] Deleting stale room ${roomCode} due to inactivity.`);
      activeMatches.delete(roomCode);
      match.clearTimers();
      io.to(roomCode).emit('matchCancelled', { message: 'Match cancelled due to inactivity.' });
      io.sockets.in(roomCode).socketsLeave(roomCode); // Force all sockets to leave the room
      // Update DB match status
      supabase.from('matches').update({
        status: 'cancelled',
        : new Date().toISOString()
      }).eq('room_code', roomCode).then(({ error }) => {
        if (error) console.error(`[Supabase Error] Updating match status to cancelled for ${roomCode}:`, error);
      });
    }
  }
}, 60 * 1000); // Check every minute

// --- Helper Functions ---
const generateRoomCode = () => {
  let code;
  do {
    // Generate a 6 character alphanumeric code
    code = Math.random().toString(36).substring(2, 8).toUpperCase();
  } while (activeMatches.has(code)); // Ensure uniqueness
  return code;
};

// Function to generate a simple math equation
const generateEquation = (level = 1) => {
  let num1, num2, operator, result;
  const operators = ['+', '-', '*']; // Division removed for simplicity to ensure integer results
  const maxNum = Math.min(20 + level * 5, 100); // Max number increases with level, capped at 100

  do {
    num1 = Math.floor(Math.random() * maxNum) + 1;
    num2 = Math.floor(Math.random() * maxNum) + 1;
    operator = operators[Math.floor(Math.random() * operators.length)];

    switch (operator) {
      case '+':
        result = num1 + num2;
        break;
      case '-':
        result = num1 - num2;
        break;
      case '*':
        result = num1 * num2;
        break;
      default:
        result = 0; // Should not happen
    }
  } while (result < 0 || result > 200); // Ensure results are within a reasonable range

  return { equation: `${num1} ${operator} ${num2}`, correctAnswer: result };
};


// --- API Endpoints ---
// All API endpoints are prefixed with '/api' to avoid conflict with root health check
// Health Check
app.get('/', (req, res) => {
  try {
    console.log('[API] Health check requested');
    res.status(200).json({ success: true, message: 'Server is healthy' });
  } catch (error) {
    console.error('[API Error] Health check:', error);
    res.status(500).json({ success: false, error: 'Internal server error' });
  }
});

// Leaderboard
app.get('/api/leaderboard', async (req, res) => {
  try {
    const limit = parseInt(req.query.limit, 10) || 10;
    console.log(`[API] Leaderboard requested with limit: ${limit}`);

    // Assuming a Supabase function 'get_leaderboard' exists as defined in the prompt.
    // This function should return 'username' and 'highest_score'.
    const { data, error } = await supabase
      .rpc('get_leaderboard', { limit_val: limit });

    if (error) {
      console.error('[Supabase Error] Leaderboard fetch:', error);
      throw error;
    }

    res.status(200).json({ success: true, data });
  } catch (error) {
    console.error('[API Error] Leaderboard:', error.message || error);
    res.status(500).json({ success: false, error: error.message || 'Failed to retrieve leaderboard.' });
  }
});

// Users - Create/Update User (Upsert)
app.post('/api/users', async (req, res) => {
  try {
    const { username, email } = req.body;

    if (!username || !email) {
      return res.status(400).json({ success: false, error: 'Username and email are required.' });
    }

    const now = new Date().toISOString();

    const { data, error } = await supabase
      .from('users')
      .upsert(
        {
          username: username.trim(),
          email: email.trim(),
          last_active: now,
          
        },
        {
          onConflict: 'username', // Conflict on username
          ignoreDuplicates: false // Ensure update happens on conflict
        }
      )
      .select(); // Return the upserted record

    if (error) {
      // Supabase typically handles 'onConflict' gracefully, but explicit duplicate handling can be useful
      if (error.code === '23505') { // PostgreSQL unique_violation for cases where onConflict might not be fully applied as expected (e.g., if multiple unique constraints)
        console.warn(`[Supabase Warning] Duplicate username constraint violated for ${username}.`);
        return res.status(409).json({ success: false, error: 'Username already exists and cannot be duplicated.' });
      }
      console.error('[Supabase Error] User upsert:', error);
      throw error;
    }

    console.log(`[API] User ${username} upserted successfully.`);
    res.status(200).json({ success: true, data: data[0] });
  } catch (error) {
    console.error('[API Error] User upsert:', error.message || error);
    res.status(500).json({ success: false, error: error.message || 'Failed to upsert user.' });
  }
});

// Users - Get All Users
app.get('/api/users', async (req, res) => {
  try {
    console.log('[API] Get all users requested');
    const { data, error } = await supabase
      .from('users')
      .select('*');

    if (error) {
      console.error('[Supabase Error] Get all users:', error);
      throw error;
    }

    res.status(200).json({ success: true, data });
  } catch (error) {
    console.error('[API Error] Get all users:', error.message || error);
    res.status(500).json({ success: false, error: error.message || 'Failed to retrieve users.' });
  }
});

// Scores - Post New Score or Update Highest Score
app.post('/api/scores', async (req, res) => {
  try {
    const { username, email, score, round, combo, correct, wrong, accuracy, level } = req.body;

    // Basic validation
    if (!username || !email || score === undefined || round === undefined || combo === undefined ||
      correct === undefined || wrong === undefined || accuracy === undefined || level === undefined) {
      return res.status(400).json({ success: false, error: 'All score fields (username, email, score, round, combo, correct, wrong, accuracy, level) are required.' });
    }

    const now = new Date().toISOString();
    const trimmedUsername = username.trim();
    const trimmedEmail = email.trim();

    // First, check if a score for this username already exists.
    // This assumes `scores` table holds a single best score per user.
    const { data: existingScoreData, error: fetchError } = await supabase
      .from('scores')
      .select('id, score')
      .eq('username', trimmedUsername)
      .single();

    if (fetchError && fetchError.code !== 'PGRST116') { // PGRST116 means 'no rows found'
      console.error('[Supabase Error] Fetching existing score:', fetchError);
      throw fetchError;
    }

    let resultData;
    if (existingScoreData) {
      // Player exists in scores table
      if (score > existingScoreData.score) {
        // New score is higher, update all score-related fields
        const { data, error } = await supabase
          .from('scores')
          .update({
            email: trimmedEmail,
            score,
            round,
            combo,
            correct,
            wrong,
            accuracy,
            level,
           
          })
          .eq('id', existingScoreData.id)
          .select();
        if (error) {
          console.error('[Supabase Error] Updating score (higher):', error);
          throw error;
        }
        resultData = data[0];
        console.log(`[API] Score for ${trimmedUsername} updated to a new high score: ${score}`);
      } else {
        // New score is not higher, only update 
        const { data, error } = await supabase
          .from('scores')
          .eq('id', existingScoreData.id)
          .select();
        if (error) {
          console.error('[Supabase Error] Updating score (not higher, only ):', error);
          throw error;
        }
        resultData = data[0];
        console.log(`[API] Score for ${trimmedUsername} not updated (not higher).`);
      }
    } else {
      // Player does not exist in scores table, insert new record
      const { data, error } = await supabase
        .from('scores')
        .insert({
          id: uuidv4(), // Generate UUID for new score record
          username: trimmedUsername,
          email: trimmedEmail,
          score,
          round,
          combo,
          correct,
          wrong,
          accuracy,
          level,
          created_at: now,
          
        })
        .select();
      if (error) {
        // Handle potential unique constraint violation on username if defined in DB
        if (error.code === '23505') {
          console.warn(`[Supabase Warning] Duplicate username tried to be inserted into scores: ${trimmedUsername}`);
          return res.status(409).json({ success: false, error: 'A score for this username already exists.' });
        }
        console.error('[Supabase Error] Inserting new score:', error);
        throw error;
      }
      resultData = data[0];
      console.log(`[API] New score for ${trimmedUsername} inserted: ${score}`);
    }

    res.status(200).json({ success: true, data: resultData });
  } catch (error) {
    console.error('[API Error] Post score:', error.message || error);
    res.status(500).json({ success: false, error: error.message || 'Failed to post score.' });
  }
});

// Scores - Get All Scores
app.get('/api/scores', async (req, res) => {
  try {
    console.log('[API] Get all scores requested');
    const { data, error } = await supabase
      .from('scores')
      .select('*');

    if (error) {
      console.error('[Supabase Error] Get all scores:', error);
      throw error;
    }

    res.status(200).json({ success: true, data });
  } catch (error) {
    console.error('[API Error] Get all scores:', error.message || error);
    res.status(500).json({ success: false, error: error.message || 'Failed to retrieve scores.' });
  }
});


// --- Socket.IO Handlers ---
io.on('connection', (socket) => {
  console.log(`[Socket.IO] User connected: ${socket.id}`);
  socket.emit('connected', { message: 'Successfully connected to the game server.' });

  socket.on('heartbeat', (data) => {
    const roomCode = socket.data.roomId;
    if (roomCode && activeMatches.has(roomCode)) {
      activeMatches.get(roomCode).heartbeats.set(socket.id, Date.now());
    }
  });

  socket.on('createMatch', async ({ username }) => {
    try {
      if (!username) {
        socket.emit('error', { message: 'Username is required to create a match.' });
        return;
      }
      const trimmedUsername = username.trim();
      console.log(`[Socket.IO] createMatch from ${trimmedUsername} (${socket.id})`);

      // Check if user is already in a match
      if (socket.data.roomId && activeMatches.has(socket.data.roomId)) {
        socket.emit('error', { message: `You are already in a match (room ${socket.data.roomId}). Please leave it first.` });
        return;
      }

      const roomCode = generateRoomCode();
      const now = new Date().toISOString();

      // Store match in Supabase
      const { data, error } = await supabase
        .from('matches')
        .insert({
          id: uuidv4(),
          room_code: roomCode,
          host_username: trimmedUsername,
          status: 'waiting',
          created_at: now,
         
        })
        .select();

      if (error) {
        console.error('[Supabase Error] createMatch:', error);
        socket.emit('error', { message: 'Failed to create match in database.' });
        return;
      }

      socket.join(roomCode);
      socket.data.roomId = roomCode;
      socket.data.username = trimmedUsername;
      userSocketMap.set(trimmedUsername, socket.id);
      socketUserMap.set(socket.id, trimmedUsername);

      const newMatch = new MatchState(roomCode, socket.id, trimmedUsername);
      activeMatches.set(roomCode, newMatch);

      io.to(roomCode).emit('matchCreated', { roomCode, host: trimmedUsername, match: newMatch.getPlayersInfo() });
      io.to(roomCode).emit('roomUpdate', { match: newMatch.getPlayersInfo(), status: newMatch.status, host: newMatch.host.username, gameData: newMatch.gameData, chatHistory: newMatch.chatHistory });
      console.log(`[Room Create] Room ${roomCode} created by ${trimmedUsername}.`);
    } catch (error) {
      console.error('[Socket.IO Error] createMatch:', error.message || error);
      socket.emit('error', { message: 'Failed to create match.' });
    }
  });

  socket.on('joinMatch', async ({ roomCode, username }) => {
    try {
      if (!roomCode || !username) {
        socket.emit('error', { message: 'Room code and username are required to join a match.' });
        return;
      }
      const trimmedUsername = username.trim();
      const trimmedRoomCode = roomCode.trim();
      console.log(`[Socket.IO] joinMatch by ${trimmedUsername} (${socket.id}) to room ${trimmedRoomCode}`);

      let match = activeMatches.get(trimmedRoomCode);

      if (!match) {
        // Try to retrieve from DB in case server restarted or in-memory lost
        const { data, error } = await supabase
          .from('matches')
          .select('*')
          .eq('room_code', trimmedRoomCode)
          .single();

        if (error || !data) {
          console.error('[Supabase Error] joinMatch - room not found in DB:', error?.message || 'No data');
          socket.emit('error', { message: 'Match not found or already ended.' });
          return;
        }

        // Reconstruct match state (basic)
        // Note: Host socket ID is unknown, will be updated if host reconnects
        match = new MatchState(data.room_code, null, data.host_username);
        match.status = data.status;
        if (data.guest_username) {
          match.guest = { id: null, username: data.guest_username, isReady: false };
          match.players.set(null, { username: data.guest_username, isReady: false, currentScore: 0 }); // Temporarily null ID
          match.gameData.playersScores[data.guest_username] = 0;
        }
        activeMatches.set(trimmedRoomCode, match);
        console.log(`[Socket.IO] Reconstructed match ${trimmedRoomCode} from database.`);
      }

      // Handle reconnection: if the username is already associated with a socket
      if (userSocketMap.has(trimmedUsername)) {
        const oldSocketId = userSocketMap.get(trimmedUsername);
        if (oldSocketId !== socket.id) {
          // If a user tries to join with the same username but a new socket, disconnect the old one
          const oldSocket = io.sockets.sockets.get(oldSocketId);
          if (oldSocket) {
            console.log(`[Socket.IO] Disconnecting old socket ${oldSocketId} for user ${trimmedUsername} to allow reconnection.`);
            oldSocket.emit('reconnectAttempt', { message: 'You have connected from another device. This connection will be closed.' });
            oldSocket.disconnect(true);
          }
        }
      }

      // Check if trying to join a full room as a new player
      if (match.getPlayerCount() >= 2 && !match.players.has(socket.id) && ![match.host?.username, match.guest?.username].includes(trimmedUsername)) {
        socket.emit('error', { message: 'Match is full.' });
        return;
      }

      // Cannot join a game in progress if not already part of it
      if (match.status !== 'waiting' && !match.players.has(socket.id)) {
        socket.emit('error', { message: 'Match has already started.' });
        return;
      }

      // --- Player Reconnection/Join Logic ---
      let playerInfo = match.players.get(socket.id);
      let isReconnecting = false;

      if (playerInfo && playerInfo.username === trimmedUsername) {
        // This is the same socket and username, probably just re-emitting joinMatch
        isReconnecting = true;
      } else if (match.host && match.host.username === trimmedUsername) {
        // Host rejoining, update socket ID
        if (match.players.has(userSocketMap.get(trimmedUsername))) {
            match.players.delete(userSocketMap.get(trimmedUsername)); // Remove old socket entry if any
        }
        match.host.id = socket.id;
        playerInfo = { username: trimmedUsername, isReady: match.host.isReady, currentScore: match.gameData.playersScores[trimmedUsername] || 0 };
        match.players.set(socket.id, playerInfo);
        isReconnecting = true;
      } else if (match.guest && match.guest.username === trimmedUsername) {
        // Guest rejoining, update socket ID
        if (match.players.has(userSocketMap.get(trimmedUsername))) {
            match.players.delete(userSocketMap.get(trimmedUsername)); // Remove old socket entry if any
        }
        match.guest.id = socket.id;
        playerInfo = { username: trimmedUsername, isReady: match.guest.isReady, currentScore: match.gameData.playersScores[trimmedUsername] || 0 };
        match.players.set(socket.id, playerInfo);
        isReconnecting = true;
      } else if (!match.guest && match.host.username !== trimmedUsername) {
        // New guest joining (and host is different)
        match.guest = { id: socket.id, username: trimmedUsername, isReady: false };
        playerInfo = { username: trimmedUsername, isReady: false, currentScore: 0 };
        match.players.set(socket.id, playerInfo);
        match.gameData.playersScores[trimmedUsername] = 0; // Initialize score for guest
        isReconnecting = false;
      } else if (match.host?.username === trimmedUsername && !match.host.id) {
         // Host from DB but no active socket found yet
         match.host.id = socket.id;
         playerInfo = { username: trimmedUsername, isReady: match.host.isReady, currentScore: match.gameData.playersScores[trimmedUsername] || 0 };
         match.players.set(socket.id, playerInfo);
         isReconnecting = true;
      }
      else {
        // Should not happen if logic is sound, but catches other cases like third player with new username
        socket.emit('error', { message: 'A player with this username is already in the room or the room is full.' });
        return;
      }

      socket.join(trimmedRoomCode);
      socket.data.roomId = trimmedRoomCode;
      socket.data.username = trimmedUsername;
      userSocketMap.set(trimmedUsername, socket.id);
      socketUserMap.set(socket.id, trimmedUsername);
      match.heartbeats.set(socket.id, Date.now()); // Reset heartbeat

      // Update match status in DB (e.g., add guest username)
      await supabase.from('matches').update({
        guest_username: match.guest?.username || null,
         new Date().toISOString()
      }).eq('room_code', trimmedRoomCode);

      if (isReconnecting) {
        io.to(trimmedRoomCode).emit('playerRejoined', { username: trimmedUsername });
        console.log(`[Room Join] Player ${trimmedUsername} (${socket.id}) reconnected to room ${trimmedRoomCode}.`);
      } else {
        io.to(trimmedRoomCode).emit('playerJoined', { username: trimmedUsername, roomCode: trimmedRoomCode });
        console.log(`[Room Join] New player ${trimmedUsername} (${socket.id}) joined room ${trimmedRoomCode}.`);
      }

      // Always send full state on join/rejoin
      io.to(trimmedRoomCode).emit('roomUpdate', {
        match: match.getPlayersInfo(),
        status: match.status,
        host: match.host?.username,
        gameData: match.gameData,
        chatHistory: match.chatHistory
      });
      socket.emit('syncGameState', {
        match: match.getPlayersInfo(),
        status: match.status,
        host: match.host?.username,
        gameData: match.gameData,
        chatHistory: match.chatHistory
      });

    } catch (error) {
      console.error('[Socket.IO Error] joinMatch:', error.message || error);
      socket.emit('error', { message: 'Failed to join match.' });
    }
  });

  socket.on('leaveMatch', async () => {
    try {
      const roomCode = socket.data.roomId;
      const username = socket.data.username;
      if (!roomCode || !username) return;

      console.log(`[Socket.IO] leaveMatch from ${username} (${socket.id}) in room ${roomCode}`);

      socket.leave(roomCode);
      socket.data.roomId = undefined;
      userSocketMap.delete(username);
      socketUserMap.delete(socket.id);

      const match = activeMatches.get(roomCode);
      if (match) {
        match.players.delete(socket.id);
        match.heartbeats.delete(socket.id);

        if (match.host && match.host.id === socket.id) {
          match.host = null;
        } else if (match.guest && match.guest.id === socket.id) {
          match.guest = null;
        }

        // If host left and guest exists, guest becomes host
        if (!match.host && match.guest) {
          console.log(`[Room Update] ${match.guest.username} became host for room ${roomCode}.`);
          match.host = { ...match.guest };
          match.guest = null;
          // Update DB as well
          await supabase.from('matches').update({
            host_username: match.host.username,
            guest_username: null,
            new Date().toISOString()
          }).eq('room_code', roomCode);
        }

        io.to(roomCode).emit('playerLeft', { username });

        // Cleanup room if empty or single player not yet started
        if (match.getPlayerCount() < 2 && (match.status === 'waiting' || match.status === 'countdown')) {
          console.log(`[Room Cleanup] Deleting room ${roomCode} due to insufficient players.`);
          activeMatches.delete(roomCode);
          match.clearTimers();
          io.to(roomCode).emit('matchCancelled', { message: 'Match cancelled due to host leaving or insufficient players.' });
          // Update DB match status
          await supabase.from('matches').update({
            status: 'cancelled',
            new Date().toISOString()
          }).eq('room_code', roomCode);
        } else if (match.getPlayerCount() === 0) {
          console.log(`[Room Cleanup] Deleting empty room ${roomCode}.`);
          activeMatches.delete(roomCode);
          match.clearTimers();
          await supabase.from('matches').update({
            status: 'ended', // Or 'cancelled' depending on exact state
            new Date().toISOString()
          }).eq('room_code', roomCode);
        } else {
          io.to(roomCode).emit('roomUpdate', { match: match.getPlayersInfo(), status: match.status, host: match.host?.username, gameData: match.gameData, chatHistory: match.chatHistory });
        }
      }
    } catch (error) {
      console.error('[Socket.IO Error] leaveMatch:', error.message || error);
      socket.emit('error', { message: 'Failed to leave match.' });
    }
  });

  socket.on('cancelMatch', async () => {
    try {
      const roomCode = socket.data.roomId;
      const username = socket.data.username;
      if (!roomCode || !username) return;

      const match = activeMatches.get(roomCode);
      if (!match) return socket.emit('error', { message: 'Match not found.' });

      if (match.host?.username !== username) {
        socket.emit('error', { message: 'Only the host can cancel the match.' });
        return;
      }

      console.log(`[Room Cancel] Host ${username} cancelled room ${roomCode}.`);
      activeMatches.delete(roomCode);
      match.clearTimers();
      io.to(roomCode).emit('matchCancelled', { message: 'Host cancelled the match.' });

      // Clean up player data and force them to leave the room
      match.players.forEach((player, sockId) => {
        const playerSocket = io.sockets.sockets.get(sockId);
        if (playerSocket) {
          playerSocket.leave(roomCode);
          playerSocket.data.roomId = undefined;
          // userSocketMap.delete(player.username); // This will be handled by disconnect if they fully disconnect
          // socketUserMap.delete(sockId);
        }
      });
      io.sockets.in(roomCode).socketsLeave(roomCode); // Ensure all sockets leave

      // Update DB
      await supabase.from('matches').update({
        status: 'cancelled',
       new Date().toISOString()
      }).eq('room_code', roomCode);

    } catch (error) {
      console.error('[Socket.IO Error] cancelMatch:', error.message || error);
      socket.emit('error', { message: 'Failed to cancel match.' });
    }
  });

  socket.on('playerReady', () => {
    try {
      const roomCode = socket.data.roomId;
      const username = socket.data.username;
      if (!roomCode || !username) return;

      const match = activeMatches.get(roomCode);
      if (!match) return;

      const player = match.players.get(socket.id);
      if (player) {
        player.isReady = true;
        // Also update host/guest readiness states
        if (match.host?.id === socket.id) match.host.isReady = true;
        if (match.guest?.id === socket.id) match.guest.isReady = true;

        io.to(roomCode).emit('roomUpdate', { match: match.getPlayersInfo(), status: match.status, host: match.host?.username, gameData: match.gameData, chatHistory: match.chatHistory });
        console.log(`[Room Update] Player ${username} in room ${roomCode} is ready.`);
      }
    } catch (error) {
      console.error('[Socket.IO Error] playerReady:', error.message || error);
    }
  });

  socket.on('playerNotReady', () => {
    try {
      const roomCode = socket.data.roomId;
      const username = socket.data.username;
      if (!roomCode || !username) return;

      const match = activeMatches.get(roomCode);
      if (!match) return;

      const player = match.players.get(socket.id);
      if (player) {
        player.isReady = false;
        // Also update host/guest readiness states
        if (match.host?.id === socket.id) match.host.isReady = false;
        if (match.guest?.id === socket.id) match.guest.isReady = false;

        io.to(roomCode).emit('roomUpdate', { match: match.getPlayersInfo(), status: match.status, host: match.host?.username, gameData: match.gameData, chatHistory: match.chatHistory });
        console.log(`[Room Update] Player ${username} in room ${roomCode} is not ready.`);
      }
    } catch (error) {
      console.error('[Socket.IO Error] playerNotReady:', error.message || error);
    }
  });

  socket.on('startCountdown', () => {
    try {
      const roomCode = socket.data.roomId;
      const username = socket.data.username;
      if (!roomCode || !username) return;

      const match = activeMatches.get(roomCode);
      if (!match) return socket.emit('error', { message: 'Match not found.' });

      if (match.host?.username !== username) {
        socket.emit('error', { message: 'Only the host can start the countdown.' });
        return;
      }
      if (!match.isEveryoneReady()) {
        socket.emit('error', { message: 'All players must be ready to start the countdown.' });
        return;
      }
      if (match.getPlayerCount() < 2) {
        socket.emit('error', { message: 'At least two players are required to start the game.' });
        return;
      }
      if (match.status !== 'waiting') {
        socket.emit('error', { message: 'Match is not in waiting state.' });
        return;
      }

      match.status = 'countdown';
      let countdown = 5; // 5-second countdown
      io.to(roomCode).emit('countdownStart', { count: countdown });
      console.log(`[Game Start] Room ${roomCode} countdown started.`);

      match.countdownInterval = setInterval(() => {
        countdown--;
        io.to(roomCode).emit('countdown', { count: countdown });
        if (countdown <= 0) {
          clearInterval(match.countdownInterval);
          match.countdownInterval = null;
          // Start the match
          io.to(roomCode).emit('countdownEnd');
          startGame(roomCode);
        }
      }, 1000);
    } catch (error) {
      console.error('[Socket.IO Error] startCountdown:', error.message || error);
      socket.emit('error', { message: 'Failed to start countdown.' });
    }
  });

  const startGame = async (roomCode) => {
    try {
      const match = activeMatches.get(roomCode);
      if (!match) return;

      match.status = 'playing';
      match.resetGameData(); // Reset scores, rounds etc.
      // Reset readiness here as well in case of issues
      for (const player of match.players.values()) {
        player.isReady = false;
      }

      // Update DB match status
      await supabase.from('matches').update({
        status: 'playing',
         new Date().toISOString()
      }).eq('room_code', roomCode);

      console.log(`[Game Start] Room ${roomCode} game started.`);
      io.to(roomCode).emit('startMatch', {
        match: match.getPlayersInfo(),
        gameData: match.gameData
      });

      startNewRound(roomCode);
    } catch (error) {
      console.error(`[Socket.IO Error] startGame for room ${roomCode}:`, error.message || error);
      io.to(roomCode).emit('error', { message: 'Failed to start the game.' });
    }
  };

  const startNewRound = (roomCode, level = 1) => {
    const match = activeMatches.get(roomCode);
    if (!match) return;

    match.gameData.round++;
    const MAX_ROUNDS = 10; // Example: 10 rounds per game
    if (match.gameData.round > MAX_ROUNDS) {
      endMatch(roomCode);
      return;
    }

    if (match.gameRoundTimeout) clearTimeout(match.gameRoundTimeout);
    if (match.gameInterval) clearInterval(match.gameInterval);

    const { equation, correctAnswer } = generateEquation(level); // Use current game level
    match.gameData.equation = equation;
    match.gameData.correctAnswer = correctAnswer;
    match.gameData.timer = 15; // 15 seconds per round

    // Reset player's answered status for the new round if implementing immediate round progression
    // for (const player of match.players.values()) {
    //   player.hasAnsweredThisRound = false;
    // }

    io.to(roomCode).emit('roundUpdate', {
      round: match.gameData.round,
      equation,
      timer: match.gameData.timer,
      playersScores: match.gameData.playersScores
    });
    console.log(`[Game Round] Room ${roomCode} - Round ${match.gameData.round}: ${equation} = ${correctAnswer}`);

    match.gameInterval = setInterval(() => {
      if (match.gameData.timer <= 0) {
        clearInterval(match.gameInterval);
        match.gameInterval = null;
        // Move to next round after timeout
        startNewRound(roomCode, level);
      } else {
        match.gameData.timer--;
        io.to(roomCode).emit('timerUpdate', { timer: match.gameData.timer });
      }
    }, 1000);

    // Timeout to proceed to next round even if game timer is paused or something
    match.gameRoundTimeout = setTimeout(() => {
      if (match.gameData.timer > 0) { // If timer still running, clear it to avoid double-triggering
        clearInterval(match.gameInterval);
        match.gameInterval = null;
      }
      console.log(`[Game Timeout] Round ${match.gameData.round} in room ${roomCode} timed out. Advancing round.`);
      startNewRound(roomCode, level);
    }, match.gameData.timer * 1000 + 1000); // Give an extra second for final timer update/network latency
  };


  socket.on('submitAnswer', ({ answer }) => {
    try {
      const roomCode = socket.data.roomId;
      const username = socket.data.username;
      if (!roomCode || !username) return;

      const match = activeMatches.get(roomCode);
      if (!match || match.status !== 'playing') return socket.emit('error', { message: 'Game is not currently playing.' });

      const player = match.players.get(socket.id);
      if (!player) return;

      const isCorrect = parseInt(answer, 10) === match.gameData.correctAnswer;
      let scoreChange = 0;
      if (isCorrect) {
        scoreChange = 100 + (match.gameData.timer * 10); // Base score + bonus for speed
        player.currentScore += scoreChange;
        match.gameData.playersScores[username] = player.currentScore;
        console.log(`[Game Answer] Player ${username} in room ${roomCode} submitted correct answer. Score: ${player.currentScore}`);
        io.to(roomCode).emit('scoreUpdate', { username, score: player.currentScore, isCorrect: true, scoreChange });
      } else {
        scoreChange = -20; // Penalty for wrong answer
        player.currentScore += scoreChange;
        match.gameData.playersScores[username] = player.currentScore;
        console.log(`[Game Answer] Player ${username} in room ${roomCode} submitted wrong answer. Score: ${player.currentScore}`);
        io.to(roomCode).emit('scoreUpdate', { username, score: player.currentScore, isCorrect: false, scoreChange });
      }

      // If all players have answered, potentially advance the round faster
      // This part is commented out as round progression is primarily timer-driven for simplicity.
      // If immediate progression is desired, 'player.hasAnsweredThisRound' would need to be tracked.
      // player.hasAnsweredThisRound = true;
      // if (Array.from(match.players.values()).every(p => p.hasAnsweredThisRound)) {
      //   startNewRound(roomCode, match.gameData.level);
      // }
    } catch (error) {
      console.error('[Socket.IO Error] submitAnswer:', error.message || error);
      socket.emit('error', { message: 'Failed to submit answer.' });
    }
  });

  const endMatch = async (roomCode) => {
    try {
      const match = activeMatches.get(roomCode);
      if (!match) return;

      match.status = 'ended';
      match.clearTimers(); // Clear all intervals and timeouts

      let winner = 'Draw';
      let highestScore = -Infinity;
      let winnerUsername = null;
      let scoresToUpdate = [];

      // Determine winner and prepare scores for DB update
      for (const [socketId, player] of match.players.entries()) {
        const username = player.username;
        const score = player.currentScore;

        scoresToUpdate.push({ username, score });

        if (score > highestScore) {
          highestScore = score;
          winnerUsername = username;
          winner = username;
        } else if (score === highestScore && winnerUsername !== null) {
          winner = 'Draw'; // If multiple players have same highest score
        }
      }

      // Post final score to DB for each player
      for (const { username: playerUsername, score: finalScore } of scoresToUpdate) {
        // Fetch existing score for this player from 'scores' table
        const { data: existingScore, error: fetchScoreError } = await supabase
          .from('scores')
          .select('id, score')
          .eq('username', playerUsername)
          .single();

        if (fetchScoreError && fetchScoreError.code !== 'PGRST116') {
          console.error(`[Supabase Error] Fetching score for ${playerUsername} on match end:`, fetchScoreError);
          continue;
        }

        const now = new Date().toISOString();
        if (existingScore) {
          if (finalScore > existingScore.score) {
            await supabase.from('scores').update({
              score: finalScore,
              now,
              // Update other game stats if tracked (combo, correct, wrong, accuracy, level)
              // For now, using default/placeholder values as per original prompt for these not explicitly managed by game logic
              round: match.gameData.round,
              combo: 0, // Placeholder
              correct: 0, // Placeholder
              wrong: 0, // Placeholder
              accuracy: 0.0, // Placeholder
              level: 1, // Placeholder
            }).eq('id', existingScore.id);
            console.log(`[Supabase] Final score for ${playerUsername} updated to new high: ${finalScore}`);
          } else {
            await supabase.from('scores').update({ updated_at: now }).eq('id', existingScore.id);
            console.log(`[Supabase] Final score for ${playerUsername} not higher, only`);
          }
        } else {
          // No existing score, insert new one
          await supabase.from('scores').insert({
            id: uuidv4(),
            username: playerUsername,
            email: `${playerUsername}@equacards.com`, // Placeholder email, should ideally come from user registration
            score: finalScore,
            round: match.gameData.round,
            combo: 0, // Placeholder
            correct: 0, // Placeholder
            wrong: 0, // Placeholder
            accuracy: 0.0, // Placeholder
            level: 1, // Placeholder
            created_at: now,
          : now
          });
          console.log(`[Supabase] New final score for ${playerUsername} inserted: ${finalScore}`);
        }
      }

      // Update match status and winner in DB
      await supabase.from('matches').update({
        status: 'ended',
        winner: winner === 'Draw' ? null : winnerUsername, // Store winner or null for draw
       : new Date().toISOString()
      }).eq('room_code', roomCode);

      io.to(roomCode).emit('matchEnded', {
        scores: match.gameData.playersScores,
        winner: winnerUsername,
        message: winnerUsername === 'Draw' ? 'It\'s a draw!' : `${winnerUsername} wins!`
      });
      console.log(`[Game End] Room ${roomCode} match ended. Winner: ${winner}.`);

      // Keep room active for a bit to allow rematch, then clean up
      setTimeout(() => {
        if (activeMatches.has(roomCode) && activeMatches.get(roomCode).status === 'ended') {
          console.log(`[Room Cleanup] Removing ended room ${roomCode} after timeout.`);
          activeMatches.delete(roomCode);
          io.to(roomCode).socketsLeave(roomCode); // Force all sockets to leave the room
        }
      }, 5 * 60 * 1000); // 5 minutes after match ends for rematch possibility
    } catch (error) {
      console.error(`[Socket.IO Error] endMatch for room ${roomCode}:`, error.message || error);
      io.to(roomCode).emit('error', { message: 'An error occurred while ending the match.' });
    }
  };


  socket.on('rematch', async () => {
    try {
      const roomCode = socket.data.roomId;
      const username = socket.data.username;
      if (!roomCode || !username) return;

      const match = activeMatches.get(roomCode);
      if (!match) return socket.emit('error', { message: 'Match not found.' });

      if (match.status !== 'ended') {
        socket.emit('error', { message: 'Match must be ended to request a rematch.' });
        return;
      }

      // Reset players readiness and game data
      match.resetGameData(); // This also resets status to 'waiting' and player scores to 0
      match.chatHistory = []; // Clear chat for new match

      // Notify all players in the room about the rematch request
      io.to(roomCode).emit('rematchRequested', { username });
      io.to(roomCode).emit('roomUpdate', { match: match.getPlayersInfo(), status: match.status, host: match.host?.username, gameData: match.gameData, chatHistory: match.chatHistory });
      console.log(`[Game Rematch] Player ${username} requested rematch in room ${roomCode}.`);

      // Update match status in DB
      await supabase.from('matches').update({
        status: 'waiting',
        winner: null,
       : new Date().toISOString()
      }).eq('room_code', roomCode);
    } catch (error) {
      console.error('[Socket.IO Error] rematch:', error.message || error);
      socket.emit('error', { message: 'Failed to request rematch.' });
    }
  });


  socket.on('chatMessage', ({ message }) => {
    try {
      const roomCode = socket.data.roomId;
      const username = socket.data.username;
      if (!roomCode || !username || !message || message.trim() === '') return;

      const match = activeMatches.get(roomCode);
      if (!match) return;

      const chatEntry = { username, message: message.trim(), timestamp: Date.now() };
      match.chatHistory.push(chatEntry);
      if (match.chatHistory.length > 50) { // Keep chat history limited to last 50 messages
        match.chatHistory.shift();
      }

      io.to(roomCode).emit('chatMessage', chatEntry);
      console.log(`[Chat] Room ${roomCode} - ${username}: ${message.trim()}`);
    } catch (error) {
      console.error('[Socket.IO Error] chatMessage:', error.message || error);
    }
  });

  socket.on('typing', ({ isTyping }) => {
    try {
      const roomCode = socket.data.roomId;
      const username = socket.data.username;
      if (!roomCode || !username) return;

      // Broadcast to all in the room EXCEPT the sender
      socket.to(roomCode).emit('typing', { username, isTyping });
    } catch (error) {
      console.error('[Socket.IO Error] typing:', error.message || error);
    }
  });


  socket.on('disconnect', async () => {
    try {
      const roomCode = socket.data.roomId;
      const username = socket.data.username;
      console.log(`[Socket.IO] User disconnected: ${socket.id}${username ? ` (${username})` : ''}`);

      if (username) {
        userSocketMap.delete(username);
        socketUserMap.delete(socket.id);
      }

      if (roomCode && activeMatches.has(roomCode)) {
        const match = activeMatches.get(roomCode);
        match.heartbeats.delete(socket.id); // Remove heartbeat entry

        // Remove the disconnected socket from the players list
        match.players.delete(socket.id);

        io.to(roomCode).emit('playerLeft', { username });
        console.log(`[Room Update] Player ${username} left room ${roomCode}.`);

        if (match.host && match.host.id === socket.id) {
          match.host = null;
        } else if (match.guest && match.guest.id === socket.id) {
          match.guest = null;
        }

        // If guest is present and host left, promote guest to host
        if (!match.host && match.guest) {
          console.log(`[Room Update] Promoting ${match.guest.username} to host in room ${roomCode}.`);
          match.host = { ...match.guest };
          match.guest = null;
          // Update DB as well
          await supabase.from('matches').update({
            host_username: match.host.username,
            guest_username: null,
        : new Date().toISOString()
          }).eq('room_code', roomCode);
        }

        // Cleanup room if empty or single player not yet started
        if (match.getPlayerCount() < 2 && (match.status === 'waiting' || match.status === 'countdown')) {
          console.log(`[Room Cleanup] Deleting room ${roomCode} due to insufficient players.`);
          activeMatches.delete(roomCode);
          match.clearTimers();
          io.to(roomCode).emit('matchCancelled', { message: 'Match cancelled due to host leaving or insufficient players.' });
          await supabase.from('matches').update({
            status: 'cancelled',
           : new Date().toISOString()
          }).eq('room_code', roomCode);
        } else if (match.getPlayerCount() === 0) {
          console.log(`[Room Cleanup] Deleting empty room ${roomCode}.`);
          activeMatches.delete(roomCode);
          match.clearTimers();
          await supabase.from('matches').update({
            status: 'ended', // Or 'cancelled' if no game started
         : new Date().toISOString()
          }).eq('room_code', roomCode);
        } else {
          // If match continues with remaining players, send room update
          io.to(roomCode).emit('roomUpdate', { match: match.getPlayersInfo(), status: match.status, host: match.host?.username, gameData: match.gameData, chatHistory: match.chatHistory });
        }
      }
    } catch (error) {
      console.error('[Socket.IO Error] disconnect:', error.message || error);
    }
  });
});


// Graceful shutdown
process.on('SIGTERM', () => {
  console.log('SIGTERM signal received: closing HTTP server');
  httpServer.close(() => {
    console.log('HTTP server closed');
    process.exit(0);
  });
});

process.on('SIGINT', () => {
  console.log('SIGINT signal received: closing HTTP server');
  httpServer.close(() => {
    console.log('HTTP server closed');
    process.exit(0);
  });
});

// Start server
httpServer.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
  console.log(`CORS allowed origin: https://equacards.netlify.app`);
});

/*
--- Supabase SQL Function for Leaderboard ---

IMPORTANT: You need to create this function in your Supabase SQL editor for the /api/leaderboard endpoint to work correctly.

-- Function to get the highest score per username for the leaderboard
CREATE OR REPLACE FUNCTION get_leaderboard(limit_val INT DEFAULT 10)
RETURNS TABLE (username TEXT, highest_score INT)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    SELECT s.username, MAX(s.score) AS highest_score
    FROM scores s
    GROUP BY s.username
    ORDER BY highest_score DESC
    LIMIT limit_val;
END;
$$;

*/
