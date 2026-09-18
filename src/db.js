'use strict';

const { DatabaseSync } = require('node:sqlite');
const { now } = require('./util');

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

/**
 * 持久层：SQLite（WAL 模式）。
 *
 * 可靠性设计要点：
 * 1. 消息与房间序号 seq 在同一事务中「先落库、后广播」——进程崩溃也不丢已确认消息。
 * 2. messages 上 (room_id, sender_id, client_msg_id) 唯一约束——客户端重试/网络重复
 *    提交同一条消息时不会产生重复记录，实现发送幂等。
 * 3. seq 为每房间单调递增序号，由 rooms.last_seq 计数器在事务内分配——保证房间内
 *    消息全序（时序可控），客户端可凭 seq 检测空洞并触发补发。
 *
 * 并发与忙锁：
 * - 关键写路径（写消息、保存游标、建房、禁言等）在 SQLITE_BUSY/LOCKED 时做有限次
 *   指数退避重试；退避用 setTimeout（异步），不占用事件循环。
 * - busy_timeout 设较短，到点未拿到锁立即抛出交由应用层重试，避免同步阻塞事件循环。
 * - 事务体尽量收窄：只包含必须的读写，广播/发帧一律在事务提交之后进行。
 */

/** 判断是否为 SQLite 忙/锁错误（SQLITE_BUSY / SQLITE_LOCKED） */
function isBusyError(err) {
  if (!err) return false;
  if (err.code === 'SQLITE_BUSY' || err.code === 'SQLITE_LOCKED') return true;
  // node:sqlite 将扩展码归并为 ERR_SQLITE_ERROR，需结合消息文案判断
  if (err.code === 'ERR_SQLITE_ERROR') {
    return /database is locked|database table is locked/i.test(err.message || '');
  }
  return false;
}

const SCHEMA = `
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;
PRAGMA synchronous = FULL;

CREATE TABLE IF NOT EXISTS users (
  id           TEXT PRIMARY KEY,
  name         TEXT NOT NULL UNIQUE,
  token_random TEXT NOT NULL,
  created_at   INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS rooms (
  id         TEXT PRIMARY KEY,
  name       TEXT NOT NULL UNIQUE,
  created_by TEXT NOT NULL REFERENCES users(id),
  created_at INTEGER NOT NULL,
  last_seq   INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS members (
  room_id     TEXT NOT NULL REFERENCES rooms(id),
  user_id     TEXT NOT NULL REFERENCES users(id),
  role        TEXT NOT NULL DEFAULT 'member' CHECK (role IN ('admin','member')),
  muted_until INTEGER NOT NULL DEFAULT 0,
  joined_at   INTEGER NOT NULL,
  PRIMARY KEY (room_id, user_id)
);

CREATE TABLE IF NOT EXISTS messages (
  room_id       TEXT NOT NULL REFERENCES rooms(id),
  seq           INTEGER NOT NULL,
  client_msg_id TEXT NOT NULL,
  sender_id     TEXT NOT NULL REFERENCES users(id),
  content       TEXT NOT NULL,
  ts            INTEGER NOT NULL,
  PRIMARY KEY (room_id, seq),
  UNIQUE (room_id, sender_id, client_msg_id)  -- 幂等键
);

-- 服务端保存的每用户每房间已确认游标（断线补发的兜底依据）
CREATE TABLE IF NOT EXISTS cursors (
  room_id      TEXT NOT NULL REFERENCES rooms(id),
  user_id      TEXT NOT NULL REFERENCES users(id),
  last_ack_seq INTEGER NOT NULL DEFAULT 0,
  updated_at   INTEGER NOT NULL,
  PRIMARY KEY (room_id, user_id)
);
`;

const MSG_SELECT = `
  SELECT m.room_id AS roomId, m.seq, m.client_msg_id AS clientMsgId,
         m.sender_id AS "from", u.name AS fromName, m.content, m.ts
    FROM messages m JOIN users u ON u.id = m.sender_id
`;

class ChatDB {
  constructor(dbPath, options = {}) {
    this.db = new DatabaseSync(dbPath);
    this.busyTimeoutMs = options.busyTimeoutMs ?? 1_500;
    this.maxRetries = options.maxRetries ?? 5;
    this.backoffBaseMs = options.backoffBaseMs ?? 10;
    this.backoffMaxMs = options.backoffMaxMs ?? 200;
    this.db.exec(`PRAGMA busy_timeout = ${this.busyTimeoutMs};`);
    this.db.exec(SCHEMA);
    this._prepare();
  }

  /**
   * 关键写操作的有限次忙锁重试：仅对 SQLITE_BUSY/LOCKED 退避后重试，其它错误立即抛出。
   * 退避走 setTimeout（非阻塞），等待期间事件循环可处理其它连接。
   */
  async _withRetry(label, fn) {
    let attempt = 0;
    for (;;) {
      try {
        return fn();
      } catch (err) {
        if (!isBusyError(err) || attempt >= this.maxRetries) throw err;
        const delay = this._backoffDelay(attempt);
        console.warn(
          `[db] ${label} 忙锁，第 ${attempt + 1}/${this.maxRetries} 次重试，等待 ${delay}ms`
        );
        await sleep(delay);
        attempt++;
      }
    }
  }

  _prepare() {
    const d = this.db;
    this.stmt = {
      insertUser: d.prepare('INSERT INTO users (id, name, token_random, created_at) VALUES (?, ?, ?, ?)'),
      userByName: d.prepare('SELECT * FROM users WHERE name = ?'),
      userById: d.prepare('SELECT * FROM users WHERE id = ?'),

      insertRoom: d.prepare('INSERT INTO rooms (id, name, created_by, created_at) VALUES (?, ?, ?, ?)'),
      roomById: d.prepare('SELECT * FROM rooms WHERE id = ?'),
      roomByName: d.prepare('SELECT * FROM rooms WHERE name = ?'),
      roomsForUser: d.prepare(
        `SELECT r.id, r.name, r.last_seq AS lastSeq, m.role, m.muted_until AS mutedUntil
           FROM rooms r JOIN members m ON m.room_id = r.id
          WHERE m.user_id = ? ORDER BY r.created_at`
      ),

      upsertMember: d.prepare(
        `INSERT INTO members (room_id, user_id, role, muted_until, joined_at)
         VALUES (?, ?, ?, 0, ?)
         ON CONFLICT (room_id, user_id) DO NOTHING`
      ),
      member: d.prepare('SELECT * FROM members WHERE room_id = ? AND user_id = ?'),
      setMuted: d.prepare('UPDATE members SET muted_until = ? WHERE room_id = ? AND user_id = ?'),
      membersOfRoom: d.prepare(
        `SELECT m.user_id AS userId, u.name, m.role, m.muted_until AS mutedUntil
           FROM members m JOIN users u ON u.id = m.user_id WHERE m.room_id = ?`
      ),

      // —— 消息写入（事务内使用）——
      msgByClientId: d.prepare(
        `${MSG_SELECT} WHERE m.room_id = ? AND m.sender_id = ? AND m.client_msg_id = ?`
      ),
      bumpSeq: d.prepare('UPDATE rooms SET last_seq = last_seq + 1 WHERE id = ? RETURNING last_seq'),
      insertMsg: d.prepare(
        'INSERT INTO messages (room_id, seq, client_msg_id, sender_id, content, ts) VALUES (?, ?, ?, ?, ?, ?)'
      ),

      // —— 消息读取 ——
      msgsAfter: d.prepare(`${MSG_SELECT} WHERE m.room_id = ? AND m.seq > ? ORDER BY m.seq LIMIT ?`),
      msgsBefore: d.prepare(
        `${MSG_SELECT} WHERE m.room_id = ? AND m.seq < ? ORDER BY m.seq DESC LIMIT ?`
      ),

      // —— 游标 ——
      upsertCursor: d.prepare(
        `INSERT INTO cursors (room_id, user_id, last_ack_seq, updated_at) VALUES (?, ?, ?, ?)
         ON CONFLICT (room_id, user_id)
         DO UPDATE SET last_ack_seq = MAX(last_ack_seq, excluded.last_ack_seq), updated_at = excluded.updated_at`
      ),
      cursor: d.prepare('SELECT last_ack_seq AS lastAckSeq FROM cursors WHERE room_id = ? AND user_id = ?'),
    };
  }

  /** 指数退避延迟（毫秒） */
  _backoffDelay(attempt) {
    return Math.min(this.backoffBaseMs * 2 ** attempt, this.backoffMaxMs);
  }

  /**
   * 在 IMMEDIATE 事务中执行 fn，失败回滚。
   * BEGIN / 事务体 / COMMIT 任一处遇到忙锁都会整体回滚后重试（共用 maxRetries 预算）：
   * 回滚后 bumpSeq 等操作一并撤销，重试不会造成 seq 跳号/空洞。
   * 注意：fn 内只能包含 DB 语句（重试会整体重新执行），不得夹带广播等外部副作用。
   */
  async _tx(fn) {
    let attempt = 0;
    for (;;) {
      let began = false;
      try {
        this.db.exec('BEGIN IMMEDIATE');
        began = true;
        const r = fn();
        this.db.exec('COMMIT');
        return r;
      } catch (err) {
        if (began) {
          try { this.db.exec('ROLLBACK'); } catch { /* 已自动回滚或连接异常 */ }
        }
        if (!isBusyError(err) || attempt >= this.maxRetries) throw err;
        const delay = this._backoffDelay(attempt);
        console.warn(
          `[db] 事务忙锁，第 ${attempt + 1}/${this.maxRetries} 次重试，等待 ${delay}ms`
        );
        await sleep(delay);
        attempt++;
      }
    }
  }

  // ---------- 用户 ----------

  async createUser(id, name, tokenRandom) {
    return this._tx(() => {
      this.stmt.insertUser.run(id, name, tokenRandom, now());
      return this.stmt.userById.get(id);
    });
  }

  async getUserByName(name) {
    return this._withRetry('getUserByName', () => this.stmt.userByName.get(name));
  }
  async getUserById(id) {
    return this._withRetry('getUserById', () => this.stmt.userById.get(id));
  }

  // ---------- 房间与成员 ----------

  async createRoom(id, name, creatorId) {
    return this._tx(() => {
      this.stmt.insertRoom.run(id, name, creatorId, now());
      // 创建者即管理员
      this.stmt.upsertMember.run(id, creatorId, 'admin', now());
      return this.stmt.roomById.get(id);
    });
  }

  async getRoom(id) {
    return this._withRetry('getRoom', () => this.stmt.roomById.get(id));
  }
  async getRoomByName(name) {
    return this._withRetry('getRoomByName', () => this.stmt.roomByName.get(name));
  }
  async listRoomsForUser(userId) {
    return this._withRetry('listRoomsForUser', () => this.stmt.roomsForUser.all(userId));
  }
  async listMembers(roomId) {
    return this._withRetry('listMembers', () => this.stmt.membersOfRoom.all(roomId));
  }

  async joinRoom(roomId, userId) {
    return this._tx(() => {
      this.stmt.upsertMember.run(roomId, userId, 'member', now());
      return this.stmt.member.get(roomId, userId);
    });
  }

  async getMember(roomId, userId) {
    return this._withRetry('getMember', () => this.stmt.member.get(roomId, userId));
  }

  /** 设置禁言截止时间（0 表示解除禁言） */
  async setMuted(roomId, userId, mutedUntil) {
    return this._tx(() => {
      this.stmt.setMuted.run(mutedUntil, roomId, userId);
      return this.stmt.member.get(roomId, userId);
    });
  }

  // ---------- 消息 ----------

  /**
   * 幂等写入消息。
   * 返回 { message, duplicate }：
   *  - duplicate=false：新消息，已分配 seq 并落库（调用方负责广播）；
   *  - duplicate=true ：同 clientMsgId 的消息已存在，直接返回原消息（调用方只回 ACK，不再广播）。
   */
  async insertMessage({ roomId, clientMsgId, senderId, content }) {
    return this._tx(() => {
      const existing = this.stmt.msgByClientId.get(roomId, senderId, clientMsgId);
      if (existing) return { message: existing, duplicate: true };

      const { last_seq: seq } = this.stmt.bumpSeq.get(roomId);
      const ts = now();
      this.stmt.insertMsg.run(roomId, seq, clientMsgId, senderId, content, ts);
      const message = this.stmt.msgByClientId.get(roomId, senderId, clientMsgId);
      return { message, duplicate: false };
    });
  }

  /** 断线补发：取 seq > afterSeq 的消息（升序，最多 limit 条） */
  async getMessagesAfter(roomId, afterSeq, limit) {
    return this._withRetry('getMessagesAfter', () =>
      this.stmt.msgsAfter.all(roomId, afterSeq, limit)
    );
  }

  /** 历史翻页：取 seq < beforeSeq 的消息，返回时按升序排列 */
  async getMessagesBefore(roomId, beforeSeq, limit) {
    const rows = await this._withRetry('getMessagesBefore', () =>
      this.stmt.msgsBefore.all(roomId, beforeSeq, limit)
    );
    return rows.reverse();
  }

  // ---------- 游标 ----------

  async saveCursor(roomId, userId, lastAckSeq) {
    // UPSERT 幂等（仅向前推进），忙锁时可安全重试
    await this._withRetry('saveCursor', () =>
      this.stmt.upsertCursor.run(roomId, userId, lastAckSeq, now())
    );
  }

  async getCursor(roomId, userId) {
    const row = await this._withRetry('getCursor', () =>
      this.stmt.cursor.get(roomId, userId)
    );
    return row ? row.lastAckSeq : 0;
  }

  close() {
    try { this.db.exec('PRAGMA wal_checkpoint(TRUNCATE)'); } catch { /* 内存库无 WAL */ }
    this.db.close();
  }
}

module.exports = { ChatDB };
