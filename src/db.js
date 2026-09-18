'use strict';

const { DatabaseSync } = require('node:sqlite');
const { now } = require('./util');

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
 * 并发与忙锁（SQLITE_BUSY）处理：
 * - node:sqlite 为同步驱动，读事务在 WAL 下不阻塞其他读/写，但多个写事务相互串行。
 *   外部连接（checkpoint、备份等）持锁，或同进程同步执行重叠时，写操作可能遇到忙锁。
 * - 所有写操作经单条「写队列」串行化（排队本身不占用数据库锁），消除同进程写-写
 *   忙锁；读操作直接执行、不进队列，写事务退避期间读操作仍可穿插，避免同步阻塞。
 * - 关键写操作在遇到忙锁时做有限次指数退避重试（busy_timeout 同步等待之外再异步兜底），
 *   重试期间事件循环可处理其他连接的读写；耗尽后抛 BusyError 由上层转可重试错误。
 */

/** node:sqlite 把忙锁与约束错误都归到 ERR_SQLITE_ERROR，只能按消息文本区分。 */
function isBusyError(err) {
  return !!err && /database is (locked|busy)/i.test(String(err && err.message ? err.message : ''));
}

/** 忙锁重试耗尽后抛出：上层映射为可重试的 SERVICE_BUSY，而非笼统 INTERNAL。 */
class BusyError extends Error {
  constructor(message) {
    super(message);
    this.name = 'BusyError';
    this.code = 'SQLITE_BUSY';
  }
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

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

class ChatDB {
  constructor(dbPath, options = {}) {
    this.busyTimeoutMs = options.busyTimeoutMs ?? 5000;
    this.writeRetries = options.writeRetries ?? 5; // 额外重试次数（不含首次）
    this.writeRetryBaseMs = options.writeRetryBaseMs ?? 5; // 指数退避基数：5,10,20,...
    this.db = new DatabaseSync(dbPath);
    // busy_timeout 让 SQLite 遇忙锁时先在驱动内等待（同步），再由异步重试兜底
    this.db.exec(`PRAGMA busy_timeout = ${this.busyTimeoutMs};`);
    this.db.exec(SCHEMA);
    this._prepare();
    this._writeTail = Promise.resolve(); // 写串行队列
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

  /**
   * 在 IMMEDIATE 事务中同步执行 fn（只做最小必要的 SQL），失败回滚。
   * 调用方需确保不与其他写事务并发（经 _enqueueWrite 排队）。
   */
  _tx(fn) {
    this.db.exec('BEGIN IMMEDIATE');
    try {
      const r = fn();
      this.db.exec('COMMIT');
      return r;
    } catch (err) {
      try { this.db.exec('ROLLBACK'); } catch { /* 已回滚 */ }
      throw err;
    }
  }

  /**
   * 把写操作排进单条 FIFO 队列，保证同进程写事务互不重叠（排队不占库锁）。
   * 入队立即返回 Promise，事件循环可继续处理其他连接的读/写请求。
   */
  _enqueueWrite(task) {
    const run = this._writeTail.then(task, task);
    // 单个任务失败不影响队列链后续任务
    this._writeTail = run.then(() => {}, () => {});
    return run;
  }

  /**
   * 带有限重试地执行一次写尝试。仅对忙锁（SQLITE_BUSY）做指数退避重试；
   * 约束错误等立即抛出。退避 setTimeout 期间让出事件循环，不阻塞其他连接。
   */
  async _withBusyRetry(label, attempt) {
    let lastErr;
    for (let i = 0; i <= this.writeRetries; i++) {
      try {
        return attempt();
      } catch (err) {
        lastErr = err;
        if (!isBusyError(err) || i === this.writeRetries) break;
        await sleep(this.writeRetryBaseMs * 2 ** i); // 5,10,20,40,80ms
      }
    }
    throw new BusyError(`${label} failed after ${this.writeRetries + 1} attempts: ${lastErr.message}`);
  }

  // ---------- 用户 ----------

  createUser(id, name, tokenRandom) {
    return this._enqueueWrite(() =>
      this._withBusyRetry('createUser', () => {
        this.stmt.insertUser.run(id, name, tokenRandom, now());
        return this.stmt.userById.get(id);
      })
    );
  }

  getUserByName(name) { return this.stmt.userByName.get(name); }
  getUserById(id) { return this.stmt.userById.get(id); }

  // ---------- 房间与成员 ----------

  createRoom(id, name, creatorId) {
    return this._enqueueWrite(() =>
      // 事务保持最小：仅插入房间与成员；roomById 读放到提交后
      this._withBusyRetry('createRoom', () =>
        this._tx(() => {
          this.stmt.insertRoom.run(id, name, creatorId, now());
          // 创建者即管理员
          this.stmt.upsertMember.run(id, creatorId, 'admin', now());
        })
      ).then(() => this.stmt.roomById.get(id))
    );
  }

  getRoom(id) { return this.stmt.roomById.get(id); }
  getRoomByName(name) { return this.stmt.roomByName.get(name); }
  listRoomsForUser(userId) { return this.stmt.roomsForUser.all(userId); }
  listMembers(roomId) { return this.stmt.membersOfRoom.all(roomId); }

  joinRoom(roomId, userId) {
    return this._enqueueWrite(() =>
      this._withBusyRetry('joinRoom', () => {
        this.stmt.upsertMember.run(roomId, userId, 'member', now());
        return this.stmt.member.get(roomId, userId);
      })
    );
  }

  getMember(roomId, userId) { return this.stmt.member.get(roomId, userId); }

  /** 设置禁言截止时间（0 表示解除禁言） */
  setMuted(roomId, userId, mutedUntil) {
    return this._enqueueWrite(() =>
      this._withBusyRetry('setMuted', () => {
        this.stmt.setMuted.run(mutedUntil, roomId, userId);
        return this.stmt.member.get(roomId, userId);
      })
    );
  }

  // ---------- 消息 ----------

  /**
   * 幂等写入消息。
   * 返回 { message, duplicate }：
   *  - duplicate=false：新消息，已分配 seq 并落库（调用方负责广播）；
   *  - duplicate=true ：同 clientMsgId 的消息已存在，直接返回原消息（调用方只回 ACK，不再广播）。
   *
   * 事务范围缩到最小：事务内只做「复查 + 分配 seq + 插入」；
   * 组装下发帧所需的 JOIN 查询在提交后执行（读不占写锁），缩短持锁时间。
   */
  insertMessage({ roomId, clientMsgId, senderId, content }) {
    return this._enqueueWrite(() =>
      this._withBusyRetry('insertMessage', () => {
        // 事务外先做一次幂等查重（读）：重复提交直接返回，不开启写事务
        const existing = this.stmt.msgByClientId.get(roomId, senderId, clientMsgId);
        if (existing) return { duplicate: true, message: existing };

        // 事务最小化：仅「复查 + 分配 seq + 插入」
        const r = this._tx(() => {
          // 事务内复查：串行队列下虽不会并发，保留以防外部连接已写入
          if (this.stmt.msgByClientId.get(roomId, senderId, clientMsgId)) {
            return { duplicate: true };
          }
          const { last_seq: seq } = this.stmt.bumpSeq.get(roomId);
          const ts = now();
          this.stmt.insertMsg.run(roomId, seq, clientMsgId, senderId, content, ts);
          return { duplicate: false };
        });
        // 提交后再 JOIN 取组装字段（fromName 等），读不占写锁
        const message = this.stmt.msgByClientId.get(roomId, senderId, clientMsgId);
        return { duplicate: r.duplicate, message };
      })
    );
  }

  /** 断线补发：取 seq > afterSeq 的消息（升序，最多 limit 条）。WAL 下读不阻塞写，直接执行。 */
  getMessagesAfter(roomId, afterSeq, limit) {
    return this.stmt.msgsAfter.all(roomId, afterSeq, limit);
  }

  /** 历史翻页：取 seq < beforeSeq 的消息，返回时按升序排列 */
  getMessagesBefore(roomId, beforeSeq, limit) {
    return this.stmt.msgsBefore.all(roomId, beforeSeq, limit).reverse();
  }

  // ---------- 游标 ----------

  /** 游标为高频写入（每条 ACK 一次），经写队列串行 + 忙锁重试，避免与消息写互撞。 */
  saveCursor(roomId, userId, lastAckSeq) {
    return this._enqueueWrite(() =>
      this._withBusyRetry('saveCursor', () =>
        this.stmt.upsertCursor.run(roomId, userId, lastAckSeq, now())
      )
    );
  }

  getCursor(roomId, userId) {
    const row = this.stmt.cursor.get(roomId, userId);
    return row ? row.lastAckSeq : 0;
  }

  close() {
    // 等队列中的写落盘后再关闭，避免关闭时丢弃未完成写
    return this._writeTail.then(() => {
      try { this.db.exec('PRAGMA wal_checkpoint(TRUNCATE)'); } catch { /* 内存库无 WAL */ }
      this.db.close();
    });
  }
}

module.exports = { ChatDB, BusyError, isBusyError };
