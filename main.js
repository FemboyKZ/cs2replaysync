#!/usr/bin/env node

const { spawn } = require('child_process');
const fs = require('fs').promises;
const path = require('path');
const os = require('os');
require("dotenv").config();

// Configuration from environment
const CONFIG = {
  localPath: process.env.LOCAL_PATH || '/data/replays',
  retentionMinutes: parseInt(process.env.RETENTION_MINUTES || '2880', 10),
  syncIntervalMs: parseInt(process.env.SYNC_INTERVAL_MS || '60000', 10),
  fileRetryCount: parseInt(process.env.FILE_RETRY_COUNT || '3', 10),
  fileRetryDelayMs: parseInt(process.env.FILE_RETRY_DELAY_MS || '1000', 10),
  statsResetIntervalMs: parseInt(process.env.STATS_RESET_INTERVAL_MS || String(24 * 60 * 60 * 1000), 10),
  lftp: {
    timeout: parseInt(process.env.LFTP_TIMEOUT || '10', 10),
    maxRetries: parseInt(process.env.LFTP_MAX_RETRIES || '3', 10),
    reconnectInterval: parseInt(process.env.LFTP_RECONNECT_INTERVAL || '5', 10),
  },
};

const UUID_REGEX = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

class Mutex {
  constructor() {
    this._locked = false;
    this._waiting = [];
  }

  async acquire() {
    return new Promise((resolve) => {
      if (!this._locked) {
        this._locked = true;
        resolve();
      } else {
        this._waiting.push(resolve);
      }
    });
  }

  release() {
    if (this._waiting.length > 0) {
      const next = this._waiting.shift();
      next();
    } else {
      this._locked = false;
    }
  }

  async withLock(fn) {
    await this.acquire();
    try {
      return await fn();
    } finally {
      this.release();
    }
  }
}

const archiveIndexMutex = new Mutex();

function isValidUUID(UUID) {
  return UUID_REGEX.test(UUID);
}

// Format: REMOTE_1=protocol|user|password|host|path
function parseRemotesFromEnv() {
  const remotes = [];
  
  for (const [key, value] of Object.entries(process.env)) {
    if (key.startsWith('REMOTE_') && value) {
      const parts = value.split('|');
      if (parts.length === 5) {
        remotes.push({
          protocol: parts[0],
          user: parts[1],
          password: parts[2],
          host: parts[3],
          path: parts[4],
        });
      } else {
        console.error(`Invalid remote config for ${key}: expected 5 parts, got ${parts.length}`);
      }
    }
  }
  
  return remotes;
}

const REMOTES = parseRemotesFromEnv();
const hostStates = new Map();

class HostState {
  constructor(remote) {
    this.remote = remote;
    this.hostId = remote.host.replace(/[^a-zA-Z0-9._-]/g, '_');
    this.lastSync = null;
    this.lastError = null;
    this.syncInProgress = false;
    this.filesOnHost = new Set();
    this.stats = {
      totalSyncs: 0,
      successfulSyncs: 0,
      failedSyncs: 0,
      filesUploaded: 0,
      filesDownloaded: 0,
      filesDeleted: 0,
    };
  }
}

function log(message, hostId = 'MAIN') {
  const timestamp = new Date().toISOString().replace('T', ' ').substring(0, 19);
  console.log(`${timestamp} [${hostId}] - ${message}`);
}

function executeLftp(protocol, user, password, host, commands) {
  return new Promise((resolve, reject) => {
    const esc = s => String(s).replace(/\\/g, '\\\\').replace(/"/g, '\\"');
    const escPassword = s => String(s).replace(/'/g, "'\\''");

    let lftpConfig;
    if (protocol === 'sftp') {
      lftpConfig = `
set sftp:auto-confirm yes
set sftp:connect-program "ssh -o StrictHostKeyChecking=no"
set net:timeout ${CONFIG. lftp.timeout}
set net:max-retries ${CONFIG. lftp.maxRetries}
set net:reconnect-interval-base ${CONFIG.lftp.reconnectInterval}
user "${esc(user)}" '${escPassword(password)}'
${commands}
quit
`;
    } else {
      lftpConfig = `
set ftp:use-mdtm off
set ftp:ssl-allow no
set net:timeout ${CONFIG. lftp.timeout}
set net: max-retries ${CONFIG.lftp.maxRetries}
set net:reconnect-interval-base ${CONFIG.lftp.reconnectInterval}
user "${esc(user)}" '${escPassword(password)}'
${commands}
quit
`;
    }

    const lftp = spawn('lftp', [`${protocol}://${host}`], { stdio: ['pipe', 'pipe', 'pipe'] });

    let stdout = '';
    let stderr = '';
    lftp.stdout.on('data', d => stdout += d.toString());
    lftp.stderr.on('data', d => stderr += d.toString());

    lftp.on('close', code => {
      if (code === 0) resolve(stdout);
      else reject(new Error(`lftp exited with code ${code}: ${stderr}`));
    });
    lftp.on('error', reject);

    lftp.stdin.write(lftpConfig);
    lftp.stdin.end();
  });
}

function parseArchiveIndex(content) {
  const entries = new Map();
  const regex = /"([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})"\s*=\s*(\d+)/g;
  let match;
  while ((match = regex.exec(content)) !== null) {
    entries.set(match[1], parseInt(match[2], 10));
  }
  return entries;
}

function mergeArchiveIndices(indices) {
  const merged = new Map();
  for (const index of indices) {
    for (const [UUID, timestamp] of index.entries()) {
      if (!merged.has(UUID) || timestamp < merged.get(UUID)) {
        merged.set(UUID, timestamp);
      }
    }
  }
  return merged;
}

function formatArchiveIndex(index) {
  let content = `<!-- kv3 encoding:text:version{e21c7f3c-8a33-41c5-9977-a76d3a32aa0d} format:generic:version{7412167c-06e9-4698-aff2-e63eb59037e7} -->
{
`;
  for (const [UUID, timestamp] of index.entries()) {
    content += `\t"${UUID}" = ${timestamp}\n`;
  }
  content += '}\n';
  return content;
}

function getExpiredUUIDs(index) {
  const currentTime = Math.floor(Date.now() / 1000);
  const retentionSeconds = CONFIG.retentionMinutes * 60;
  const expired = new Set();
  for (const [UUID, timestamp] of index.entries()) {
    if (currentTime - timestamp >= retentionSeconds) {
      expired.add(UUID);
    }
  }
  return expired;
}

async function getLocalFiles() {
  const files = new Map();
  try {
    const entries = await fs.readdir(CONFIG.localPath);
    for (const entry of entries) {
      if (entry.endsWith('.replay')) {
        const UUID = entry.replace('.replay', '');
        if (!isValidUUID(UUID)) {
          log(`Skipping file with invalid UUID format: ${entry}`);
          continue;
        }
        const filePath = path.join(CONFIG.localPath, entry);
        try {
          const stat = await fs.stat(filePath);
          files.set(UUID, Math.floor(stat.mtimeMs / 1000));
        } catch (statErr) {
          log(`File disappeared or inaccessible: ${entry} - ${statErr.message}`);
        }
      }
    }
  } catch (err) {
    log(`Error reading local directory: ${err.message}`);
  }
  return files;
}

async function getRemoteFiles(hostState) {
  const { protocol, user, password, host, path: remotePath } = hostState.remote;
  const files = new Set();
  try {
    const output = await executeLftp(protocol, user, password, host, `cls -1 "${remotePath}"`);
    for (const line of output.split('\n')) {
      const trimmed = line.trim();
      if (trimmed.endsWith('.replay')) {
        const filename = path.basename(trimmed);
        const UUID = filename.replace('.replay', '');
        if (isValidUUID(UUID)) {
          files.add(UUID);
        } else {
          log(`Skipping remote file with invalid UUID format: ${filename}`, hostState.hostId);
        }
      }
    }
  } catch (err) {
    throw new Error(`Failed to get remote listing: ${err.message}`);
  }
  return files;
}

async function getRemoteFileSize(hostState, filename) {
  const { protocol, user, password, host, path: remotePath } = hostState.remote;
  try {
    // Use ls -la for exact byte size (cls --size returns KB on some FTP servers)
    // Must cd first, then ls
    const output = await executeLftp(protocol, user, password, host, 
      `cd "${remotePath}"
ls -la "${filename}"`);
    // Parse ls -la output: "-rw-r--r-- 1 user group SIZE date time filename"
    const parts = output.trim().split(/\s+/);
    if (parts.length >= 5) {
      const size = parseInt(parts[4], 10);
      if (!isNaN(size)) {
        return size;
      }
    }
    return null;
  } catch (err) {
    return null;
  }
}

async function getLocalFileSize(filePath) {
  try {
    const stat = await fs.stat(filePath);
    return stat.size;
  } catch (err) {
    return null;
  }
}

async function downloadArchiveIndex(hostState) {
  const { protocol, user, password, host, path: remotePath } = hostState.remote;
  const tempFile = path.join(os.tmpdir(), `archive_index_${hostState.hostId}.txt`);
  try {
    await executeLftp(protocol, user, password, host, 
      `get "${remotePath}/archive_index.txt" -o "${tempFile}"`);
    const content = await fs.readFile(tempFile, 'utf8');
    await fs.unlink(tempFile).catch(() => {});
    return parseArchiveIndex(content);
  } catch (err) {
    log(`No archive index on ${host} or download failed`, hostState.hostId);
    return new Map();
  }
}

async function withRetry(fn, retries = CONFIG.fileRetryCount, delayMs = CONFIG.fileRetryDelayMs, context = '') {
  let lastError;
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      return await fn();
    } catch (err) {
      lastError = err;
      if (attempt < retries) {
        log(`${context} attempt ${attempt} failed, retrying in ${delayMs}ms: ${err.message}`);
        await new Promise(resolve => setTimeout(resolve, delayMs));
      }
    }
  }
  throw lastError;
}

async function uploadFiles(hostState, UUIDs) {
  if (UUIDs.length === 0) return 0;
  const { protocol, user, password, host, path: remotePath } = hostState.remote;
  
  let successCount = 0;
  for (const UUID of UUIDs) {
    if (!isValidUUID(UUID)) {
      log(`Skipping upload of invalid UUID: ${UUID}`, hostState.hostId);
      continue;
    }
    
    const localFilePath = path.join(CONFIG.localPath, `${UUID}.replay`);
    const localSize = await getLocalFileSize(localFilePath);
    
    if (localSize === null) {
      log(`Cannot read local file for upload: ${UUID}`, hostState.hostId);
      continue;
    }
    
    try {
      await withRetry(async () => {
        const commands = `cd "${remotePath}"\nput "${localFilePath}"`;
        await executeLftp(protocol, user, password, host, commands);

        const remoteSize = await getRemoteFileSize(hostState, `${UUID}.replay`);
        if (remoteSize === null) {
          throw new Error('Failed to verify remote file after upload');
        }
        if (remoteSize !== localSize) {
          try {
            await executeLftp(protocol, user, password, host, `rm -f "${remotePath}/${UUID}.replay"`);
          } catch (cleanupErr) {
          }
          throw new Error(`Size mismatch after upload: local=${localSize}, remote=${remoteSize}`);
        }
      }, CONFIG.fileRetryCount, CONFIG.fileRetryDelayMs, `Upload ${UUID}`);
      successCount++;
    } catch (err) {
      log(`Failed to upload ${UUID} after ${CONFIG.fileRetryCount} attempts: ${err.message}`, hostState.hostId);
    }
  }
  
  if (successCount > 0) {
    log(`Uploaded ${successCount} files`, hostState.hostId);
  }
  return successCount;
}

async function downloadFiles(hostState, UUIDs) {
  if (UUIDs.length === 0) return 0;
  const { protocol, user, password, host, path: remotePath } = hostState.remote;
  const tempDir = path.join(os.tmpdir(), `downloads_${hostState.hostId}`);
  let downloaded = 0;
  
  try {
    await fs.mkdir(tempDir, { recursive: true });
    
    for (const UUID of UUIDs) {
      if (!isValidUUID(UUID)) {
        log(`Skipping download of invalid UUID: ${UUID}`, hostState.hostId);
        continue;
      }
      
      // Get remote size for verification (optional - don't skip if unavailable)
      const remoteSize = await getRemoteFileSize(hostState, `${UUID}.replay`);
      
      const tempFilePath = path.join(tempDir, `${UUID}.replay`);
      
      try {
        await withRetry(async () => {
          await fs.unlink(tempFilePath).catch(() => {});
          
          const commands = `cd "${remotePath}"\nget "${UUID}.replay" -o "${tempFilePath}"`;
          await executeLftp(protocol, user, password, host, commands);
          
          const downloadedSize = await getLocalFileSize(tempFilePath);
          if (downloadedSize === null) {
            throw new Error('Downloaded file not found or inaccessible');
          }
          if (downloadedSize === 0) {
            await fs.unlink(tempFilePath).catch(() => {});
            throw new Error('Downloaded file is empty');
          }
          // Only verify size if we could determine remote size beforehand
          if (remoteSize !== null && downloadedSize !== remoteSize) {
            await fs.unlink(tempFilePath).catch(() => {});
            throw new Error(`Size mismatch after download: expected=${remoteSize}, got=${downloadedSize}`);
          }
        }, CONFIG.fileRetryCount, CONFIG.fileRetryDelayMs, `Download ${UUID}`);
        
        try {
          const destPath = path.join(CONFIG.localPath, `${UUID}.replay`);
          // Use copyFile + unlink instead of rename to handle cross-device moves
          await fs.copyFile(tempFilePath, destPath);
          
          const tempSize = await getLocalFileSize(tempFilePath);
          const finalSize = await getLocalFileSize(destPath);
          if (finalSize !== tempSize) {
            await fs.unlink(destPath).catch(() => {});
            throw new Error(`Final copy size mismatch: temp=${tempSize}, final=${finalSize}`);
          }
          
          await fs.chmod(destPath, 0o644);
          await fs.unlink(tempFilePath).catch(() => {});
          downloaded++;
        } catch (err) {
          log(`Failed to move ${UUID}: ${err.message}`, hostState.hostId);
        }
      } catch (err) {
        log(`Failed to download ${UUID} after ${CONFIG.fileRetryCount} attempts: ${err.message}`, hostState.hostId);
        await fs.unlink(tempFilePath).catch(() => {});
      }
    }
    
    if (downloaded > 0) {
      log(`Downloaded ${downloaded} files`, hostState.hostId);
    }
  } catch (err) {
    log(`Error setting up download: ${err.message}`, hostState.hostId);
  } finally {
    await fs.rm(tempDir, { recursive: true, force: true }).catch(() => {});
  }
  return downloaded;
}

async function deleteRemoteFiles(hostState, UUIDs) {
  if (UUIDs.length === 0) return 0;
  const { protocol, user, password, host, path: remotePath } = hostState.remote;
  
  let commands = '';
  for (const UUID of UUIDs) {
    commands += `rm -f "${remotePath}/${UUID}.replay"\n`;
  }
  
  try {
    await executeLftp(protocol, user, password, host, commands);
    log(`Deleted ${UUIDs.length} expired files`, hostState.hostId);
    return UUIDs.length;
  } catch (err) {
    log(`Error deleting files: ${err.message}`, hostState.hostId);
    return 0;
  }
}

async function uploadArchiveIndex(hostState, index) {
  const { protocol, user, password, host, path: remotePath } = hostState.remote;
  const tempFile = path.join(os.tmpdir(), `archive_index_upload_${hostState.hostId}.txt`);
  
  await archiveIndexMutex.withLock(async () => {
    try {
      await fs.writeFile(tempFile, formatArchiveIndex(index));
      await executeLftp(protocol, user, password, host, 
        `put "${tempFile}" -o "${remotePath}/archive_index.txt"`);
      await fs.unlink(tempFile).catch(() => {});
      log('Uploaded archive index', hostState.hostId);
    } catch (err) {
      log(`Error uploading archive index: ${err.message}`, hostState.hostId);
      await fs.unlink(tempFile).catch(() => {});
    }
  });
}

async function syncHost(hostState) {
  if (hostState.syncInProgress) {
    log('Sync already in progress, skipping', hostState.hostId);
    return;
  }
  
  hostState.syncInProgress = true;
  hostState.stats.totalSyncs++;
  const startTime = Date.now();
  log('Starting sync...', hostState.hostId);
  
  try {
    const localFiles = await getLocalFiles();
    const localUUIDs = new Set(localFiles.keys());
    const remoteFiles = await getRemoteFiles(hostState);
    hostState.filesOnHost = remoteFiles;
    
    const localIndexPath = path.join(CONFIG.localPath, 'archive_index.txt');
    let localIndex = new Map();
    await archiveIndexMutex.withLock(async () => {
      try {
        const content = await fs.readFile(localIndexPath, 'utf8');
        localIndex = parseArchiveIndex(content);
      } catch (err) {}
    });
    
    const remoteIndex = await downloadArchiveIndex(hostState);
    const mergedIndex = mergeArchiveIndices([localIndex, remoteIndex]);
    const expiredUUIDs = getExpiredUUIDs(mergedIndex);
    
    log(`Local: ${localUUIDs.size}, Remote: ${remoteFiles.size}, Expired: ${expiredUUIDs.size}`, hostState.hostId);
    
    const toDownload = [...remoteFiles].filter(UUID => !localUUIDs.has(UUID) && !expiredUUIDs.has(UUID));
    const toUpload = [...localUUIDs].filter(UUID => !remoteFiles.has(UUID) && !expiredUUIDs.has(UUID));
    const toDeleteRemote = [...remoteFiles].filter(UUID => expiredUUIDs.has(UUID));
    const toDeleteLocal = [...localUUIDs].filter(UUID => expiredUUIDs.has(UUID));
    
    if (toDownload.length > 0) {
      hostState.stats.filesDownloaded += await downloadFiles(hostState, toDownload);
    }
    if (toUpload.length > 0) {
      hostState.stats.filesUploaded += await uploadFiles(hostState, toUpload);
    }
    if (toDeleteRemote.length > 0) {
      hostState.stats.filesDeleted += await deleteRemoteFiles(hostState, toDeleteRemote);
    }
    
    for (const UUID of toDeleteLocal) {
      try {
        await fs.unlink(path.join(CONFIG.localPath, `${UUID}.replay`));
        log(`Deleted expired local: ${UUID}`, hostState.hostId);
      } catch (err) {}
    }
    
    await archiveIndexMutex.withLock(async () => {
      await uploadArchiveIndex(hostState, mergedIndex);
      await fs.writeFile(localIndexPath, formatArchiveIndex(mergedIndex));
    });
    
    hostState.lastSync = new Date();
    hostState.lastError = null;
    hostState.stats.successfulSyncs++;
    
    log(`Sync completed in ${((Date.now() - startTime) / 1000).toFixed(2)}s`, hostState.hostId);
  } catch (err) {
    hostState.lastError = err.message;
    hostState.stats.failedSyncs++;
    log(`Sync failed: ${err.message}`, hostState.hostId);
  } finally {
    hostState.syncInProgress = false;
  }
}

function getStatus() {
  return {
    timestamp: new Date().toISOString(),
    hosts: [...hostStates.values()].map(state => ({
      hostId: state.hostId,
      host: state.remote.host,
      lastSync: state.lastSync?.toISOString() || null,
      lastError: state.lastError,
      syncInProgress: state.syncInProgress,
      filesOnHost: state.filesOnHost.size,
      stats: { ...state.stats },
    })),
  };
}

function resetStats() {
  log('=== RESETTING STATS ===');
  for (const state of hostStates.values()) {
    state.stats = {
      totalSyncs: 0,
      successfulSyncs: 0,
      failedSyncs: 0,
      filesUploaded: 0,
      filesDownloaded: 0,
      filesDeleted: 0,
    };
    log(`Stats reset for ${state.remote.host}`, state.hostId);
  }
}

function printStatus() {
  log('=== HOST STATUS ===');
  for (const state of hostStates.values()) {
    log(`${state.remote.host}: Files=${state.filesOnHost.size}, ` +
        `Syncs=${state.stats.successfulSyncs}/${state.stats.totalSyncs}, ` +
        `Up=${state.stats.filesUploaded}, Down=${state.stats.filesDownloaded}, ` +
        `Del=${state.stats.filesDeleted}` +
        (state.lastError ? ` [ERROR: ${state.lastError}]` : ''));
  }
}

async function main() {
  log('Starting CS2 Replay Sync Service');
  log(`Local path: ${CONFIG.localPath}`);
  log(`Retention: ${CONFIG.retentionMinutes} minutes`);
  log(`Sync interval: ${CONFIG.syncIntervalMs / 1000} seconds`);
  log(`Configured hosts: ${REMOTES.length}`);
  
  await fs.mkdir(CONFIG.localPath, { recursive: true }).catch(() => {});
  
  for (const remote of REMOTES) {
    const state = new HostState(remote);
    hostStates.set(state.hostId, state);
    log(`Added host: ${remote.host}`, state.hostId);
  }
  
  if (REMOTES.length === 0) {
    log('No remote hosts configured. Set REMOTE_1, REMOTE_2, etc. environment variables.');
    log('Format: protocol|user|password|host|path');
    process.exit(1);
  }
  
  log('Running initial sync for all hosts...');
  await Promise.all([...hostStates.values()].map(state => syncHost(state)));
  printStatus();
  
  for (const state of hostStates.values()) {
    setInterval(() => syncHost(state), CONFIG.syncIntervalMs);
  }
  
  setInterval(printStatus, 5 * 60 * 1000);
  
  setInterval(resetStats, CONFIG.statsResetIntervalMs);
  log(`Stats will reset every ${CONFIG.statsResetIntervalMs / 1000 / 60 / 60} hours`);
  
  log('Sync service running. Press Ctrl+C to stop.');
  
  process.on('SIGINT', () => { log('Shutting down...'); process.exit(0); });
  process.on('SIGTERM', () => { log('Shutting down...'); process.exit(0); });
  
  await new Promise(() => {});
}

module.exports = { syncHost, getStatus, resetStats, hostStates, CONFIG, REMOTES, isValidUUID };

if (require.main === module) {
  main().catch((err) => { console.error('Fatal error:', err); process.exit(1); });
}
