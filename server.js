require('dotenv').config();
const express = require('express');
const path = require('path');
const fs = require('fs');
const zlib = require('zlib');
const Client = require('ssh2-sftp-client');
const crypto = require('crypto');
const OpenAI = require('openai');

const app = express();
const PORT = process.env.PORT || 3000;

// SFTP-Zugangsdaten werden über Umgebungsvariablen gesetzt, damit sie nicht im Code/Repository liegen.
// Notwendige Variablen:
//   SFTP_HOST, SFTP_PORT (optional, Standard 22), SFTP_USERNAME, SFTP_PASSWORD
const sftpConfig = {
  host: process.env.SFTP_HOST,
  port: process.env.SFTP_PORT ? Number(process.env.SFTP_PORT) : 22,
  username: process.env.SFTP_USERNAME,
  password: process.env.SFTP_PASSWORD,
};

// Remote-Verzeichnis auf dem SFTP-Server, aus dem .gz-Dateien geholt werden
const remoteDirectory = '/logs';

// Lokales Zielverzeichnis (relativ zum Projektordner)
const localDirectory = path.join(__dirname, 'downloads');
// Unterverzeichnis für entpackte Dateien
const extractedDirectory = path.join(localDirectory, 'unzipped');
// Metadatei für bereits verarbeitete Downloads
const downloadMetaFile = path.join(localDirectory, 'download-meta.json');
// Datei für tägliche Besuchsstatistiken
const dailyStatsFile = path.join(extractedDirectory, 'daily-stats.json');

function loadDownloadMeta() {
  try {
    if (!fs.existsSync(downloadMetaFile)) {
      return { files: {} };
    }
    const raw = fs.readFileSync(downloadMetaFile, 'utf8');
    const parsed = JSON.parse(raw);
    if (!parsed || typeof parsed !== 'object') {
      return { files: {} };
    }
    if (!parsed.files || typeof parsed.files !== 'object') {
      parsed.files = {};
    }
    return parsed;
  } catch (e) {
    console.error('Konnte Download-Metadatei nicht lesen, starte neu:', e);
    return { files: {} };
  }
}

function saveDownloadMeta(meta) {
  try {
    if (!fs.existsSync(localDirectory)) {
      fs.mkdirSync(localDirectory, { recursive: true });
    }
    fs.writeFileSync(downloadMetaFile, JSON.stringify(meta, null, 2), 'utf8');
  } catch (e) {
    console.error('Konnte Download-Metadatei nicht schreiben:', e);
  }
}

function loadDailyStats() {
  try {
    if (!fs.existsSync(dailyStatsFile)) {
      return {};
    }
    const raw = fs.readFileSync(dailyStatsFile, 'utf8');
    const parsed = JSON.parse(raw);
    if (!parsed || typeof parsed !== 'object') {
      return {};
    }
    return parsed;
  } catch (e) {
    console.error('Konnte tägliche Statistikdatei nicht lesen, starte neu:', e);
    return {};
  }
}

function saveDailyStats(stats) {
  try {
    if (!fs.existsSync(extractedDirectory)) {
      fs.mkdirSync(extractedDirectory, { recursive: true });
    }
    fs.writeFileSync(dailyStatsFile, JSON.stringify(stats, null, 2), 'utf8');
  } catch (e) {
    console.error('Konnte tägliche Statistikdatei nicht schreiben:', e);
  }
}

function sha256File(filePath) {
  return new Promise((resolve, reject) => {
    const hash = crypto.createHash('sha256');
    const stream = fs.createReadStream(filePath);
    stream.on('error', reject);
    stream.on('data', (chunk) => hash.update(chunk));
    stream.on('end', () => resolve(hash.digest('hex')));
  });
}

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// Health-Check: Prüfen, ob das Download-Verzeichnis ansprechbar ist
app.get('/download-dir-status', (req, res) => {
  try {
    if (!fs.existsSync(localDirectory)) {
      fs.mkdirSync(localDirectory, { recursive: true });
    }

    fs.access(localDirectory, fs.constants.R_OK | fs.constants.W_OK, (err) => {
      if (err) {
        console.error('Download-Verzeichnis nicht ansprechbar:', err);
        return res.status(500).json({
          success: false,
          message: 'Download-Verzeichnis ist nicht ansprechbar.',
          error: err.message,
        });
      }

      res.json({
        success: true,
        message: 'Download-Verzeichnis ist ansprechbar.',
      });
    });
  } catch (error) {
    console.error('Fehler beim Prüfen des Download-Verzeichnisses:', error);
    res.status(500).json({
      success: false,
      message: 'Fehler beim Prüfen des Download-Verzeichnisses.',
      error: error.message,
    });
  }
});

app.post('/download', async (req, res) => {
  // Fortschritt als Server-Sent Events (SSE) streamen
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.flushHeaders();

  function send(data) {
    res.write(`data: ${JSON.stringify(data)}\n\n`);
  }

  const sftp = new Client();

  try {
    const meta = loadDownloadMeta();

    send({ step: 'connect', label: 'Verbinde mit SFTP-Server…', pct: 1 });
    await sftp.connect(sftpConfig);

    send({ step: 'scan', label: 'Lese Dateiliste vom SFTP-Server…', pct: 3 });
    const fileList = await sftp.list(remoteDirectory);
    const gzFiles = fileList.filter((item) => {
      if (item.type !== '-') return false;
      const nameLower = item.name.toLowerCase();
      return nameLower.endsWith('.gz') && nameLower.startsWith('access.log');
    });

    const totalRemoteFiles = gzFiles.length;
    const totalRemoteBytes = gzFiles.reduce((sum, f) => {
      const size = typeof f.size === 'number' ? f.size : 0;
      return sum + size;
    }, 0);

    if (!fs.existsSync(localDirectory)) {
      fs.mkdirSync(localDirectory, { recursive: true });
    }

    const downloadedFiles = [];
    const skippedFiles = [];
    const localPaths = [];

    // Schritt 1: Archive herunterladen – 5 % → 55 %
    let downloadDone = 0;
    for (const file of gzFiles) {
      const key = file.name;
      const existing = meta.files && meta.files[key];
      const remoteSize = file.size;
      const remoteMtime = file.modifyTime;
      const pct = Math.round(2 + (downloadDone / Math.max(totalRemoteFiles, 1)) * 5);

      if (
        existing &&
        typeof existing.remoteSize === 'number' &&
        typeof existing.remoteMtime === 'number' &&
        existing.remoteSize === remoteSize &&
        existing.remoteMtime === remoteMtime
      ) {
        skippedFiles.push(key);
        downloadDone++;
        send({ step: 'download', label: `Überspringe (unverändert): ${key}  (${downloadDone}/${totalRemoteFiles})`, file: key, pct });
        continue;
      }

      send({ step: 'download', label: `Herunterladen: ${key}  (${downloadDone + 1}/${totalRemoteFiles})`, file: key, pct });
      const remotePath = `${remoteDirectory.replace(/\/+$/, '')}/${file.name}`;
      const localPath = path.join(localDirectory, file.name);

      await sftp.fastGet(remotePath, localPath);
      downloadedFiles.push(file.name);
      localPaths.push({ localPath, key, remoteSize, remoteMtime });
      downloadDone++;
    }

    await sftp.end();

    // Schritt 2: Entpacken – 55 % → 75 %
    const processedExtractedFiles = new Set();
    let processedBytes = 0;
    let skippedBytes = 0;

    for (const file of gzFiles) {
      if (skippedFiles.includes(file.name)) {
        const size = typeof file.size === 'number' ? file.size : 0;
        skippedBytes += size;
      }
    }

    const totalToExtract = localPaths.length;
    let extractDone = 0;
    for (const item of localPaths) {
      const { localPath, key, remoteSize, remoteMtime } = item;
      const pct = Math.round(7 + (extractDone / Math.max(totalToExtract, 1)) * 90);
      send({ step: 'extract', label: `Entpacke: ${key}  (${extractDone + 1}/${totalToExtract})`, file: key, pct });
      try {
        const extractedNames = await extractAndNormalizeLog(localPath);
        if (Array.isArray(extractedNames)) {
          extractedNames.forEach((n) => processedExtractedFiles.add(n));
        } else if (typeof extractedNames === 'string' && extractedNames) {
          processedExtractedFiles.add(extractedNames);
        }

        if (typeof remoteSize === 'number') {
          processedBytes += remoteSize;
        }

        try {
          const hash = await sha256File(localPath);
          meta.files[key] = {
            remotePath: `${remoteDirectory.replace(/\/+$/, '')}/${key}`,
            remoteSize,
            remoteMtime,
            hash,
            processedAt: new Date().toISOString(),
          };
        } catch (hashErr) {
          console.error('Konnte Hash für heruntergeladene Datei nicht berechnen:', hashErr);
        }
      } catch (e) {
        console.error('Fehler beim automatischen Entpacken/Normalisieren:', e);
      }
      extractDone++;
    }

    saveDownloadMeta(meta);

    // Schritt 3: Tagesstatistiken – 75 % → 98 %
    const dailyStats = loadDailyStats();
    const updatedDates = [];
    const allExtracted = Array.from(processedExtractedFiles);
    const totalDates = allExtracted.length;
    let dailyDone = 0;
    let lastDay = '';
    for (const fileName of allExtracted) {
      const pct = Math.round(97 + (dailyDone / Math.max(totalDates, 1)) * 2);
      send({ step: 'daily', label: `Analysiere: ${fileName}  (${dailyDone + 1}/${totalDates})`, file: fileName, pct, lastDay });
      try {
        const stats = await computeDailyStatsForFile(fileName);
        if (stats && stats.date) {
          dailyStats[stats.date] = stats;
          updatedDates.push(stats.date);
          lastDay = stats.date;
        }
      } catch (statsErr) {
        console.error('Fehler beim Berechnen der Tagesstatistik für', fileName, statsErr);
      }
      dailyDone++;
    }
    if (updatedDates.length > 0) {
      saveDailyStats(dailyStats);
    }

    const processedFilesTotal = downloadedFiles.length + skippedFiles.length;
    const processedBytesTotal = processedBytes + skippedBytes;

    send({
      step: 'done',
      label: 'Abgeschlossen',
      pct: 100,
      result: {
        success: true,
        message: 'Download und Verarbeitung abgeschlossen',
        count: downloadedFiles.length,
        files: downloadedFiles,
        skipped: skippedFiles,
        totalRemoteFiles,
        totalRemoteBytes,
        processedFiles: processedFilesTotal,
        processedBytes: processedBytesTotal,
        skippedBytes,
        extracted: Array.from(processedExtractedFiles),
        dailyStatsUpdated: updatedDates,
      },
    });
    res.end();
  } catch (error) {
    console.error('SFTP-Fehler:', error);
    try { await sftp.end(); } catch (_) {}
    send({ step: 'error', label: `Fehler: ${error.message}`, pct: 0 });
    res.end();
  }
});

// Hilfsfunktion: Entpacke eine heruntergeladene access.log-*.gz-Datei automatisch,
// teile sie nach Datum auf und führe pro Tag in eine Zieldatei zusammen.
async function extractAndNormalizeLog(localGzPath) {
  if (!fs.existsSync(localGzPath)) return null;

  if (!fs.existsSync(extractedDirectory)) {
    fs.mkdirSync(extractedDirectory, { recursive: true });
  }

  const gzBaseName = path.basename(localGzPath, '.gz');
  const tmpPath = path.join(extractedDirectory, `${gzBaseName}.tmp`);

  // 1) Entpacken in eine temporäre Datei
  await new Promise((resolve, reject) => {
    const readStream = fs.createReadStream(localGzPath);
    const gunzip = zlib.createGunzip();
    const writeStream = fs.createWriteStream(tmpPath);

    readStream
      .pipe(gunzip)
      .pipe(writeStream)
      .on('finish', resolve)
      .on('error', reject);
  });

  // 2) Inhalt nach Datum aufsplitten und pro Tag in eine Datei schreiben
  const readline = require('readline');
  const rl = readline.createInterface({
    input: fs.createReadStream(tmpPath, { encoding: 'utf8' }),
    crlfDelay: Infinity,
  });

  const logRegex = /^(\S+)\s+\S+\s+\S+\s+\[([^\]]+)\]\s+"(\S+)\s+([^\"]+)\s+\S+"\s+(\d{3})\s+\S+\s+(\S+)\s+"([^\"]*)"\s+"([^\"]*)"/;

  const monthMap = {
    Jan: '01', Feb: '02', Mar: '03', Apr: '04', May: '05', Jun: '06',
    Jul: '07', Aug: '08', Sep: '09', Oct: '10', Nov: '11', Dec: '12',
  };

  function toIsoFromApacheTime(str) {
    const m = /^(\d{2})\/(\w{3})\/(\d{4}):(\d{2}):(\d{2}):(\d{2}) ([+\-]\d{4})$/.exec(str);
    if (!m) return null;
    const [, dd, monStr, yyyy, hh, mm, ss, offset] = m;
    const mon = monthMap[monStr] || '01';
    const offH = offset.slice(0, 3);
    const offM = offset.slice(3);
    return `${yyyy}-${mon}-${dd}T${hh}:${mm}:${ss}${offH}:${offM}`;
  }

  const writtenDates = new Set();

  await new Promise((resolve) => {
    rl.on('line', (line) => {
      const match = logRegex.exec(line);
      if (!match) {
        return;
      }
      const timeRaw = match[2];
      const iso = toIsoFromApacheTime(timeRaw);
      if (!iso) {
        return;
      }
      const datePart = iso.slice(0, 10);
      const finalName = `access-${datePart}.log`;
      const finalPath = path.join(extractedDirectory, finalName);

      // Pro Tag eine Datei; bestehende Dateien werden nicht überschrieben,
      // sondern höchstens erweitert (append).
      const lineWithNewline = line.endsWith('\n') ? line : line + '\n';
      fs.appendFileSync(finalPath, lineWithNewline);
      writtenDates.add(finalName);
    });

    rl.on('close', () => {
      resolve();
    });
  });

  // Temporäre Datei entfernen
  try {
    fs.unlinkSync(tmpPath);
  } catch (_) {}

  // Rückgabe: alle erzeugten/erweiterten Dateien (oder null)
  return writtenDates.size ? Array.from(writtenDates) : null;
}

// Hilfsfunktion: Werte eine Tages-Logdatei (access-YYYY-MM-DD.log) aus
// und berechne Aufrufe/Besuche nach Typ.
async function computeDailyStatsForFile(fileName) {
  const sourcePath = path.join(extractedDirectory, fileName);

  if (!sourcePath.startsWith(extractedDirectory)) {
    return null;
  }

  if (!fs.existsSync(sourcePath)) {
    return null;
  }

  const readline = require('readline');
  const stream = fs.createReadStream(sourcePath, { encoding: 'utf8' });

  const rl = readline.createInterface({
    input: stream,
    crlfDelay: Infinity,
  });

  // Beispielzeile:
  // 82.38.56.0 - - [30/Mar/2026:00:02:29 +0200] "GET /aboutus HTTP/1.1" 301 273 hrv.de "https://hrv.de/aboutus" "UA" "-"
  const logRegex = /^(\S+)\s+\S+\s+\S+\s+\[([^\]]+)\]\s+"(\S+)\s+([^\"]+)\s+\S+"\s+(\d{3})\s+\S+\s+(\S+)\s+"([^\"]*)"\s+"([^\"]*)"/;

  const monthMap = {
    Jan: '01', Feb: '02', Mar: '03', Apr: '04', May: '05', Jun: '06',
    Jul: '07', Aug: '08', Sep: '09', Oct: '10', Nov: '11', Dec: '12',
  };

  function toIsoFromApacheTime(str) {
    const m = /^(\d{2})\/(\w{3})\/(\d{4}):(\d{2}):(\d{2}):(\d{2}) ([+\-]\d{4})$/.exec(str);
    if (!m) return str;
    const [, dd, monStr, yyyy, hh, mm, ss, offset] = m;
    const mon = monthMap[monStr] || '01';
    const offH = offset.slice(0, 3);
    const offM = offset.slice(3);
    return `${yyyy}-${mon}-${dd}T${hh}:${mm}:${ss}${offH}:${offM}`;
  }

  function classifyUserAgent(uaRaw) {
    const ua = (uaRaw || '').toLowerCase();
    if (!ua) return 'human';

    if (ua.includes('chatgpt') || ua.includes('gpt') || ua.includes('openai') || ua.includes('copilot') || ua.includes('oai-searchbot') || ua.includes('ai-')) {
      return 'ai';
    }

    if (/bot|crawl|spider|searchbot|crawler/.test(ua)) {
      return 'bot';
    }

    return 'human';
  }

  let totalLines = 0;
  const visitors = new Map();
  const MAX_VISITORS = 2000;
  let firstDateIsoFull = null;

  await new Promise((resolve, reject) => {
    rl.on('line', (line) => {
      totalLines += 1;

      const match = logRegex.exec(line);
      if (!match) return;

      const ip = match[1];
      const timeRaw = match[2];
      const method = match[3];
      const pathRequested = match[4];
      const status = match[5];
      const host = match[6];
      const userAgent = match[8];

      // Nur 200er berücksichtigen, 404/3xx ignorieren
      if (status !== '200') return;

      const url = host && host !== '-' ? `https://${host}${pathRequested}` : pathRequested;
      const timeIso = toIsoFromApacheTime(timeRaw);

      if (!firstDateIsoFull || timeIso < firstDateIsoFull) {
        firstDateIsoFull = timeIso;
      }

      const key = `${ip}|${userAgent || ''}`;
      let visitor = visitors.get(key);

      const currentType = classifyUserAgent(userAgent);

      if (!visitor) {
        if (visitors.size >= MAX_VISITORS) {
          return;
        }
        visitor = {
          ip,
          userAgent,
          firstTime: timeIso,
          lastTime: timeIso,
          lastUrl: url,
          hits: 1,
          type: currentType,
        };
      } else {
        visitor.lastTime = timeIso;
        visitor.lastUrl = url;
        visitor.hits += 1;
        if (visitor.type !== 'ai') {
          if (currentType === 'ai') {
            visitor.type = 'ai';
          } else if (visitor.type === 'human' && currentType === 'bot') {
            visitor.type = 'bot';
          }
        }
      }

      visitors.set(key, visitor);
    });

    rl.on('close', () => resolve());
    rl.on('error', (err) => reject(err));
    stream.on('error', (err) => reject(err));
  });

  const entries = Array.from(visitors.values());
  const firstDateIso = firstDateIsoFull ? firstDateIsoFull.slice(0, 10) : null;

  let visitsTotal = 0;
  let visitsBots = 0;
  let visitsAi = 0;
  let visitsHuman = 0;

  function calcSessionsForVisitor(v) {
    if (!v.firstTime || !v.lastTime) return 1;
    const start = Date.parse(v.firstTime);
    const end = Date.parse(v.lastTime);
    if (Number.isNaN(start) || Number.isNaN(end) || end <= start) return 1;
    const diffMinutes = (end - start) / 60000;
    return diffMinutes > 5 ? 2 : 1;
  }

  for (const v of entries) {
    const sessions = calcSessionsForVisitor(v);
    visitsTotal += sessions;

    if (v.type === 'ai') {
      visitsAi += sessions;
    } else if (v.type === 'bot') {
      visitsBots += sessions;
    } else {
      visitsHuman += sessions;
    }
  }

  const m = /^access-(\d{4}-\d{2}-\d{2})\.log$/.exec(fileName);
  const dateKey = m ? m[1] : firstDateIso || fileName;

  return {
    date: dateKey,
    lineCountApprox: totalLines,
    visitsTotal,
    visitsBots,
    visitsAi,
    visitsHuman,
  };
}

// Liste alle Dateien im lokalen downloads-Verzeichnis auf
app.get('/files', (req, res) => {
  try {
    if (!fs.existsSync(localDirectory)) {
      return res.json({ success: true, files: [] });
    }

    const files = fs.readdirSync(localDirectory)
      .filter((name) => {
        const fullPath = path.join(localDirectory, name);
        return fs.statSync(fullPath).isFile() && name.toLowerCase().endsWith('.gz');
      })
      .map((name) => {
        const st = fs.statSync(path.join(localDirectory, name));
        return { name, size: st.size, mtime: st.mtime.toISOString() };
      });

    res.json({ success: true, files });
  } catch (error) {
    console.error('Fehler beim Auflisten der Downloads:', error);
    res.status(500).json({
      success: false,
      message: 'Fehler beim Auflisten der Dateien im downloads-Verzeichnis',
      error: error.message,
    });
  }
});

// Liste nur entpackte Dateien (alles außer .gz) im downloads-Verzeichnis auf
app.get('/unzipped', (req, res) => {
  try {
    if (!fs.existsSync(extractedDirectory)) {
      return res.json({ success: true, files: [], total: 0 });
    }

    const offset = Math.max(0, parseInt(req.query.offset, 10) || 0);
    const limit  = Math.min(100, Math.max(1, parseInt(req.query.limit,  10) || 25));

    const allFiles = fs
      .readdirSync(extractedDirectory)
      .filter((name) => {
        const fullPath = path.join(extractedDirectory, name);
        return fs.statSync(fullPath).isFile() && name.toLowerCase().endsWith('.log');
      })
      .map((name) => {
        const st = fs.statSync(path.join(extractedDirectory, name));
        return { name, size: st.size, mtime: st.mtime.toISOString() };
      })
      .sort((a, b) => {
        // Datum aus Dateinamen extrahieren (z.B. access-2026-03-31.log)
        const dateA = a.name.match(/(\d{4}-\d{2}-\d{2})/)?.[1] ?? '';
        const dateB = b.name.match(/(\d{4}-\d{2}-\d{2})/)?.[1] ?? '';
        return dateB.localeCompare(dateA); // absteigend: neueste zuerst
      });

    const files = allFiles.slice(offset, offset + limit);
    res.json({ success: true, files, total: allFiles.length, offset, limit });
  } catch (error) {
    console.error('Fehler beim Auflisten der entpackten Dateien:', error);
    res.status(500).json({
      success: false,
      message: 'Fehler beim Auflisten der entpackten Dateien im Unterverzeichnis',
      error: error.message,
    });
  }
});

// Entpacke eine ausgewählte .gz-Datei aus dem downloads-Verzeichnis
app.post('/extract', (req, res) => {
  const { fileName } = req.body || {};

  if (!fileName || typeof fileName !== 'string') {
    return res.status(400).json({
      success: false,
      message: 'Es wurde keine gültige Datei angegeben',
    });
  }

  const sourcePath = path.join(localDirectory, fileName);

  if (!sourcePath.startsWith(localDirectory)) {
    return res.status(400).json({
      success: false,
      message: 'Ungültiger Dateiname',
    });
  }

  if (!fs.existsSync(sourcePath)) {
    return res.status(404).json({
      success: false,
      message: 'Datei wurde nicht gefunden',
    });
  }

  if (!fs.existsSync(extractedDirectory)) {
    fs.mkdirSync(extractedDirectory, { recursive: true });
  }

  // Ziel-Dateiname: gleich wie .gz-Datei, aber ohne .gz-Endung
  const targetFileName = fileName.toLowerCase().endsWith('.gz')
    ? fileName.slice(0, -3)
    : `${fileName}.unzipped`;
  const targetPath = path.join(extractedDirectory, targetFileName);

  const readStream = fs.createReadStream(sourcePath);
  const writeStream = fs.createWriteStream(targetPath);
  const gunzip = zlib.createGunzip();

  readStream
    .pipe(gunzip)
    .pipe(writeStream)
    .on('finish', () => {
      res.json({
        success: true,
        message: 'Datei erfolgreich entpackt',
        source: fileName,
        target: targetFileName,
      });
    })
    .on('error', (error) => {
      console.error('Fehler beim Entpacken:', error);
      res.status(500).json({
        success: false,
        message: 'Fehler beim Entpacken der Datei',
        error: error.message,
      });
    });
});

  // Analysiere eine ausgewählte entpackte Datei aus dem Unterverzeichnis
  app.post('/analyze', (req, res) => {
    const { fileName } = req.body || {};

    if (!fileName || typeof fileName !== 'string') {
      return res.status(400).json({
        success: false,
        message: 'Es wurde keine gültige Datei angegeben',
      });
    }

    const sourcePath = path.join(extractedDirectory, fileName);

    if (!sourcePath.startsWith(extractedDirectory)) {
      return res.status(400).json({
        success: false,
        message: 'Ungültiger Dateiname',
      });
    }

    if (!fs.existsSync(sourcePath)) {
      return res.status(404).json({
        success: false,
        message: 'Entpackte Datei wurde nicht gefunden',
      });
    }

    const MAX_CHARS = 5000; // Vorschau begrenzen

    const readline = require('readline');
    const stream = fs.createReadStream(sourcePath, { encoding: 'utf8' });

    const rl = readline.createInterface({
      input: stream,
      crlfDelay: Infinity,
    });

    let preview = '';
    let previewTruncated = false;
    let totalLines = 0;
    const visitors = new Map(); // key = IP+UA, Wert = zusammengefasste Daten
    const MAX_VISITORS = 2000;
    let firstDateIsoFull = null; // frühester Zeitstempel als vollständige ISO-Zeit

    // Beispielzeile:
    // 82.38.56.0 - - [30/Mar/2026:00:02:29 +0200] "GET /aboutus HTTP/1.1" 301 273 hrv.de "https://hrv.de/aboutus" "UA" "-"
    const logRegex = /^(\S+)\s+\S+\s+\S+\s+\[([^\]]+)\]\s+"(\S+)\s+([^\"]+)\s+\S+"\s+(\d{3})\s+\S+\s+(\S+)\s+"([^\"]*)"\s+"([^\"]*)"/;

    const monthMap = {
      Jan: '01', Feb: '02', Mar: '03', Apr: '04', May: '05', Jun: '06',
      Jul: '07', Aug: '08', Sep: '09', Oct: '10', Nov: '11', Dec: '12',
    };

    function toIsoFromApacheTime(str) {
      // z.B. "30/Mar/2026:00:02:29 +0200"
      const m = /^(\d{2})\/(\w{3})\/(\d{4}):(\d{2}):(\d{2}):(\d{2}) ([+\-]\d{4})$/.exec(str);
      if (!m) return str;
      const [, dd, monStr, yyyy, hh, mm, ss, offset] = m;
      const mon = monthMap[monStr] || '01';
      const offH = offset.slice(0, 3); // +02
      const offM = offset.slice(3);    // 00
      return `${yyyy}-${mon}-${dd}T${hh}:${mm}:${ss}${offH}:${offM}`;
    }

    function classifyUserAgent(uaRaw) {
      const ua = (uaRaw || '').toLowerCase();
      if (!ua) return 'human';

      // AI-Clients priorisieren, falls sowohl "bot" als auch AI-Muster vorkommen
      if (ua.includes('chatgpt') || ua.includes('gpt') || ua.includes('openai') || ua.includes('copilot') || ua.includes('oai-searchbot') || ua.includes('ai-')) {
        return 'ai';
      }

      if (/bot|crawl|spider|searchbot|crawler/.test(ua)) {
        return 'bot';
      }

      return 'human';
    }

    rl.on('line', (line) => {
      totalLines += 1;

      // Vorschau auf die ersten MAX_CHARS Zeichen aufbauen
      if (!previewTruncated) {
        if (preview.length + line.length + 1 <= MAX_CHARS) {
          preview += line + '\n';
        } else {
          const remaining = MAX_CHARS - preview.length;
          if (remaining > 0) {
            preview += line.slice(0, remaining);
          }
          previewTruncated = true;
        }
      }

      // Logzeile parsen: IP, Zeit, Methode, Pfad, Status, Host, Referrer, User-Agent
      const match = logRegex.exec(line);
      if (!match) return;

      const ip = match[1];
      const timeRaw = match[2];
      const method = match[3];
      const pathRequested = match[4];
      const status = match[5];
      const host = match[6];
      const userAgent = match[8];

      // Nur 200er berücksichtigen, 404/3xx ignorieren
      if (status !== '200') return;

      const url = host && host !== '-' ? `https://${host}${pathRequested}` : pathRequested;
      const timeIso = toIsoFromApacheTime(timeRaw);

      if (!firstDateIsoFull || timeIso < firstDateIsoFull) {
        firstDateIsoFull = timeIso;
      }

      const key = `${ip}|${userAgent || ''}`;
      let visitor = visitors.get(key);

      const currentType = classifyUserAgent(userAgent);

      if (!visitor) {
        if (visitors.size >= MAX_VISITORS) {
          // Bei sehr vielen unterschiedlichen Besuchern brechen wir still mit Aggregation ab
          return;
        }
        visitor = {
          ip,
          userAgent,
          firstTime: timeIso,
          lastTime: timeIso,
          lastUrl: url,
          hits: 1,
          type: currentType,
        };
      } else {
        visitor.lastTime = timeIso;
        visitor.lastUrl = url;
        visitor.hits += 1;
        // Typ ggf. hochstufen (human -> bot/ai, bot -> ai), aber nie von ai zurückstufen
        if (visitor.type !== 'ai') {
          if (currentType === 'ai') {
            visitor.type = 'ai';
          } else if (visitor.type === 'human' && currentType === 'bot') {
            visitor.type = 'bot';
          }
        }
      }

      visitors.set(key, visitor);
    });

    rl.on('close', () => {
      try {
        const entries = Array.from(visitors.values());
        const firstDateIso = firstDateIsoFull ? firstDateIsoFull.slice(0, 10) : null; // nur Datum

        let visitsTotal = 0;
        let visitsBots = 0;
        let visitsAi = 0;
        let visitsHuman = 0;

        function calcSessionsForVisitor(v) {
          if (!v.firstTime || !v.lastTime) return 1;
          const start = Date.parse(v.firstTime);
          const end = Date.parse(v.lastTime);
          if (Number.isNaN(start) || Number.isNaN(end) || end <= start) return 1;
          const diffMinutes = (end - start) / 60000;
          // Wenn mehr als 5 Minuten auseinander, als zwei Aufrufe zählen
          return diffMinutes > 5 ? 2 : 1;
        }

        for (const v of entries) {
          const sessions = calcSessionsForVisitor(v);
          visitsTotal += sessions;

          if (v.type === 'ai') {
            visitsAi += sessions;
          } else if (v.type === 'bot') {
            visitsBots += sessions;
          } else {
            visitsHuman += sessions;
          }
        }

        res.json({
          success: true,
          message: 'Analyse der entpackten Datei abgeschlossen',
          file: fileName,
          lineCountApprox: totalLines,
          preview,
          previewTruncated,
          visitsTotal,
          visitsBots,
          visitsAi,
          visitsHuman,
          firstDateIso,
          entries,
        });
      } catch (error) {
        console.error('Fehler bei der Analyse der entpackten Datei:', error);
        res.status(500).json({
          success: false,
          message: 'Fehler bei der Analyse der entpackten Datei',
          error: error.message,
        });
      }
    });

    rl.on('error', (error) => {
      console.error('Fehler beim Lesen der entpackten Datei für Analyse:', error);
      res.status(500).json({
        success: false,
        message: 'Fehler beim Lesen der entpackten Datei für die Analyse',
        error: error.message,
      });
    });
  });

// Rechne alle vorhandenen Tages-Logdateien neu in die daily-stats.json
app.post('/recompute-daily-stats', async (req, res) => {
  try {
    if (!fs.existsSync(extractedDirectory)) {
      return res.json({
        success: true,
        message: 'Kein Unterverzeichnis für entpackte Dateien vorhanden.',
        updatedDates: [],
        totalFiles: 0,
      });
    }

    const files = fs
      .readdirSync(extractedDirectory)
      .filter((name) => {
        const fullPath = path.join(extractedDirectory, name);
        return (
          fs.statSync(fullPath).isFile() &&
          name.toLowerCase().endsWith('.log') &&
          name.startsWith('access-')
        );
      });

    if (!files.length) {
      return res.json({
        success: true,
        message: 'Keine Tages-Logdateien zum Auswerten gefunden.',
        updatedDates: [],
        totalFiles: 0,
      });
    }

    const dailyStats = loadDailyStats();
    const updatedDates = [];

    for (const fileName of files) {
      try {
        const stats = await computeDailyStatsForFile(fileName);
        if (stats && stats.date) {
          dailyStats[stats.date] = stats;
          updatedDates.push(stats.date);
        }
      } catch (err) {
        console.error('Fehler beim Berechnen der Tagesstatistik für', fileName, err);
      }
    }

    if (updatedDates.length > 0) {
      saveDailyStats(dailyStats);
    }

    res.json({
      success: true,
      message: 'Tägliche Statistiken wurden neu berechnet.',
      updatedDates,
      totalFiles: files.length,
    });
  } catch (error) {
    console.error('Fehler beim Neuberechnen der täglichen Statistiken:', error);
    res.status(500).json({
      success: false,
      message: 'Fehler beim Neuberechnen der täglichen Statistiken',
      error: error.message,
    });
  }
});

// Liefert die letzten 30 Tage aus daily-stats.json (sortiert nach Datum)
app.get('/daily-stats-last30', (req, res) => {
  try {
    const stats = loadDailyStats();
    const allDates = Object.keys(stats || {})
      .filter((d) => /^\d{4}-\d{2}-\d{2}$/.test(d))
      .sort();

    const last30 = allDates.slice(-30);
    const days = last30.map((date) => {
      const s = stats[date] || {};
      return {
        date,
        lineCountApprox: s.lineCountApprox || 0,
        visitsTotal: s.visitsTotal || 0,
        visitsHuman: s.visitsHuman || 0,
        visitsBots: s.visitsBots || 0,
        visitsAi: s.visitsAi || 0,
      };
    });

    res.json({
      success: true,
      days,
    });
  } catch (error) {
    console.error('Fehler beim Lesen der letzten 30 Tagesstatistiken:', error);
    res.status(500).json({
      success: false,
      message: 'Fehler beim Lesen der letzten 30 Tagesstatistiken',
      error: error.message,
    });
  }
});

// KI-Analyse der letzten 90 Tage via GitHub Models (gpt-4o-mini)
app.post('/ai-analyze', async (req, res) => {
  const token = process.env.GITHUB_TOKEN;
  if (!token) {
    return res.status(500).json({ success: false, error: 'GITHUB_TOKEN nicht konfiguriert.' });
  }

  const dailyStats = loadDailyStats();
  const entries = Object.values(dailyStats)
    .filter((d) => d && d.date)
    .sort((a, b) => a.date.localeCompare(b.date))
    .slice(-90);

  if (!entries.length) {
    return res.status(400).json({ success: false, error: 'Keine Tagesstatistiken vorhanden. Bitte zuerst Dateien herunterladen und analysieren.' });
  }

  const statsJson = JSON.stringify(entries.map((d) => ({
    date: d.date,
    total: d.visitsTotal ?? 0,
    human: d.visitsHuman ?? 0,
    bot: d.visitsBots ?? 0,
    ai: d.visitsAi ?? 0,
  })));

  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.flushHeaders();

  try {
    const client = new OpenAI({
      baseURL: 'https://models.inference.ai.azure.com',
      apiKey: token,
    });

    const stream = await client.chat.completions.create({
      model: 'gpt-4o-mini',
      stream: true,
      messages: [
        {
          role: 'system',
          content:
            'Du bist ein Webanalyse-Experte. Du bekommst Tagesstatistiken einer Website als JSON mit den Feldern ' +
            'date (Datum), total (Gesamtaufrufe), human (menschliche Besucher), bot (Bots), ai (KI-Crawler). ' +
            'Analysiere die Daten auf Deutsch und berichte über: Trends bei menschlichen Besuchern, ' +
            'Bot-Auffälligkeiten oder Angriffsmuster, KI-Crawler-Entwicklung, Wochentags-Muster, ' +
            'besondere Ausreißer und konkrete Handlungsempfehlungen. Antworte strukturiert mit Überschriften.',
        },
        {
          role: 'user',
          content: `Hier sind die Tagesstatistiken der letzten ${entries.length} Tage:\n${statsJson}`,
        },
      ],
    });

    for await (const chunk of stream) {
      const text = chunk.choices[0]?.delta?.content ?? '';
      if (text) {
        res.write(`data: ${JSON.stringify({ text })}\n\n`);
      }
    }
    res.write('data: {"done":true}\n\n');
    res.end();
  } catch (err) {
    console.error('KI-Analyse Fehler:', err);
    res.write(`data: ${JSON.stringify({ error: err.message })}\n\n`);
    res.end();
  }
});

// KI-Analyse der Rohdaten der letzten 30 Tage (access-YYYY-MM-DD.log)
app.post('/ai-analyze-raw', async (req, res) => {
  const token = process.env.GITHUB_TOKEN;
  if (!token) {
    return res.status(500).json({ success: false, error: 'GITHUB_TOKEN nicht konfiguriert.' });
  }

  if (!fs.existsSync(extractedDirectory)) {
    return res.status(400).json({ success: false, error: 'Kein unzipped-Verzeichnis gefunden.' });
  }

  // Letzte 30 Tage bestimmen
  const cutoff = new Date();
  cutoff.setDate(cutoff.getDate() - 30);
  const cutoffStr = cutoff.toISOString().slice(0, 10);

  const logFiles = fs.readdirSync(extractedDirectory)
    .filter((name) => {
      const m = /^access-(\d{4}-\d{2}-\d{2})\.log$/.exec(name);
      return m && m[1] >= cutoffStr;
    })
    .sort().reverse();

  if (!logFiles.length) {
    return res.status(400).json({ success: false, error: 'Keine Log-Dateien der letzten 30 Tage gefunden.' });
  }

  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.flushHeaders();

  try {
    // Pro Datei max. 300 Zeilen sampeln – nur Einträge zwischen 10:00 und 16:00 Uhr
    const SAMPLE_PER_FILE = 300;
    const sampleLines = [];

    // Apache-Zeitformat: [31/Mar/2026:14:23:01 +0200] → Stunde und Minute extrahieren
    const timeInWindowRegex = /\[[\d]{2}\/\w+\/\d{4}:(\d{2}):(\d{2}):\d{2} [+\-]\d{4}\]/;

    for (const name of logFiles) {
      const fullPath = path.join(extractedDirectory, name);
      const content = fs.readFileSync(fullPath, 'utf8');
      const lines = content.split('\n').filter((line) => {
        if (!line) return false;
        // Nur Zeilen für www.hrv.de
        if (!line.includes('www.hrv.de')) return false;
        // Statische Ressourcen und Bilder ignorieren
        if (/\.(ico|js|css|png|jpg|jpeg|gif|svg|webp|bmp|woff2?|ttf|eot)("| )/.test(line)) return false;
        // HTTP-Statuscode extrahieren: nach "GET /pfad HTTP/1.1" folgt der Status
        const statusMatch = /"\s+(\d{3})\s+/.exec(line);
        if (statusMatch) {
          const status = parseInt(statusMatch[1], 10);
          // 3xx und 404 ignorieren
          if (status >= 300 && status < 400) return false;
          if (status === 404) return false;
        }
        // Zeitfenster 07:30–21:00
        const m = timeInWindowRegex.exec(line);
        if (!m) return false;
        const totalMinutes = parseInt(m[1], 10) * 60 + parseInt(m[2], 10);
        return totalMinutes >= 7 * 60 + 30 && totalMinutes < 21 * 60;
      });
      // Von hinten sampeln → neueste Einträge der Datei zuerst
      const reversed = lines.slice().reverse();
      const step = Math.max(1, Math.floor(reversed.length / SAMPLE_PER_FILE));
      for (let i = 0; i < reversed.length && sampleLines.length < logFiles.length * SAMPLE_PER_FILE; i += step) {
        sampleLines.push(reversed[i].replace(/\b(\d{1,3}\.\d{1,3}\.\d{1,3})\.0\b/g, '$1.*'));
      }
    }

    // Token-Budget: 8000 gesamt – ca. 600 für System-/User-Prompt-Overhead → 7400 für Rohdaten
    // Schätzung: 1 Token ≈ 4 Zeichen
    const TOKEN_BUDGET = 7400;
    let totalChars = 0;
    const budgetedLines = [];
    for (const line of sampleLines) {
      const lineChars = line.length + 1; // +1 für '\n'
      if (totalChars + lineChars > TOKEN_BUDGET * 4) break;
      budgetedLines.push(line);
      totalChars += lineChars;
    }

    const rawSample = budgetedLines.join('\n');
    const actualDays = logFiles.length;
    const dateFrom = logFiles[logFiles.length - 1].slice(7, 17);
    const dateTo = logFiles[0].slice(7, 17);

    res.write(`data: ${JSON.stringify({ info: `Werte ${actualDays} Tag(e) aus (${dateFrom} bis ${dateTo}), ${budgetedLines.length} Log-Zeilen` })}\n\n`);

    const client = new OpenAI({
      baseURL: 'https://models.inference.ai.azure.com',
      apiKey: token,
    });

    const stream = await client.chat.completions.create({
      model: 'gpt-4o-mini',
      stream: true,
      messages: [
        {
          role: 'system',
          content:
            'Du bist ein Webanalyse-Experte. Du bekommst eine repräsentative Auswahl von Apache-Access-Log-Zeilen ' +
            'der letzten 30 Tage. Jede Zeile hat das Format: IP - - [Datum] "Methode Pfad Protokoll" Status Bytes Referer UserAgent. ' +
            'Analysiere auf Deutsch und berichte über: ' +
            '1) Häufigste aufgerufene Seiten/Pfade, ' +
            '2) Auffällige IP-Adressen oder Angriffsmuster (z.B. viele 4xx-Fehler, Scanner), ' +
            '3) HTTP-Statuscodes (Fehler 4xx/5xx häufig?), ' +
            '4) Bot- und KI-Crawler-User-Agents, ' +
            '5) Ungewöhnliche Anfragen oder Sicherheitshinweise, ' +
            '6) Konkrete Handlungsempfehlungen. ' +
            'Antworte strukturiert mit Überschriften.',
        },
        {
          role: 'user',
          content: `Analysiere diese ${budgetedLines.length} Apache-Log-Zeilen aus ${actualDays} Tagen (${dateFrom} bis ${dateTo}):\n\n${rawSample}`,
        },
      ],
    });

    for await (const chunk of stream) {
      const text = chunk.choices[0]?.delta?.content ?? '';
      if (text) {
        res.write(`data: ${JSON.stringify({ text })}\n\n`);
      }
    }
    res.write('data: {"done":true}\n\n');
    res.end();
  } catch (err) {
    console.error('KI-Rohdaten-Analyse Fehler:', err);
    res.write(`data: ${JSON.stringify({ error: err.message })}\n\n`);
    res.end();
  }
});

app.listen(PORT, () => {
  console.log(`Server läuft auf http://localhost:${PORT}`);
});
