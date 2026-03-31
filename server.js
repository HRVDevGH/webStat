const express = require('express');
const path = require('path');
const fs = require('fs');
const zlib = require('zlib');
const Client = require('ssh2-sftp-client');
const crypto = require('crypto');

const app = express();
const PORT = process.env.PORT || 3000;

// *** HIER DEINE SFTP-ZUGANGSDATEN EINTRAGEN ***
const sftpConfig = {
  host: 'access-5000008740.ud-webspace.de',    // z.B. 'sftp.meinserver.de'
  port: 22,
  username: 'a4467',
  password: 'Did!HvN2017u',
};

// Remote-Verzeichnis auf dem SFTP-Server, aus dem .gz-Dateien geholt werden
const remoteDirectory = '/logs';

// Lokales Zielverzeichnis (relativ zum Projektordner)
const localDirectory = path.join(__dirname, 'downloads');
// Unterverzeichnis für entpackte Dateien
const extractedDirectory = path.join(localDirectory, 'unzipped');
// Metadatei für bereits verarbeitete Downloads
const downloadMetaFile = path.join(localDirectory, 'download-meta.json');

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
  const sftp = new Client();

  try {
    const meta = loadDownloadMeta();

    await sftp.connect(sftpConfig);

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

    // Schritt 1: Alle passenden Archive herunterladen (nur neue/ungeänderte)
    for (const file of gzFiles) {
      const key = file.name;
      const existing = meta.files && meta.files[key];
      const remoteSize = file.size;
      const remoteMtime = file.modifyTime;

      if (
        existing &&
        typeof existing.remoteSize === 'number' &&
        typeof existing.remoteMtime === 'number' &&
        existing.remoteSize === remoteSize &&
        existing.remoteMtime === remoteMtime
      ) {
        // Datei wurde bereits heruntergeladen und verarbeitet, überspringen
        skippedFiles.push(key);
        continue;
      }

      const remotePath = `${remoteDirectory.replace(/\/+$/,'')}/${file.name}`; // POSIX-Pfad
      const localPath = path.join(localDirectory, file.name);

      await sftp.fastGet(remotePath, localPath);
      downloadedFiles.push(file.name);
      localPaths.push({ localPath, key, remoteSize, remoteMtime });
    }

    // Verbindung zum SFTP-Server kann nach dem Download beendet werden
    await sftp.end();

    // Schritt 2+3: Alle neu geladenen Archive entpacken und in Tagesdateien aufteilen
    const processedExtractedFiles = new Set();
    let processedBytes = 0;
    let skippedBytes = 0;

    // Bytes der übersprungenen Archive mitrechnen (für Gesamtübersicht)
    for (const file of gzFiles) {
      if (skippedFiles.includes(file.name)) {
        const size = typeof file.size === 'number' ? file.size : 0;
        skippedBytes += size;
      }
    }

    for (const item of localPaths) {
      const { localPath, key, remoteSize, remoteMtime } = item;
      try {
        const extractedNames = await extractAndNormalizeLog(localPath);
        if (Array.isArray(extractedNames)) {
          extractedNames.forEach((n) => processedExtractedFiles.add(n));
        } else if (typeof extractedNames === 'string' && extractedNames) {
          processedExtractedFiles.add(extractedNames);
        }

        // Nach erfolgreicher Verarbeitung Bytes und Hash erfassen
        if (typeof remoteSize === 'number') {
          processedBytes += remoteSize;
        }

        // Hash berechnen und in Metadatei speichern
        try {
          const hash = await sha256File(localPath);
          meta.files[key] = {
            remotePath: `${remoteDirectory.replace(/\/+$/,'')}/${key}`,
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
    }

    // Metadatei aktualisieren
    saveDownloadMeta(meta);

    // Für die Fortschrittsanzeige gelten auch übersprungene Dateien als "bereits bearbeitet"
    const processedFilesTotal = downloadedFiles.length + skippedFiles.length;
    const processedBytesTotal = processedBytes + skippedBytes;

    res.json({
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
    });
  } catch (error) {
    console.error('SFTP-Fehler:', error);
    try {
      await sftp.end();
    } catch (_) {}

    res.status(500).json({
      success: false,
      message: `Fehler beim Herunterladen der Dateien vom Host ${sftpConfig.host}`,
      error: error.message,
      host: sftpConfig.host,
    });
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

// Liste alle Dateien im lokalen downloads-Verzeichnis auf
app.get('/files', (req, res) => {
  try {
    if (!fs.existsSync(localDirectory)) {
      return res.json({ success: true, files: [] });
    }

    const files = fs.readdirSync(localDirectory).filter((name) => {
      const fullPath = path.join(localDirectory, name);
      return (
        fs.statSync(fullPath).isFile() &&
        name.toLowerCase().endsWith('.gz')
      );
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
      return res.json({ success: true, files: [] });
    }

    const files = fs
      .readdirSync(extractedDirectory)
      .filter((name) => {
        const fullPath = path.join(extractedDirectory, name);
        return (
          fs.statSync(fullPath).isFile() &&
          name.toLowerCase().endsWith('.log')
        );
      });

    res.json({ success: true, files });
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

app.listen(PORT, () => {
  console.log(`Server läuft auf http://localhost:${PORT}`);
});
