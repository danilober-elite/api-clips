CREATE TABLE IF NOT EXISTS clips (
    id INTEGER AUTOINCREMENT,
    device_serial TEXT NOT NULL, -- Podría ser una FK que referencie a una tabla de dispositivos
    uploaded_by TEXT NOT NULL, -- Podría ser una FK que referencia a una tabla de usuarios
    path TEXT NOT NULL UNIQUE,
    duration REAL NOT NULL CHECK (duration > 0), -- En segundos
    status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'reviewed', 'rejected')),
    tags TEXT,
    -- is_deleted INTEGER NOT NULL DEFAULT 0 CHECK (is_deleted IN (0, 1)),
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
);

CREATE TABLE IF NOT EXISTS clip_reviews (
    id INTEGER AUTOINCREMENT,
    clip_id INTEGER NOT NULL UNIQUE,
    reviewer TEXT NOT NULL, -- Podría ser una FK que referencia a una tabla de usuarios
    comment TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    FOREIGN KEY(clip_id) REFERENCES clips(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS clip_metrics (
    id INTEGER AUTOINCREMENT,
    clip_id INTEGER NOT NULL UNIQUE,
    views INTEGER DEFAULT 0 CHECK (views >= 0),
    likes INTEGER DEFAULT 0 CHECK (likes >=0),
    downloads INTEGER DEFAULT 0 CHECK (downloads >= 0),
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    FOREIGN KEY(clip_id) REFERENCES clips(id) ON DELETE CASCADE
);

-- CREATE INDEX IF NOT EXISTS idx_device_serial IN clips (device_serial);
-- CREATE INDEX IF NOT EXISTS idx_status IN clips (status);
-- CREATE INDEX IF NOT EXISTS idx_created_at IN clips (created_at DESC);