import sqlite3

from config import CLIPS_DB
from math import ceil
from pathlib import Path

class DatabaseManager:
    def __init__(self, db_path=CLIPS_DB):
        self.db_path = str(db_path) if isinstance(db_path, Path) else db_path

    def _get_connection(self):
        conn = sqlite3.connect(self.db_path)
        conn.execute('PRAGMA foreign_keys = ON')
        return conn
    
    def create_clips_table(self):
        conn = self._get_connection()
        cur = conn.cursor()
        try:
            cur.execute('''
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
                    PRIMARY KEY (id)
            )''')
            conn.commit()
        finally:
            conn.close()

    def create_clip_reviews_table(self):
        conn = self._get_connection()
        cur = conn.cursor()
        try:
            cur.execute('''
                CREATE TABLE IF NOT EXISTS clip_reviews (
                    id INTEGER AUTOINCREMENT,
                    clip_id INTEGER NOT NULL UNIQUE,
                    reviewer TEXT NOT NULL, -- Podría ser una FK que referencia a una tabla de usuarios
                    comment TEXT,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (id),
                    FOREIGN KEY(clip_id) REFERENCES clips(id) ON DELETE CASCADE
            )''')
            conn.commit()
        finally:
            conn.close()

    def create_clip_metrics_table(self):
        conn = self._get_connection()
        cur = conn.cursor()
        try:
            cur.execute('''
                CREATE TABLE IF NOT EXISTS clip_metrics (
                    id INTEGER AUTOINCREMENT,
                    clip_id INTEGER NOT NULL UNIQUE,
                    views INTEGER DEFAULT 0 CHECK (views >= 0),
                    likes INTEGER DEFAULT 0 CHECK (likes >=0),
                    downloads INTEGER DEFAULT 0 CHECK (download >= 0),
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (id),
                    FOREIGN KEY(clip_id) REFERENCES clips(id) ON DELETE CASCADE
            )''')
            conn.commit()
        finally:
            conn.close()

    def add_clip(self, device_serial, uploaded_by, path, duration):
        """..."""
        with self._get_connection() as conn:
            conn.execute("""BEGIN TRANSACTION""")
            try:
                # Validar que device existe
                cursor = conn.execute(
                    """SELECT * FROM clips WHERE device_serial = ? LIMIT 1""",
                    (device_serial,) # True si se ha subido algún clip antes
                )

                if not cursor.fetchone():
                    conn.rollback()
                    return False
                
                # Validar que user existe
                cursor = conn.execute(
                    """SELECT * FROM clips WHERE uploaded_by = ? LIMIT 1""",
                    (uploaded_by,)
                )

                if not cursor.fetchone():
                    conn.rollback()
                    return False

                # Insertar clip
                cursor = conn.execute(
                    """INSERT INTO clips (
                        device_serial, uploaded_by, path, duration
                    ) VALUES (?, ?, ?, ?)""", 
                    (device_serial, uploaded_by, path, duration)
                )
                
                # Insertar métricas iniciales
                clip_id = cursor.lastrowid
                conn.execute(
                    """INSERT INTO clip_metrics (clip_id) VALUES (?)""",
                    (clip_id,)
                )

                conn.commit()
                return clip_id

            except sqlite3.Error:
                conn.rollback()
                return False

    def bulk_update_status(self, db_path, clip_ids, new_status):
        """..."""
        if not clip_ids or new_status not in ('pending', 'reviewed', 'rejected'):
            return False
        
        with self._get_connection(db_path) as conn:
            conn.execute("""BEGIN TRANSACTION""")
            try:
                # Insertar lote de clip_ids con un solo UPDATE
                placeholders = ','.join('?' * len(clip_ids))
                cursor = conn.execute(
                    f"""UPDATE clips SET status = ? WHERE id IN ({placeholders})""",
                    [new_status] + clip_ids
                )

                if cursor.rowcount == 0:
                    conn.rollback()
                    return False
                
                conn.commit()
                return True
            except sqlite3.Error:
                conn.rollback()
                return False

    def get_clip_statistics(self, db_path, clip_id):
        """..."""
        with self._get_connection(db_path) as conn:
            cursor = conn.execute("""
                SELECT
                    c.id, c.device_serial, c.uploaded_by, c.duration, c.tags
                    COALESCE(cm.views) AS views,
                    COALESCE(cm.likes) AS likes,
                    COALESCE(cm.downloads) AS downloads,
                FROM clips c
                LEFT JOIN clip_metrics cm ON c.id = cm.clip_id
                WHERE c.id = ?
            """), (clip_id,)
            row = cursor.fetchone()
            if not row:
                return False
            
            return {
                "id": row[0],
                "device_serial": row[1],
                "uploaded_by": row[2],
                "duration": row[3],
                "tags": row[4],
                "views": row[5],
                "likes": row[6],
                "downloads": row[7]
            }

    def get_pending_clips(self, db_path, status='pending', device_serial=None,
                          reviewer=None, tags=None, page=1, per_page=10):
        """..."""
        if page < 1 or per_page < 1 or per_page > 100:
            return False

        with self._get_connection(db_path) as conn:
            base_query = """
                SELECT
                    c.id, c.device_serial, c.uploaded_by, c.path, c.duration,
                    c.status, c.tags,
                    COALESCE(cm.views) AS views,
                    COALESCE(cm.likes) AS likes,
                    COALESCE(cm.downloads) AS downloads,
                    cr.reviewer AS reviewed_by, cr.comment
                FROM clips c
                LEFT JOIN clip_metrics cm ON c.id = cm.clip_id
                LEFT JOIN clip_reviews cr ON c.id = cr.clip_id
                WHERE c.status = ?
            """
            params = [status]
            where_clauses = []

            if device_serial:
                where_clauses.append('c.device_serial = ?')
                params.append(device_serial)

            if reviewer:
                where_clauses.append('cr.reviewer = ?')
                params.append(reviewer)

            if tags:
                where_clauses.append('c.tags LIKE ?')
                params.append(f'%{tags}%')

            # Contruir query de conteo
            count_query = base_query.replace(
                'SELECT ... FROM',
                'SELECT COUNT(DISTINCT c.id) as total FROM')
            if where_clauses:
                count_query += ' AND ' + ' AND '.join(where_clauses)

            # Ejecutar query de conteo
            try:
                conn.execute(count_query, params)
                total = conn.fetchone()[0]
            except sqlite3.Error:
                return False

            # Construir query paginada
            pagination_query = base_query
            if where_clauses:
                pagination_query += ' AND ' + ' AND '.join(where_clauses)
            # Ordenación por más recientes primero
            pagination_query += ' ORDER BY c.created_at DESC'

            if page is not None:
                pagination_query += " LIMIT ?"
                params.append(page)
            if per_page is not None:
                pagination_query += " OFFSET ?"
                params.append(per_page)

            # Ejecutar query paginada
            try:
                conn.execute(pagination_query, params)
                rows = conn.fetchall()
            except sqlite3.Error:
                return False
                
            clips = []
            for clip in rows:
                clips.append({
                    "id": clip[0],
                    "device_serial": clip[1],
                    "uploaded_by": clip[2],
                    "path": clip[3],
                    "duration": clip[4],
                    "status": clip[5],
                    "tags": clip[6],
                    "views": clip[7],
                    "likes": clip[8],
                    "downloads": clip[9]
                })
            
            results = {
                "total": total,
                "page": page,
                "per_page": per_page,
                "total_pages": ceil(total / per_page),
                "results": clips
            }

            return results, total
    
    def record_review(self, db_path, clip_id, reviewer, comment):
        """..."""
        with self._get_connection(db_path) as conn:
            conn.execute("""BEGIN TRANSACTION""")
            try:
                # Verificar que el clip existe y que está 'pending'
                cursor = conn.execute(
                    """SELECT status FROM clips WHERE id = ?""",
                    (clip_id,)
                )
                row = cursor.fetchone()
                if not row or row[0] is not 'pending':
                    conn.rollback()
                    return False
                
                # Insertar review
                cursor = conn.execute(
                    """INSERT INTO clip_reviews (clip_id, reviewer, comment)
                    VALUES (?, ?, ?)
                    """,
                    (clip_id, reviewer, comment)
                )

                # Actualizar status del clip a 'reviewed'
                cursor = conn.execute(
                    """UPDATE clips SET status = 'reviewed' WHERE id = ?""",
                    (clip_id,)
                )

                conn.commit()
                return True

            except sqlite3.Error:
                conn.rollback()
                return False

