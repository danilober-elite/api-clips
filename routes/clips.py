import os
import sqlite3

from datetime import datetime
from flask import Blueprint, request, jsonify
from controllers.db_manager import DatabaseManager
from urllib.parse import unquote

db_manager = DatabaseManager()
DB_PATH = os.getenv('DB_PATH')

bp_clips = Blueprint("clips", __name__)


@bp_clips.route("/clips", methods=["POST"])
def save_clip():
    """..."""

    # Comprobar los datos de entrada
    payload = request.json
    if not payload:
        return jsonify({"error": "JSON requerido"}), 400
    
    # Comprobar que están todos los campos requeridos
    required = ['device_serial', 'uploaded_by', 'path', 'duration']
    for field in required:
        if field not in payload:
            return jsonify({"error": f"Falta {field}"}), 400
    
    # Validar duración de clip
    try:
        duration = float(payload['duration'])
        if duration <= 0:
            return jsonify({"error": "duration > 0 requerido"}), 400
    except ValueError:
        return jsonify({"error": "duration inválido"}), 400
    
    # Almacenar clip y obtener su id
    clip_id = db_manager.add_clip(
        db_path=DB_PATH,
        device_serial=payload['device_serial'],
        uploaded_by=payload['uploaded_by'],
        path=payload['path'],
        duration=duration
    )

    if clip_id is None:
        return jsonify({"error": "Dispositivo o usuario inválido"}), 409
    
    return jsonify({"clip_id": clip_id}), 201


@bp_clips.route("/clips/status", methods=["PUT"])
def update_clips_status():
    """..."""
    # Validar datos
    data = request.json
    if not data or 'clip_ids' not in data or 'status' not in data:
        return jsonify({"error": "clip_ids y status requeridos"}), 400
    
    clip_ids = data['clip_ids']
    status = data['status']
    
    if not isinstance(clip_ids, list) or not clip_ids:
        return jsonify({"error": "clip_ids debe ser lista no vacía"}), 400
    
    if status not in ('pending', 'reviewed', 'rejected'):
        return jsonify({"error": "status inválido"}), 400
    
    success = db_manager.bulk_update_status(
        db_path=DB_PATH,
        clip_ids=clip_ids,
        new_status=status
    )

    if not success:
        return jsonify({"error": "Actualización falló"}), 500
    
    # Obtener count real
    with db_manager._get_connection(DB_PATH) as conn:
        try:
            placeholders = ','.join('?' * len(clip_ids))
            count = conn.execute(
                f"""SELECT COUNT(*) FROM clips WHERE id IN ({placeholders})""",
                (clip_ids,)
            ).fetchone()[0]
        except sqlite3.Error:
            return jsonify({"error": "Conteo falló"}), 500

    return jsonify({"updated": count}), 200


@bp_clips.route("/clips/pending", methods=["GET"])
def get_pending_clips():
    """..."""
    # Parsear query params
    page = int(request.args.get('page', 1))
    per_page = int(request.args.get('per_page', 20))
    device_serial = request.args.get('device_serial')
    reviewer = request.args.get('reviewer')
    tags = request.args.get('tags')
    sort = request.args.get('sort', 'created_at:desc')
    created_before = request.args.get('created_before')
    created_after = request.args.get('created_after')
    
    if page < 1 or per_page < 1 or per_page > 100:
        return jsonify({"error": "Paginación inválida"}), 400
    
    # Desempaquetar los tags
    if tags:
        tags = [unquote(t.strip()) for t in tags.split(',') if t.strip()]

    # Validar fechas
    try:
        if created_before:
            created_before = datetime.strptime(created_before, '%Y-%m-%d')
        if created_after:
            created_after = datetime.strptime(created_after, '%Y-%m-%d')
    except ValueError:
        return jsonify({"error": "Formato de fecha inválido."
        "Tanto 'created_before' como 'created_after' deben seguir el"
        "formato: YYYY-MM-DD"}), 400
    
    results = db_manager.get_pending_clips(
        db_path=DB_PATH,
        device_serial=device_serial,
        reviewer=reviewer,
        tags=tags,
        sort=sort,
        created_before=created_before,
        created_after=created_after,
        page=page,
        per_page=per_page
    )
    
    if results is None:
        return jsonify({"error": "Filtros inválidos"}), 400
    
    return jsonify(results), 200


@bp_clips.route("/clips/<int:clip_id>/review", methods=["POST"])
def update_clip_review_by_id(clip_id):
    """..."""
    data = request.json
    if not data or 'reviewer' not in data or 'comment' not in data:
        return jsonify({"error": "reviewer y comment requeridos"}), 400
    
    success = db_manager.record_review(
        db_path=DB_PATH,
        clip_id=clip_id,
        reviewer=data['reviewer'],
        comment=data['comment']
    )

    if not success:
        return jsonify({"error": "Clip no existe o ya revisado"}), 409
    
    return jsonify({"message": "Review registrada"}), 201


@bp_clips.route("/clips/<int:clip_id>/stats", methods=["GET"])
def get_clip_stats_by_id(clip_id):
    """..."""
    stats = db_manager.get_clip_statistics(
        db_path=DB_PATH,
        clip_id=clip_id
    )
    if stats is None:
        return jsonify({"error": "Clip no encontrado"}), 404
    
    return jsonify(stats), 200

