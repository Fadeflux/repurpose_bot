"""
Post-processor MP4 pour patcher les creation_time au niveau atomique.

FFmpeg écrase systématiquement les creation_time des streams (tracks) avec
celui du format quand on utilise le container MP4. On contourne en éditant
directement les atoms 'mvhd' (movie header) et 'tkhd' (track header) du
container QuickTime/MP4 après l'encodage.

Les atoms MP4 stockent les dates en "Mac Epoch" (secondes depuis 1904-01-01).
"""
import struct
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional, Tuple

from app.utils.logger import get_logger

logger = get_logger("mp4_patcher")


# Référence Mac Epoch : secondes entre 1904-01-01 UTC et 1970-01-01 UTC
MAC_EPOCH_OFFSET = 2082844800


def _to_mac_time(dt: datetime) -> int:
    """Convertit un datetime UTC en timestamp Mac Epoch (1904-01-01)."""
    return int(dt.replace(tzinfo=timezone.utc).timestamp()) + MAC_EPOCH_OFFSET


def _find_atoms(data: bytes, atom_name: bytes, start: int = 0, end: Optional[int] = None) -> List[Tuple[int, int]]:
    """
    Cherche tous les atoms `atom_name` dans `data`.
    Retourne liste de (offset, size).
    Parcourt le container de façon récursive.
    """
    if end is None:
        end = len(data)
    results = []
    pos = start
    while pos < end - 8:
        size = struct.unpack(">I", data[pos:pos+4])[0]
        name = data[pos+4:pos+8]
        if size < 8 or pos + size > end:
            break
        if name == atom_name:
            results.append((pos, size))
        # Containers MP4 qui peuvent contenir des sous-atoms
        if name in (b"moov", b"trak", b"mdia", b"minf", b"stbl", b"udta", b"meta", b"edts"):
            # Certains atoms ont un header étendu
            header_size = 8
            if name == b"meta":
                header_size = 12   # meta a un version/flags
            sub_results = _find_atoms(data, atom_name, pos + header_size, pos + size)
            results.extend(sub_results)
        pos += size
    return results


def _patch_header_dates(
    data: bytearray,
    offset: int,
    creation_dt: datetime,
    modification_dt: Optional[datetime] = None,
) -> bool:
    """
    Patche les dates creation_time et modification_time dans un atom mvhd ou tkhd.

    Structure atom (v0) :
      [4] size
      [4] type (mvhd/tkhd)
      [1] version (0 ou 1)
      [3] flags
      [4] creation_time (si v0) ou [8] (si v1)
      [4] modification_time (si v0) ou [8] (si v1)
      ...
    """
    if modification_dt is None:
        modification_dt = creation_dt

    # Position du version byte (juste après size + type = 8 bytes)
    version = data[offset + 8]
    creation_mac = _to_mac_time(creation_dt)
    modif_mac = _to_mac_time(modification_dt)

    # Start of creation_time field (après size 4 + type 4 + version 1 + flags 3 = 12)
    ct_offset = offset + 12

    if version == 0:
        # v0 : 32-bit timestamps
        struct.pack_into(">I", data, ct_offset, creation_mac & 0xFFFFFFFF)
        struct.pack_into(">I", data, ct_offset + 4, modif_mac & 0xFFFFFFFF)
        return True
    elif version == 1:
        # v1 : 64-bit timestamps
        struct.pack_into(">Q", data, ct_offset, creation_mac)
        struct.pack_into(">Q", data, ct_offset + 8, modif_mac)
        return True
    return False


def patch_mp4_creation_times(
    mp4_path: Path,
    format_time: datetime,
    video_time: datetime,
    audio_time: datetime,
) -> bool:
    """
    Patche un MP4 pour avoir des creation_time distincts :
    - mvhd (movie header) = format_time
    - tkhd du track vidéo = video_time
    - tkhd du track audio = audio_time

    Retourne True si succès, False sinon.
    """
    try:
        with open(mp4_path, "rb") as f:
            data = bytearray(f.read())
    except Exception as e:
        logger.error(f"Impossible de lire {mp4_path}: {e}")
        return False

    # Patch mvhd (movie header) — dans moov, un seul
    mvhd_atoms = _find_atoms(data, b"mvhd")
    if mvhd_atoms:
        offset, _ = mvhd_atoms[0]
        _patch_header_dates(data, offset, format_time)

    # Patch chaque tkhd (track header) — un par stream
    # Le premier tkhd = vidéo, le second = audio (ordre standard FFmpeg)
    tkhd_atoms = _find_atoms(data, b"tkhd")
    if len(tkhd_atoms) >= 1:
        offset, _ = tkhd_atoms[0]
        _patch_header_dates(data, offset, video_time)
    if len(tkhd_atoms) >= 2:
        offset, _ = tkhd_atoms[1]
        _patch_header_dates(data, offset, audio_time)

    # Patch aussi les mdhd (media header) de chaque track
    mdhd_atoms = _find_atoms(data, b"mdhd")
    if len(mdhd_atoms) >= 1:
        _patch_header_dates(data, mdhd_atoms[0][0], video_time)
    if len(mdhd_atoms) >= 2:
        _patch_header_dates(data, mdhd_atoms[1][0], audio_time)

    try:
        with open(mp4_path, "wb") as f:
            f.write(data)
        return True
    except Exception as e:
        logger.error(f"Impossible d'écrire {mp4_path}: {e}")
        return False


def parse_iso_datetime(s: str) -> datetime:
    """Parse un string ISO8601 en datetime UTC."""
    # Enlève le Z final et le .000000 pour simplifier
    s = s.replace("Z", "").split(".")[0]
    return datetime.fromisoformat(s)
