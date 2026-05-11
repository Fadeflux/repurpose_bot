// ClipFusion Clone — frontend logic
const API = '/api/clipfusion';

const state = {
    align: 'center',
    audioPriority: 'template',
    sizeLabel: 'L',
    maxVariants: 1,
    eventSource: null,
    mixStartTime: 0,
    editingTplId: null,
    // Step 5 new controls
    positionPct: 50,
    fontSizePx: 56,
    durationEnabled: false,
    durationSec: 15,
    captionStyle: 'outlined',
    selectedVA: '',
    selectedTeam: 'geelark',
    selectedDevice: 'iphone_random',
    selectedModelId: '',
    models: [],
    contentModelId: '',         // catégorie active à l'étape 3 (upload)
    contentFilterModelId: '',   // filtre d'affichage (pour la liste vidéos uploadées)
    vasByTeam: { geelark: [], instagram: [] },
    vaEmails: {},  // discord_id -> email (pour afficher badge "pas d'email" dans le sélecteur)
    spoof: {
        // key -> { enabled, min, max, default_min, default_max, step, label, isInt }
        bitrate:    { enabled: true, min: 8000,  max: 12000, default_min: 8000,  default_max: 12000, step: 100,    label: 'Video Bitrate', isInt: true },
        brightness: { enabled: true, min: -0.05, max: 0.05,  default_min: -0.05, default_max: 0.05,  step: 0.01,   label: 'Brightness',    isInt: false },
        contrast:   { enabled: true, min: 0.95,  max: 1.10,  default_min: 0.95,  default_max: 1.10,  step: 0.01,   label: 'Contrast',      isInt: false },
        saturation: { enabled: true, min: 0.95,  max: 1.15,  default_min: 0.95,  default_max: 1.15,  step: 0.01,   label: 'Saturation',    isInt: false },
        gamma:      { enabled: true, min: 0.95,  max: 1.05,  default_min: 0.95,  default_max: 1.05,  step: 0.01,   label: 'Gamma',         isInt: false },
        speed:      { enabled: true, min: 1.03,  max: 1.04,  default_min: 1.03,  default_max: 1.04,  step: 0.01,   label: 'Speed',         isInt: false },
        zoom:       { enabled: true, min: 1.03,  max: 1.06,  default_min: 1.03,  default_max: 1.06,  step: 0.01,   label: 'Zoom',          isInt: false },
        noise:      { enabled: true, min: 5,     max: 15,    default_min: 5,     default_max: 15,    step: 1,      label: 'Noise',         isInt: true },
        vignette:   { enabled: true, min: 0.20,  max: 0.40,  default_min: 0.20,  default_max: 0.40,  step: 0.05,   label: 'Vignette',      isInt: false },
        rotation:   { enabled: true, min: -0.5,  max: 0.5,   default_min: -0.5,  default_max: 0.5,   step: 0.1,    label: 'Rotation',      isInt: false },
        cut_start:  { enabled: true, min: 0.10,  max: 0.15,  default_min: 0.10,  default_max: 0.15,  step: 0.05,   label: 'Cut Start',     isInt: false },
        cut_end:    { enabled: true, min: 0.10,  max: 0.15,  default_min: 0.10,  default_max: 0.15,  step: 0.05,   label: 'Cut End',       isInt: false },
    },
};

// ============ EMOJI DATA ============
const EMOJI_QUICK = ['😅', '💀', '😂', '🥺', '🔥', '❤️', '😭', '✨', '💯', '👀'];
const EMOJI_CATEGORIES = {
    faces: ['😀','😁','😂','🤣','😅','😆','😉','😊','😇','🥰','😍','🤩','😘','😋','😛','😜','🤪','😎','🥳','😏','😒','🙄','😬','🤔','🫠','😴','😪','🤤','🤐','🥱','🤨','🧐','🤓','🥺','😢','😭','😡','🤬','🤯','😱','😳','🥵','🥶','🫣','😈','👻','💀','☠️','🤡','💩'],
    hearts: ['❤️','🧡','💛','💚','💙','💜','🤎','🖤','🤍','💔','❣️','💕','💞','💓','💗','💖','💘','💝','💟','♥️'],
    hands: ['👍','👎','👌','🤌','🤏','✌️','🤞','🤟','🤘','🤙','👈','👉','👆','👇','☝️','✋','🤚','🖐️','🖖','👋','🤝','🙏','💅','🦶','💪','🫶','👏','🙌','🤲','🫳','🫴'],
    symbols: ['🔥','✨','💫','⭐','🌟','💥','💢','💯','💨','💦','💤','🎯','🚀','⚡','🌈','🎊','🎉','🎁','💎','🏆','👑','🍀','☘️','🌹','🌸','🌺','🌻','🌷','🌼','💐','🍑','🍒','🥺','😩'],
    objects: ['🎵','🎶','🎤','🎧','🎬','📷','📸','📹','🎥','💃','🕺','💋','💄','👗','👙','👠','👜','💍','📱','💌','💸','💰','🍷','🍸','🥂','🍾','🍓','🍑','🍌','🍒'],
};

function buildEmojiQuickRow(targetId, textareaId) {
    const target = document.getElementById(targetId);
    if (!target) return;
    target.innerHTML = '';
    EMOJI_QUICK.forEach(emoji => {
        const btn = document.createElement('button');
        btn.type = 'button';
        btn.className = 'emoji-btn';
        btn.textContent = emoji;
        btn.title = 'Insérer ' + emoji;
        btn.addEventListener('click', () => insertEmoji(textareaId, emoji));
        target.appendChild(btn);
    });
}

function insertEmoji(textareaId, emoji) {
    const ta = document.getElementById(textareaId);
    if (!ta) return;
    const start = ta.selectionStart ?? ta.value.length;
    const end = ta.selectionEnd ?? ta.value.length;
    const before = ta.value.slice(0, start);
    const after = ta.value.slice(end);
    // Add a space if needed
    const sep = (before && !before.endsWith(' ') && !before.endsWith('\n')) ? ' ' : '';
    const newVal = before + sep + emoji + after;
    ta.value = newVal;
    const pos = (before + sep + emoji).length;
    ta.selectionStart = ta.selectionEnd = pos;
    ta.focus();
}

// ============ TOAST ============
function toast(msg, isError = false) {
    const t = document.getElementById('toast');
    t.textContent = msg;
    t.className = 'toast' + (isError ? ' error' : '');
    setTimeout(() => t.classList.add('hidden'), 3000);
}

// ============ NAVIGATION ============
function goToStep(n) {
    document.querySelectorAll('.step').forEach(s => s.classList.remove('active'));
    document.querySelectorAll('.step-panel').forEach(p => p.classList.remove('active'));
    document.querySelector(`.step[data-step="${n}"]`)?.classList.add('active');
    document.getElementById(`step-${n}`)?.classList.add('active');
    refreshAll();
}
window.goToStep = goToStep;
document.querySelectorAll('.step').forEach(s => {
    s.addEventListener('click', () => goToStep(s.dataset.step));
});

// ============ MODE TABS ============
document.querySelectorAll('.mode-tab').forEach(t => {
    t.addEventListener('click', () => {
        document.querySelectorAll('.mode-tab').forEach(x => x.classList.remove('active'));
        t.classList.add('active');
        document.querySelectorAll('.mode-panel').forEach(p => p.classList.remove('active'));
        document.getElementById('mode-' + t.dataset.mode).classList.add('active');
    });
});

// ============ ALIGN/AUDIO/SIZE BUTTONS ============
document.querySelectorAll('.align-btn').forEach(b => {
    b.addEventListener('click', () => {
        document.querySelectorAll('.align-btn').forEach(x => x.classList.remove('active'));
        b.classList.add('active');
        state.align = b.dataset.align;
    });
});
document.querySelectorAll('.audio-btn').forEach(b => {
    b.addEventListener('click', () => {
        document.querySelectorAll('.audio-btn').forEach(x => {
            x.classList.remove('active');
            x.classList.remove('warn-active');
        });
        b.classList.add('active');
        state.audioPriority = b.dataset.audio;
        refreshMixCounts();
    });
});
document.querySelectorAll('.caption-style-btn').forEach(b => {
    b.addEventListener('click', () => {
        document.querySelectorAll('.caption-style-btn').forEach(x => x.classList.remove('active'));
        b.classList.add('active');
        state.captionStyle = b.dataset.style;
    });
});
document.querySelectorAll('.size-btn').forEach(b => {
    b.addEventListener('click', () => {
        document.querySelectorAll('.size-btn').forEach(x => x.classList.remove('active'));
        b.classList.add('active');
        state.sizeLabel = b.dataset.size;
    });
});

// ============ SCREENSHOT MODE ============
const pasteZone = document.getElementById('paste-zone');
const screenshotInput = document.getElementById('screenshot-input');

document.getElementById('btn-pick-files').addEventListener('click', () => screenshotInput.click());
screenshotInput.addEventListener('change', async (e) => {
    for (const f of e.target.files) await uploadScreenshot(f);
    screenshotInput.value = '';
});

pasteZone.addEventListener('click', (e) => {
    if (e.target.closest('button')) return;
    screenshotInput.click();
});
['dragenter', 'dragover'].forEach(ev =>
    pasteZone.addEventListener(ev, (e) => { e.preventDefault(); pasteZone.classList.add('dragover'); })
);
['dragleave', 'drop'].forEach(ev =>
    pasteZone.addEventListener(ev, (e) => { e.preventDefault(); pasteZone.classList.remove('dragover'); })
);
pasteZone.addEventListener('drop', async (e) => {
    e.preventDefault();
    for (const f of e.dataTransfer.files) {
        if (f.type.startsWith('image/')) await uploadScreenshot(f);
    }
});
document.addEventListener('paste', async (e) => {
    if (!document.getElementById('step-1').classList.contains('active')) return;
    if (!document.getElementById('mode-screenshot').classList.contains('active')) return;
    // Don't intercept paste in editable fields
    const tag = (e.target.tagName || '').toLowerCase();
    if (tag === 'textarea' || tag === 'input') return;
    // Don't intercept when edit modal is open
    if (!document.getElementById('edit-modal').classList.contains('hidden')) return;

    const items = e.clipboardData?.items || [];
    for (const it of items) {
        if (it.type && it.type.startsWith('image/')) {
            const blob = it.getAsFile();
            if (blob) {
                const file = new File([blob], `paste-${Date.now()}.png`, { type: blob.type });
                await uploadScreenshot(file);
            }
        }
    }
});

async function uploadScreenshot(file) {
    const tempId = 'tmp_' + Math.random().toString(36).slice(2, 8);
    addScreenshotPlaceholder(tempId, file.name);

    const fd = new FormData();
    fd.append('file', file);
    fd.append('align', state.align);
    try {
        const r = await fetch(API + '/extractor/screenshot-auto', { method: 'POST', body: fd });
        if (!r.ok) {
            updateScreenshotPlaceholder(tempId, null, 'error');
            toast('Erreur OCR', true);
            return;
        }
        const tpl = await r.json();
        replaceScreenshotPlaceholder(tempId, tpl);
        setStepDone(1);
        refreshTemplates();
        refreshMixCounts();
    } catch (err) {
        updateScreenshotPlaceholder(tempId, null, 'error');
        toast('Erreur réseau', true);
    }
}

function addScreenshotPlaceholder(id, name) {
    const list = document.getElementById('screenshots-list');
    const div = document.createElement('div');
    div.className = 'ss-item';
    div.id = id;
    div.innerHTML = `
        <div class="ss-thumb" style="display:flex;align-items:center;justify-content:center;">⏳</div>
        <div class="ss-text">${escapeHtml(name)} — analyse OCR...</div>
        <div class="ss-status pending">…</div>
    `;
    list.prepend(div);
    document.getElementById('screenshots-header').style.display = 'flex';
    bumpScreenshotCount(1, 0);
}

function updateScreenshotPlaceholder(id, tpl, status) {
    const el = document.getElementById(id);
    if (!el) return;
    if (status === 'error') {
        el.querySelector('.ss-status').className = 'ss-status error';
        el.querySelector('.ss-status').textContent = '✕';
        el.querySelector('.ss-text').textContent = 'Échec OCR';
    }
}

function replaceScreenshotPlaceholder(id, tpl) {
    const el = document.getElementById(id);
    if (!el) return;
    el.id = 'tpl_' + tpl.id;
    el.dataset.tplId = tpl.id;
    renderScreenshotItem(el, tpl);
    bumpScreenshotCount(0, 1);
}

function renderScreenshotItem(el, tpl) {
    const captionShort = (tpl.caption || '').substring(0, 80) + ((tpl.caption || '').length > 80 ? '…' : '');
    el.innerHTML = `
        <img class="ss-thumb" src="${tpl.image_url || ''}" alt="">
        <div class="ss-text">${escapeHtml(captionShort)}</div>
        <div class="ss-status">✓</div>
        <button class="ss-delete" data-tpl-id="${tpl.id}" title="Supprimer">×</button>
    `;
    // Click on item -> open edit modal
    el.addEventListener('click', (e) => {
        if (e.target.classList.contains('ss-delete')) return;
        openEditModal(tpl.id);
    });
    // Delete button
    el.querySelector('.ss-delete').addEventListener('click', async (e) => {
        e.stopPropagation();
        await fetch(API + '/templates/' + tpl.id, { method: 'DELETE' });
        el.remove();
        bumpScreenshotCount(-1, -1);
        refreshTemplates();
        refreshMixCounts();
    });
}

function bumpScreenshotCount(deltaTotal, deltaValid) {
    const cEl = document.getElementById('ss-count');
    const vEl = document.getElementById('ss-validated');
    cEl.textContent = Math.max(0, parseInt(cEl.textContent || '0') + deltaTotal);
    vEl.textContent = Math.max(0, parseInt(vEl.textContent || '0') + deltaValid);
}

// ============ VIDEO EXTRACT MODE ============
const veZone = document.getElementById('video-extract-zone');
const veInput = document.getElementById('video-extract-input');
const veFolderInput = document.getElementById('video-extract-folder');
const veList = document.getElementById('video-extract-list');

document.getElementById('btn-pick-extract-files').addEventListener('click', () => veInput.click());
document.getElementById('btn-pick-extract-folder').addEventListener('click', () => veFolderInput.click());

veInput.addEventListener('change', async (e) => {
    await processVideoExtractFiles(Array.from(e.target.files));
    veInput.value = '';
});
veFolderInput.addEventListener('change', async (e) => {
    const all = Array.from(e.target.files);
    const videos = all.filter(isVideoFile);
    if (videos.length === 0) {
        toast('Aucune vidéo trouvée dans ce dossier', true);
        return;
    }
    toast(`📂 ${videos.length} vidéo(s) trouvée(s)`);
    await processVideoExtractFiles(videos);
    veFolderInput.value = '';
});

veZone.addEventListener('click', (e) => {
    if (e.target.closest('button')) return;
    veInput.click();
});
['dragenter', 'dragover'].forEach(ev =>
    veZone.addEventListener(ev, (e) => { e.preventDefault(); veZone.classList.add('dragover'); })
);
['dragleave', 'drop'].forEach(ev =>
    veZone.addEventListener(ev, (e) => { e.preventDefault(); veZone.classList.remove('dragover'); })
);
veZone.addEventListener('drop', async (e) => {
    e.preventDefault();
    veZone.classList.remove('dragover');
    const files = await getFilesFromDropEvent(e);
    const videos = files.filter(isVideoFile);
    if (videos.length === 0) {
        toast('Aucune vidéo détectée', true);
        return;
    }
    if (videos.length < files.length) {
        toast(`${videos.length} vidéo(s) sur ${files.length} fichier(s)`);
    }
    await processVideoExtractFiles(videos);
});

async function processVideoExtractFiles(files) {
    if (!files.length) return;
    const doText = document.getElementById('opt-extract-text').checked;
    const doMusic = document.getElementById('opt-extract-music').checked;
    if (!doText && !doMusic) {
        toast('Coche au moins une option (texte ou musique)', true);
        return;
    }
    setStepDone(1);

    // Sequential to avoid overwhelming the server (OCR on multiple frames is heavy)
    for (const file of files) {
        const itemId = 've_' + Math.random().toString(36).slice(2, 8);
        addVideoExtractItem(itemId, file.name);
        try {
            const fd = new FormData();
            fd.append('file', file);
            fd.append('extract_text', doText ? 'true' : 'false');
            fd.append('extract_music', doMusic ? 'true' : 'false');
            fd.append('align', state.align);
            const r = await fetch(API + '/extractor/video-extract', { method: 'POST', body: fd });
            if (!r.ok) {
                markVideoExtractFailed(itemId, `HTTP ${r.status}`);
                continue;
            }
            const data = await r.json();
            markVideoExtractDone(itemId, data);
        } catch (err) {
            markVideoExtractFailed(itemId, err.message || 'erreur');
        }
    }
    refreshAll();
}

function addVideoExtractItem(id, name) {
    const div = document.createElement('div');
    div.className = 've-item processing';
    div.id = id;
    div.innerHTML = `
        <div class="ve-icon">🎬</div>
        <div class="ve-content">
            <div class="ve-name">${escapeHtml(name)}</div>
            <div class="ve-result">Analyse en cours…</div>
        </div>
        <div class="ve-spinner"></div>
    `;
    veList.prepend(div);
}

function markVideoExtractDone(id, data) {
    const el = document.getElementById(id);
    if (!el) return;
    const hasText = !!data.template;
    const hasMusic = !!data.music;
    const hasErrors = (data.errors || []).length > 0;
    let cls = 've-item ';
    if (hasText || hasMusic) {
        cls += hasErrors ? 'partial' : 'success';
    } else {
        cls += 'failed';
    }
    el.className = cls;

    const tags = [];
    if (hasText) tags.push('<span class="ve-tag tag-text">📋 Template</span>');
    if (hasMusic) tags.push('<span class="ve-tag tag-music">🎵 Musique</span>');
    (data.errors || []).forEach(e => {
        tags.push(`<span class="ve-tag tag-error">⚠ ${escapeHtml(e.substring(0, 40))}</span>`);
    });

    const captionPreview = data.caption
        ? escapeHtml(data.caption.substring(0, 100)) + (data.caption.length > 100 ? '…' : '')
        : '<em style="opacity:0.7;">Aucun texte détecté</em>';

    el.innerHTML = `
        <div class="ve-icon">${hasText && hasMusic ? '✓' : (hasText ? '💬' : (hasMusic ? '🎵' : '✕'))}</div>
        <div class="ve-content">
            <div class="ve-name">${escapeHtml(data.filename || '')}</div>
            <div class="ve-result">${captionPreview}</div>
            <div class="ve-tags">${tags.join('')}</div>
        </div>
    `;
    if (data.template) {
        el.dataset.tplId = data.template.id;
        el.addEventListener('click', () => openEditModal(data.template.id));
    }
}

function markVideoExtractFailed(id, err) {
    const el = document.getElementById(id);
    if (!el) return;
    el.className = 've-item failed';
    el.innerHTML = `
        <div class="ve-icon">✕</div>
        <div class="ve-content">
            <div class="ve-name">${el.querySelector('.ve-name')?.textContent || ''}</div>
            <div class="ve-result" style="color:var(--red);">Échec : ${escapeHtml(err)}</div>
        </div>
    `;
}

// ============ MANUAL MODE ============
document.getElementById('btn-create-template').addEventListener('click', async () => {
    const caption = document.getElementById('caption-text').value.trim();
    const musicName = document.getElementById('music-name').value.trim();
    if (!caption) { toast('Caption vide', true); return; }
    const fd = new FormData();
    fd.append('caption', caption);
    fd.append('music_name', musicName);
    fd.append('align', state.align);
    const r = await fetch(API + '/extractor/manual', { method: 'POST', body: fd });
    if (!r.ok) { toast('Erreur', true); return; }
    toast('✓ Template créé');
    document.getElementById('caption-text').value = '';
    document.getElementById('music-name').value = '';
    setStepDone(1);
    refreshAll();
});

// ============ EDIT MODAL ============
async function openEditModal(tplId) {
    state.editingTplId = tplId;
    // Fetch latest template data
    const r = await fetch(API + '/templates/');
    const tpls = await r.json();
    const tpl = tpls.find(t => t.id === tplId);
    if (!tpl) { toast('Template introuvable', true); return; }

    // Populate
    document.getElementById('edit-caption').value = tpl.caption || '';
    document.getElementById('edit-music-name').value = tpl.music_name || '';

    // Image preview (or hide if none)
    const wrap = document.getElementById('edit-preview-wrap');
    const img = document.getElementById('edit-thumb');
    if (tpl.image_url) {
        img.src = tpl.image_url;
        wrap.style.display = '';
    } else {
        wrap.style.display = 'none';
    }

    // Align buttons
    document.querySelectorAll('.align-btn-edit').forEach(b => {
        b.classList.toggle('active', b.dataset.align === (tpl.align || 'center'));
    });

    // Build emoji rows
    buildEmojiQuickRow('edit-emoji-quick', 'edit-caption');
    buildEmojiPicker();

    // Reset full picker visibility
    document.getElementById('edit-emoji-picker').classList.add('hidden');

    document.getElementById('edit-modal').classList.remove('hidden');
}

function closeEditModal() {
    document.getElementById('edit-modal').classList.add('hidden');
    state.editingTplId = null;
}

document.getElementById('edit-close').addEventListener('click', closeEditModal);
document.getElementById('edit-modal').addEventListener('click', (e) => {
    if (e.target.id === 'edit-modal') closeEditModal();
});

// Edit align buttons
document.querySelectorAll('.align-btn-edit').forEach(b => {
    b.addEventListener('click', () => {
        document.querySelectorAll('.align-btn-edit').forEach(x => x.classList.remove('active'));
        b.classList.add('active');
    });
});

document.getElementById('btn-emoji-more').addEventListener('click', () => {
    document.getElementById('edit-emoji-picker').classList.toggle('hidden');
});

// Emoji tabs
document.querySelectorAll('.emoji-tab').forEach(b => {
    b.addEventListener('click', () => {
        document.querySelectorAll('.emoji-tab').forEach(x => x.classList.remove('active'));
        b.classList.add('active');
        renderEmojiGrid(b.dataset.cat);
    });
});

function buildEmojiPicker() {
    renderEmojiGrid('faces');
}

function renderEmojiGrid(category) {
    const grid = document.getElementById('emoji-grid');
    grid.innerHTML = '';
    const emojis = EMOJI_CATEGORIES[category] || [];
    emojis.forEach(emoji => {
        const btn = document.createElement('button');
        btn.type = 'button';
        btn.className = 'emoji-cell';
        btn.textContent = emoji;
        btn.addEventListener('click', () => insertEmoji('edit-caption', emoji));
        grid.appendChild(btn);
    });
}

document.getElementById('edit-save').addEventListener('click', async () => {
    if (!state.editingTplId) return;
    const caption = document.getElementById('edit-caption').value.trim();
    if (!caption) { toast('Caption vide', true); return; }
    const align = document.querySelector('.align-btn-edit.active')?.dataset.align || 'center';
    const musicName = document.getElementById('edit-music-name').value.trim();

    const r = await fetch(API + '/templates/' + state.editingTplId, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ caption, align, music_name: musicName }),
    });
    if (!r.ok) { toast('Erreur sauvegarde', true); return; }
    toast('✓ Template modifié');

    // Update the screenshot card if visible
    const updated = await r.json();
    const ssEl = document.getElementById('tpl_' + state.editingTplId);
    if (ssEl) renderScreenshotItem(ssEl, updated);

    closeEditModal();
    refreshTemplates();
});

document.getElementById('edit-delete').addEventListener('click', async () => {
    if (!state.editingTplId) return;
    if (!confirm('Supprimer ce template ?')) return;
    await fetch(API + '/templates/' + state.editingTplId, { method: 'DELETE' });
    const ssEl = document.getElementById('tpl_' + state.editingTplId);
    if (ssEl) {
        ssEl.remove();
        bumpScreenshotCount(-1, -1);
    }
    closeEditModal();
    refreshAll();
});

// ============ TEMPLATES (Step 2) ============
async function refreshTemplates() {
    const r = await fetch(API + '/templates/');
    const list = await r.json();
    const total = list.length;
    const selected = list.filter(t => t.is_selected !== false).length;

    document.getElementById('tpl-count').textContent = total;
    document.getElementById('tpl-selected-count').textContent = selected;
    document.getElementById('export-count').textContent = selected;

    // Selected pill
    const pill = document.getElementById('tpl-selected-pill');
    pill.style.display = total > 0 ? '' : 'none';

    const container = document.getElementById('templates-list');
    if (total === 0) {
        container.innerHTML = `
            <div class="empty-state">
                <div>📋</div>
                <div>Aucun template</div>
                <button class="btn btn-cyan" onclick="goToStep(1)">Aller à l'extracteur</button>
            </div>`;
        document.getElementById('tpl-preview-card').style.display = 'none';
        document.getElementById('tpl-ready-wrap').style.display = 'none';
        return;
    }

    container.innerHTML = list.map(t => {
        const isSel = t.is_selected !== false;
        const isFav = !!t.is_favorite;
        const thumb = t.image_url
            ? `<img class="tpl-thumb" src="${t.image_url}" alt="">`
            : `<div class="tpl-thumb">📋</div>`;
        const filename = t.original_name || (t.source === 'video' ? '🎬 vidéo' : 'manuel');
        return `
        <div class="tpl-row ${isSel ? 'selected' : 'deselected'}" data-tpl-id="${t.id}">
            <input type="checkbox" class="tpl-check" ${isSel ? 'checked' : ''} onchange="toggleSelect('${t.id}', this.checked)">
            ${thumb}
            <div class="tpl-content">
                <div class="tpl-caption">${escapeHtml(t.caption)}</div>
                <div class="tpl-meta">📷 ${escapeHtml(filename)}</div>
            </div>
            <button class="tpl-fav ${isFav ? 'active' : ''}" onclick="toggleFavorite('${t.id}', ${!isFav})" title="${isFav ? 'Retirer favori' : 'Ajouter aux favoris'}">${isFav ? '★' : '☆'}</button>
            <div class="tpl-actions-right">
                <button class="tpl-btn-edit" onclick="openEditModal('${t.id}')">✏️ Edit</button>
                <button class="tpl-btn-del" onclick="deleteTemplate('${t.id}')">×</button>
            </div>
        </div>`;
    }).join('');

    // Preview pills (selected only)
    const selectedTpls = list.filter(t => t.is_selected !== false);
    const previewCard = document.getElementById('tpl-preview-card');
    const previewPills = document.getElementById('tpl-preview-pills');
    const readyWrap = document.getElementById('tpl-ready-wrap');
    if (selectedTpls.length > 0) {
        previewCard.style.display = '';
        readyWrap.style.display = '';
        document.getElementById('tpl-ready-count').textContent = selectedTpls.length;
        previewPills.innerHTML = selectedTpls.map(t => {
            const short = (t.caption || '').substring(0, 30) + ((t.caption || '').length > 30 ? '…' : '');
            return `<div class="tpl-preview-pill">${escapeHtml(short)}</div>`;
        }).join('');
    } else {
        previewCard.style.display = 'none';
        readyWrap.style.display = 'none';
    }

    if (total > 0) setStepDone(2);
}

window.openEditModal = openEditModal;

window.toggleSelect = async (id, checked) => {
    await fetch(API + '/templates/' + id, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ is_selected: checked }),
    });
    refreshAll();
};

window.toggleFavorite = async (id, favorite) => {
    await fetch(API + '/templates/' + id, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ is_favorite: favorite }),
    });
    refreshTemplates();
};

window.deleteTemplate = async (id) => {
    if (!confirm('Supprimer ce template ?')) return;
    await fetch(API + '/templates/' + id, { method: 'DELETE' });
    refreshAll();
};

// Toolbar actions
document.getElementById('btn-select-none').addEventListener('click', async () => {
    await fetch(API + '/templates/select-all', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ mode: 'none' }),
    });
    toast('Tout désélectionné');
    refreshAll();
});
document.getElementById('btn-select-favorites').addEventListener('click', async () => {
    await fetch(API + '/templates/select-all', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ mode: 'favorites' }),
    });
    toast('⭐ Favoris sélectionnés');
    refreshAll();
});
document.getElementById('btn-select-reset').addEventListener('click', async () => {
    await fetch(API + '/templates/select-all', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ mode: 'reset' }),
    });
    toast('↻ Reset');
    refreshAll();
});

document.getElementById('btn-clear-tpl').addEventListener('click', async () => {
    if (!confirm('Vider tous les templates ?')) return;
    await fetch(API + '/templates/', { method: 'DELETE' });
    refreshAll();
});

document.getElementById('btn-export-tpl').addEventListener('click', async () => {
    const r = await fetch(API + '/templates/');
    const list = await r.json();
    const selected = list.filter(t => t.is_selected !== false);
    if (!selected.length) {
        toast('Rien à exporter (aucun sélectionné)', true);
        return;
    }
    const blob = new Blob([JSON.stringify({ templates: selected }, null, 2)], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url; a.download = 'clipfusion-templates.json'; a.click();
});

document.getElementById('btn-import-tpl').addEventListener('click', () => {
    document.getElementById('import-input').click();
});
document.getElementById('import-input').addEventListener('change', async (e) => {
    const file = e.target.files[0];
    if (!file) return;
    try {
        const text = await file.text();
        const data = JSON.parse(text);
        const r = await fetch(API + '/templates/import', {
            method: 'POST', headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(data),
        });
        const out = await r.json();
        toast(`✓ ${out.imported} template(s) importé(s)`);
        refreshAll();
    } catch (err) {
        toast('Fichier invalide', true);
    }
    e.target.value = '';
});

// ============ CUSTOM TEMPLATE MODAL ============
const customModal = document.getElementById('custom-modal');
const customImageInput = document.getElementById('custom-image-input');
const customImagePreview = document.getElementById('custom-image-preview');
const customImagePlaceholder = document.getElementById('custom-image-placeholder');
const customImageRemove = document.getElementById('custom-image-remove');
const customImageZone = document.getElementById('custom-image-zone');
let customImageFile = null;
let customAlign = 'center';

document.getElementById('btn-custom-template').addEventListener('click', openCustomModal);
document.getElementById('custom-close').addEventListener('click', closeCustomModal);
document.getElementById('custom-cancel').addEventListener('click', closeCustomModal);
customModal.addEventListener('click', (e) => {
    if (e.target === customModal) closeCustomModal();
});

function openCustomModal() {
    document.getElementById('custom-caption').value = '';
    document.getElementById('custom-music-name').value = '';
    customImageFile = null;
    customAlign = 'center';
    customImagePreview.classList.add('hidden');
    customImagePreview.src = '';
    customImagePlaceholder.classList.remove('hidden');
    customImageRemove.classList.add('hidden');
    customImageInput.value = '';
    document.querySelectorAll('.align-btn-custom').forEach(b => {
        b.classList.toggle('active', b.dataset.align === 'center');
    });
    buildEmojiQuickRow('custom-emoji-quick', 'custom-caption');
    customModal.classList.remove('hidden');
}

function closeCustomModal() {
    customModal.classList.add('hidden');
}

document.querySelectorAll('.align-btn-custom').forEach(b => {
    b.addEventListener('click', () => {
        document.querySelectorAll('.align-btn-custom').forEach(x => x.classList.remove('active'));
        b.classList.add('active');
        customAlign = b.dataset.align;
    });
});

customImageZone.addEventListener('click', (e) => {
    if (e.target === customImageRemove) return;
    customImageInput.click();
});
customImageInput.addEventListener('change', (e) => {
    const file = e.target.files[0];
    if (!file || !file.type.startsWith('image/')) return;
    setCustomImage(file);
});
['dragenter','dragover'].forEach(ev =>
    customImageZone.addEventListener(ev, (e) => { e.preventDefault(); customImageZone.classList.add('dragover'); })
);
['dragleave','drop'].forEach(ev =>
    customImageZone.addEventListener(ev, (e) => { e.preventDefault(); customImageZone.classList.remove('dragover'); })
);
customImageZone.addEventListener('drop', (e) => {
    e.preventDefault();
    const file = e.dataTransfer.files[0];
    if (file && file.type.startsWith('image/')) setCustomImage(file);
});

function setCustomImage(file) {
    customImageFile = file;
    const reader = new FileReader();
    reader.onload = (e) => {
        customImagePreview.src = e.target.result;
        customImagePreview.classList.remove('hidden');
        customImagePlaceholder.classList.add('hidden');
        customImageRemove.classList.remove('hidden');
    };
    reader.readAsDataURL(file);
}

customImageRemove.addEventListener('click', (e) => {
    e.stopPropagation();
    customImageFile = null;
    customImagePreview.src = '';
    customImagePreview.classList.add('hidden');
    customImagePlaceholder.classList.remove('hidden');
    customImageRemove.classList.add('hidden');
    customImageInput.value = '';
});

document.getElementById('custom-save').addEventListener('click', async () => {
    const caption = document.getElementById('custom-caption').value.trim();
    if (!caption) { toast('Caption vide', true); return; }
    const fd = new FormData();
    fd.append('caption', caption);
    fd.append('align', customAlign);
    fd.append('music_name', document.getElementById('custom-music-name').value.trim());
    if (customImageFile) fd.append('image', customImageFile);

    const r = await fetch(API + '/templates/custom', { method: 'POST', body: fd });
    if (!r.ok) { toast('Erreur création', true); return; }
    toast('✓ Template custom créé');
    closeCustomModal();
    setStepDone(2);
    refreshAll();
});

// ============ VIDEOS ============
const VIDEO_EXTS = ['.mp4', '.mov', '.m4v', '.webm', '.avi', '.mkv'];
function isVideoFile(file) {
    if (file.type && file.type.startsWith('video/')) return true;
    const name = (file.name || '').toLowerCase();
    return VIDEO_EXTS.some(ext => name.endsWith(ext));
}

const videoDropZone = document.getElementById('video-drop-zone');
const videoInput = document.getElementById('video-input');
const videoFolderInput = document.getElementById('video-folder-input');

document.getElementById('btn-pick-videos').addEventListener('click', () => videoInput.click());
document.getElementById('btn-pick-folder').addEventListener('click', () => videoFolderInput.click());

videoInput.addEventListener('change', async (e) => {
    await uploadVideoFiles(Array.from(e.target.files));
    videoInput.value = '';
});
videoFolderInput.addEventListener('change', async (e) => {
    const all = Array.from(e.target.files);
    const videos = all.filter(isVideoFile);
    if (videos.length === 0) {
        toast('Aucune vidéo trouvée dans ce dossier', true);
        return;
    }
    toast(`📂 ${videos.length} vidéo(s) trouvée(s) dans le dossier`);
    await uploadVideoFiles(videos);
    videoFolderInput.value = '';
});

videoDropZone.addEventListener('click', (e) => {
    if (e.target.closest('button')) return;
    videoInput.click();
});

['dragenter', 'dragover'].forEach(ev =>
    videoDropZone.addEventListener(ev, (e) => { e.preventDefault(); videoDropZone.classList.add('dragover'); })
);
['dragleave', 'drop'].forEach(ev =>
    videoDropZone.addEventListener(ev, (e) => { e.preventDefault(); videoDropZone.classList.remove('dragover'); })
);

videoDropZone.addEventListener('drop', async (e) => {
    e.preventDefault();
    videoDropZone.classList.remove('dragover');
    const files = await getFilesFromDropEvent(e);
    const videos = files.filter(isVideoFile);
    if (videos.length === 0) {
        toast('Aucune vidéo détectée', true);
        return;
    }
    if (videos.length < files.length) {
        toast(`${videos.length} vidéo(s) sur ${files.length} fichier(s)`);
    }
    await uploadVideoFiles(videos);
});

// Recursively walk a dropped folder using webkitGetAsEntry API
async function getFilesFromDropEvent(e) {
    const items = e.dataTransfer?.items;
    if (!items) return Array.from(e.dataTransfer.files || []);

    const files = [];
    const promises = [];
    for (const item of items) {
        const entry = item.webkitGetAsEntry?.();
        if (entry) {
            promises.push(walkEntry(entry, files));
        } else if (item.kind === 'file') {
            const f = item.getAsFile();
            if (f) files.push(f);
        }
    }
    await Promise.all(promises);
    return files;
}

function walkEntry(entry, outFiles) {
    return new Promise((resolve) => {
        if (entry.isFile) {
            entry.file((file) => { outFiles.push(file); resolve(); }, () => resolve());
        } else if (entry.isDirectory) {
            const reader = entry.createReader();
            const allEntries = [];
            const readBatch = () => {
                reader.readEntries(async (batch) => {
                    if (!batch.length) {
                        await Promise.all(allEntries.map(en => walkEntry(en, outFiles)));
                        resolve();
                    } else {
                        allEntries.push(...batch);
                        readBatch();
                    }
                }, () => resolve());
            };
            readBatch();
        } else {
            resolve();
        }
    });
}

// Upload list with progress per file
async function uploadVideoFiles(files) {
    if (!files.length) return;
    setStepDone(3);
    const queue = document.getElementById('upload-progress-list');

    // Concurrency: 3 parallel uploads max
    const POOL = 3;
    let cursor = 0;
    let okCount = 0;
    let failCount = 0;

    const workers = Array(Math.min(POOL, files.length)).fill(0).map(async () => {
        while (cursor < files.length) {
            const idx = cursor++;
            const file = files[idx];
            const itemId = 'up_' + Math.random().toString(36).slice(2, 8);
            addUploadItem(itemId, file.name, file.size);
            try {
                await uploadOne(file, itemId);
                okCount++;
                markUploadDone(itemId);
            } catch (err) {
                failCount++;
                markUploadFailed(itemId, err.message || 'erreur');
            }
        }
    });

    await Promise.all(workers);

    if (okCount > 0) toast(`✓ ${okCount} vidéo(s) uploadée(s)${failCount ? ` · ${failCount} échec(s)` : ''}`);
    if (okCount === 0 && failCount > 0) toast(`✕ ${failCount} échec(s)`, true);

    // Cleanup successful items after a moment
    setTimeout(() => {
        document.querySelectorAll('.upload-item.done').forEach(el => el.remove());
    }, 1500);

    refreshAll();
}

function uploadOne(file, itemId) {
    return new Promise((resolve, reject) => {
        // Vérification : catégorie obligatoire pour upload
        if (!state.contentModelId) {
            reject(new Error('Catégorie obligatoire — sélectionne d\'abord un modèle'));
            return;
        }
        const fd = new FormData();
        fd.append('files', file);
        fd.append('model_id', state.contentModelId);
        const xhr = new XMLHttpRequest();
        xhr.open('POST', API + '/content/upload');
        xhr.upload.onprogress = (e) => {
            if (e.lengthComputable) {
                const pct = Math.round((e.loaded / e.total) * 100);
                updateUploadProgress(itemId, pct);
            }
        };
        xhr.onload = () => {
            if (xhr.status >= 200 && xhr.status < 300) resolve();
            else {
                let detail = 'HTTP ' + xhr.status;
                try { detail = JSON.parse(xhr.responseText).detail || detail; } catch(_){}
                reject(new Error(detail));
            }
        };
        xhr.onerror = () => reject(new Error('réseau'));
        xhr.send(fd);
    });
}

function addUploadItem(id, name, size) {
    const list = document.getElementById('upload-progress-list');
    const sizeStr = formatSize(size);
    const div = document.createElement('div');
    div.className = 'upload-item';
    div.id = id;
    div.innerHTML = `
        <div class="upload-row">
            <div class="upload-name">📤 ${escapeHtml(name)}</div>
            <div class="upload-meta"><span class="upload-pct">0%</span> · <span class="muted">${sizeStr}</span></div>
        </div>
        <div class="progress-bar"><div class="progress-fill upload-fill" style="width:0%"></div></div>
    `;
    list.appendChild(div);
}

function updateUploadProgress(id, pct) {
    const el = document.getElementById(id);
    if (!el) return;
    el.querySelector('.upload-pct').textContent = pct + '%';
    el.querySelector('.upload-fill').style.width = pct + '%';
}

function markUploadDone(id) {
    const el = document.getElementById(id);
    if (!el) return;
    el.classList.add('done');
    el.querySelector('.upload-name').innerHTML = el.querySelector('.upload-name').innerHTML.replace('📤', '✓');
    el.querySelector('.upload-pct').textContent = '100%';
    el.querySelector('.upload-fill').style.width = '100%';
}

function markUploadFailed(id, err) {
    const el = document.getElementById(id);
    if (!el) return;
    el.classList.add('failed');
    el.querySelector('.upload-name').innerHTML = '✕ ' + el.querySelector('.upload-name').textContent.replace('📤', '').trim();
    el.querySelector('.upload-meta').innerHTML = `<span style="color:var(--red)">échec : ${escapeHtml(err)}</span>`;
}

function formatSize(bytes) {
    if (!bytes) return '?';
    const units = ['o', 'Ko', 'Mo', 'Go'];
    let i = 0;
    let n = bytes;
    while (n >= 1024 && i < units.length - 1) { n /= 1024; i++; }
    return n.toFixed(n >= 10 || i === 0 ? 0 : 1) + ' ' + units[i];
}
async function refreshVideos() {
    // Applique le filtre catégorie si défini
    const filterParam = state.contentFilterModelId
        ? '?model_id=' + encodeURIComponent(state.contentFilterModelId)
        : '';
    const r = await fetch(API + '/content/' + filterParam);
    const list = await r.json();
    document.getElementById('vid-count').textContent = list.length;
    const container = document.getElementById('videos-list');
    const scanBtn = document.getElementById('btn-scan');
    if (scanBtn) scanBtn.disabled = list.length === 0;

    if (list.length === 0) {
        const filterMsg = state.contentFilterModelId
            ? '<div>Aucune vidéo dans cette catégorie</div>'
            : '<div>Aucune vidéo brute</div>';
        container.innerHTML = `<div class="empty-state"><div>🎬</div>${filterMsg}</div>`;
        return;
    }
    container.innerHTML = list.map((v, i) => {
        const num = String(i + 1).padStart(3, '0');
        const date = new Date(v.created_at + 'Z').toLocaleString('fr-FR', {
            day: '2-digit', month: '2-digit', year: 'numeric',
            hour: '2-digit', minute: '2-digit'
        });
        return `
        <div class="vid-row" data-vid-id="${v.id}">
            <div class="vid-icon">🎬</div>
            <div class="vid-content">
                <div class="vid-title">Vidéo_${num}.mp4</div>
                <div class="vid-original">${escapeHtml(v.original_name || v.filename)}</div>
                <div class="vid-size">${formatSize(v.size || 0)} · ${escapeHtml(date)}</div>
            </div>
            <button class="vid-delete" onclick="deleteVideo('${v.id}')" title="Supprimer">×</button>
        </div>`;
    }).join('');
    if (list.length > 0) setStepDone(3);
}
window.deleteVideo = async (id) => {
    await fetch(API + '/content/' + id, { method: 'DELETE' });
    refreshAll();
};
document.getElementById('btn-clear-vids').addEventListener('click', async () => {
    if (!confirm('Supprimer toutes les vidéos ?')) return;
    await fetch(API + '/content/', { method: 'DELETE' });
    refreshAll();
});

// ============ FILTER PILLS (Step 3) ============
const filterState = {
    horizontal: true,
    talking: true,
    captions: true,
};

document.querySelectorAll('.filter-pill').forEach(p => {
    p.addEventListener('click', () => {
        const f = p.dataset.filter;
        filterState[f] = !filterState[f];
        p.classList.toggle('active', filterState[f]);
        const stateEl = p.querySelector('.f-state');
        if (stateEl) stateEl.textContent = filterState[f] ? 'ON' : 'OFF';
    });
});

// Folder loader (just stores the folder name for display, then triggers upload)
document.getElementById('btn-load-folder').addEventListener('click', () => {
    document.getElementById('folder-loader').click();
});
document.getElementById('folder-loader').addEventListener('change', async (e) => {
    const all = Array.from(e.target.files);
    if (all.length === 0) return;
    // Store the folder name for display
    const firstFile = all[0];
    const folderPath = firstFile.webkitRelativePath?.split('/')[0] || 'dossier';
    document.getElementById('folder-path-display').value = folderPath;
    // Filter videos and upload
    const videos = all.filter(isVideoFile);
    if (videos.length === 0) {
        toast('Aucune vidéo trouvée dans ce dossier', true);
        return;
    }
    toast(`📂 ${videos.length} vidéo(s) trouvée(s) dans "${folderPath}"`);
    await uploadVideoFiles(videos);
    e.target.value = '';
});

// Drag & drop sur la 1ère card "Dossier des vidéos brutes"
// Permet de drop des fichiers ou un dossier directement sur la card top
const folderRow = document.querySelector('#step-3 .folder-row');
const folderCard = document.querySelector('#step-3 .card');
const folderPathDisplay = document.getElementById('folder-path-display');
[folderRow, folderCard, folderPathDisplay].forEach(zone => {
    if (!zone) return;
    ['dragenter', 'dragover'].forEach(ev =>
        zone.addEventListener(ev, (e) => {
            e.preventDefault();
            e.stopPropagation();
            folderCard.classList.add('dragover-card');
        })
    );
    ['dragleave', 'drop'].forEach(ev =>
        zone.addEventListener(ev, (e) => {
            e.preventDefault();
            e.stopPropagation();
            folderCard.classList.remove('dragover-card');
        })
    );
});
folderCard?.addEventListener('drop', async (e) => {
    e.preventDefault();
    e.stopPropagation();
    folderCard.classList.remove('dragover-card');
    const files = await getFilesFromDropEvent(e);
    if (!files || files.length === 0) return;
    const videos = files.filter(isVideoFile);
    if (videos.length === 0) {
        toast('Aucune vidéo détectée dans ce drop', true);
        return;
    }
    // Si plusieurs fichiers d'un dossier, on affiche le nom du dossier
    const firstWithRel = videos.find(v => v.webkitRelativePath);
    if (firstWithRel) {
        const folderPath = firstWithRel.webkitRelativePath.split('/')[0];
        folderPathDisplay.value = folderPath;
    } else {
        folderPathDisplay.value = `${videos.length} fichier(s) déposé(s)`;
    }
    toast(`📥 ${videos.length} vidéo(s) reçue(s)`);
    await uploadVideoFiles(videos);
});

// Bloque la navigation par défaut quand on drop des fichiers à côté d'une zone
// (sinon le navigateur ouvre la vidéo et on quitte la page ClipFusion)
window.addEventListener('dragover', (e) => e.preventDefault());
window.addEventListener('drop', (e) => e.preventDefault());

// Tout charger -> just shortcut to file picker (multi-file)
document.getElementById('btn-load-all').addEventListener('click', () => {
    document.getElementById('video-input').click();
});

// Scanner & trier
document.getElementById('btn-scan').addEventListener('click', async () => {
    if (!filterState.horizontal && !filterState.talking && !filterState.captions) {
        toast('Active au moins un filtre', true);
        return;
    }
    if (!confirm('Lancer le scan & supprimer les vidéos qui ne passent pas les filtres actifs ?\n\nCela peut prendre 1-2 min selon le nombre de vidéos.')) return;

    const btn = document.getElementById('btn-scan');
    btn.disabled = true;
    btn.textContent = '⏳ Scan en cours...';
    toast('🔍 Scan & tri en cours, patiente...');

    try {
        const r = await fetch(API + '/content/filter', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                filter_horizontal: filterState.horizontal,
                filter_talking: filterState.talking,
                filter_captions: filterState.captions,
            }),
        });
        if (!r.ok) {
            toast('Erreur scan', true);
            btn.textContent = '🔍 SCANNER & TRIER';
            btn.disabled = false;
            return;
        }
        const data = await r.json();
        toast(`✓ Scan terminé : ${data.kept} gardée(s), ${data.dropped} supprimée(s)`);

        // Show details if anything dropped
        if (data.dropped > 0) {
            const dropped = data.details.filter(d => !d.kept);
            const summary = dropped.map(d => `• ${d.original_name} : ${d.reasons_dropped.join(', ')}`).join('\n');
            console.log('Dropped videos:\n' + summary);
        }
    } catch (err) {
        toast('Erreur réseau', true);
    } finally {
        btn.textContent = '🔍 SCANNER & TRIER';
        refreshAll();
    }
});

// ============ MUSIC ============
const btnAddMp3 = document.getElementById('btn-add-mp3');
const btnExtractAudio = document.getElementById('btn-extract-audio');
const musicMp3Input = document.getElementById('music-mp3-input');
const musicVideoInput = document.getElementById('music-video-input');
const musicDropZone = document.getElementById('music-drop-zone');

btnAddMp3.addEventListener('click', () => musicMp3Input.click());
btnExtractAudio.addEventListener('click', () => musicVideoInput.click());

musicMp3Input.addEventListener('change', async (e) => {
    await uploadMusicFiles(Array.from(e.target.files));
    musicMp3Input.value = '';
});
musicVideoInput.addEventListener('change', async (e) => {
    await uploadMusicFiles(Array.from(e.target.files));
    musicVideoInput.value = '';
});

['dragenter', 'dragover'].forEach(ev =>
    musicDropZone.addEventListener(ev, (e) => { e.preventDefault(); musicDropZone.classList.add('dragover'); })
);
['dragleave', 'drop'].forEach(ev =>
    musicDropZone.addEventListener(ev, (e) => { e.preventDefault(); musicDropZone.classList.remove('dragover'); })
);
musicDropZone.addEventListener('drop', async (e) => {
    e.preventDefault();
    musicDropZone.classList.remove('dragover');
    const files = Array.from(e.dataTransfer.files);
    const valid = files.filter(f => f.type.startsWith('audio/') || f.type.startsWith('video/')
        || /\.(mp3|wav|m4a|aac|ogg|mp4|mov|webm|mkv|avi)$/i.test(f.name));
    if (valid.length === 0) {
        toast('Aucun fichier audio/vidéo', true);
        return;
    }
    await uploadMusicFiles(valid);
});

async function uploadMusicFiles(files) {
    if (!files.length) return;
    const fd = new FormData();
    for (const f of files) fd.append('files', f);
    toast(`📤 Upload ${files.length} fichier(s)...`);
    const r = await fetch(API + '/music/upload', { method: 'POST', body: fd });
    if (!r.ok) { toast('Erreur upload', true); return; }
    const data = await r.json();
    toast(`✓ ${data.saved.length} musique(s) ajoutée(s)`);
    setStepDone(4);
    refreshAll();
}

document.getElementById('btn-clear-music').addEventListener('click', async () => {
    if (!confirm('Supprimer toutes les musiques ?')) return;
    const r = await fetch(API + '/music/');
    const list = await r.json();
    for (const m of list) {
        await fetch(API + '/music/' + m.id, { method: 'DELETE' });
    }
    refreshAll();
});

async function refreshMusic() {
    const r = await fetch(API + '/music/');
    const list = await r.json();
    document.getElementById('music-count').textContent = list.length;
    const container = document.getElementById('music-list');
    const emptyEl = document.getElementById('music-empty');
    const clearBtn = document.getElementById('btn-clear-music');
    if (list.length === 0) {
        container.innerHTML = '';
        if (emptyEl) emptyEl.style.display = '';
        if (clearBtn) clearBtn.style.display = 'none';
        return;
    }
    if (emptyEl) emptyEl.style.display = 'none';
    if (clearBtn) clearBtn.style.display = '';
    container.innerHTML = list.map(m => `
        <div class="vid-row">
            <div class="vid-icon" style="background:linear-gradient(135deg,var(--cyan),var(--blue));box-shadow:0 0 12px rgba(34,211,238,0.3);">🎵</div>
            <div class="vid-content">
                <div class="vid-title">${escapeHtml(m.original_name)}</div>
                <div class="vid-size">${escapeHtml(new Date(m.created_at + 'Z').toLocaleString('fr-FR'))}</div>
            </div>
            <button class="vid-delete" onclick="deleteMusic('${m.id}')" title="Supprimer">×</button>
        </div>`).join('');
    if (list.length > 0) setStepDone(4);
}
window.deleteMusic = async (id) => {
    await fetch(API + '/music/' + id, { method: 'DELETE' });
    refreshAll();
};

// ============ MIX ============
const maxVariantsSlider = document.getElementById('max-variants');
maxVariantsSlider.addEventListener('input', (e) => {
    state.maxVariants = parseInt(e.target.value);
    document.getElementById('max-variants-val').textContent = state.maxVariants;
    refreshMixCounts();
});

// Position slider
const posSlider = document.getElementById('pos-slider');
posSlider.addEventListener('input', (e) => {
    state.positionPct = parseInt(e.target.value);
    document.getElementById('pos-pct-val').textContent = state.positionPct;
    updatePhonePreview();
    syncPositionButtons();
});
document.querySelectorAll('.align-btn-mix').forEach(b => {
    b.addEventListener('click', () => {
        const pos = parseInt(b.dataset.pos);
        state.positionPct = pos;
        posSlider.value = pos;
        document.getElementById('pos-pct-val').textContent = pos;
        document.querySelectorAll('.align-btn-mix').forEach(x => x.classList.remove('active'));
        b.classList.add('active');
        updatePhonePreview();
    });
});
function syncPositionButtons() {
    document.querySelectorAll('.align-btn-mix').forEach(x => x.classList.remove('active'));
    // Highlight the closest preset
    const presets = { top: 12, center: 50, tiktok: 70, bottom: 85 };
    let closest = null, minDiff = 9999;
    for (const btn of document.querySelectorAll('.align-btn-mix')) {
        const v = parseInt(btn.dataset.pos);
        if (Math.abs(v - state.positionPct) <= 5 && Math.abs(v - state.positionPct) < minDiff) {
            closest = btn;
            minDiff = Math.abs(v - state.positionPct);
        }
    }
    if (closest) closest.classList.add('active');
}

// Size slider
const sizeSlider = document.getElementById('size-slider');
sizeSlider.addEventListener('input', (e) => {
    state.fontSizePx = parseInt(e.target.value);
    document.getElementById('size-px-val').textContent = state.fontSizePx;
    updatePhonePreview();
    syncSizeButtons();
});
document.querySelectorAll('.size-btn-mix').forEach(b => {
    b.addEventListener('click', () => {
        const px = parseInt(b.dataset.size);
        state.fontSizePx = px;
        sizeSlider.value = px;
        document.getElementById('size-px-val').textContent = px;
        document.querySelectorAll('.size-btn-mix').forEach(x => x.classList.remove('active'));
        b.classList.add('active');
        updatePhonePreview();
    });
});
function syncSizeButtons() {
    document.querySelectorAll('.size-btn-mix').forEach(x => x.classList.remove('active'));
    const map = [38, 46, 56, 72];
    const labels = ['S', 'M', 'L', 'XL'];
    let closestIdx = 0, minDiff = 9999;
    map.forEach((v, i) => {
        const diff = Math.abs(v - state.fontSizePx);
        if (diff < minDiff) { minDiff = diff; closestIdx = i; }
    });
    if (minDiff <= 5) {
        const btn = document.querySelector(`.size-btn-mix[data-size="${map[closestIdx]}"]`);
        if (btn) btn.classList.add('active');
    }
}
document.getElementById('size-minus').addEventListener('click', () => {
    state.fontSizePx = Math.max(24, state.fontSizePx - 1);
    sizeSlider.value = state.fontSizePx;
    document.getElementById('size-px-val').textContent = state.fontSizePx;
    updatePhonePreview();
    syncSizeButtons();
});
document.getElementById('size-plus').addEventListener('click', () => {
    state.fontSizePx = Math.min(120, state.fontSizePx + 1);
    sizeSlider.value = state.fontSizePx;
    document.getElementById('size-px-val').textContent = state.fontSizePx;
    updatePhonePreview();
    syncSizeButtons();
});

// Duration toggle + slider
const durationToggle = document.getElementById('duration-toggle');
const durationSlider = document.getElementById('duration-slider');
durationToggle.addEventListener('change', (e) => {
    state.durationEnabled = e.target.checked;
    document.getElementById('duration-controls').classList.toggle('hidden', !state.durationEnabled);
    document.getElementById('duration-off-hint').classList.toggle('hidden', state.durationEnabled);
});
durationSlider.addEventListener('input', (e) => {
    state.durationSec = parseInt(e.target.value);
    document.getElementById('duration-val').textContent = state.durationSec;
});

// Live phone preview
function updatePhonePreview() {
    const cap = document.getElementById('phone-caption');
    if (!cap) return;
    // Position: convert % to top px in the 142px frame
    const frameH = 142;
    const captionH = 12; // approximate caption block height
    const top = (frameH - captionH) * (state.positionPct / 100);
    cap.style.top = top + 'px';

    // Size: scale the lines based on font size (24..120 px)
    const lines = cap.querySelectorAll('.phone-caption-line');
    const scale = Math.max(0.5, Math.min(2.5, state.fontSizePx / 56));
    lines.forEach(l => {
        l.style.height = (4 * scale) + 'px';
    });

    // Safe zone check (between 12% and 85%)
    const safeTag = document.getElementById('safe-tag');
    if (safeTag) {
        if (state.positionPct >= 12 && state.positionPct <= 85) {
            safeTag.textContent = '✓ Safe';
            safeTag.classList.remove('unsafe');
        } else {
            safeTag.textContent = '⚠ Hors zone';
            safeTag.classList.add('unsafe');
        }
    }
}
// ============ BIG PREVIEW MODAL ============
const previewModal = document.getElementById('preview-modal');
const bigPosSlider = document.getElementById('big-pos-slider');
const bigSizeSlider = document.getElementById('big-size-slider');
const bigCaption = document.getElementById('big-caption');
const bigPhoneScreen = document.getElementById('big-phone-screen');

document.getElementById('btn-zoom-preview').addEventListener('click', openPreviewModal);
document.getElementById('btn-preview-ok').addEventListener('click', closePreviewModal);
previewModal.addEventListener('click', (e) => {
    if (e.target === previewModal) closePreviewModal();
});

function openPreviewModal() {
    // Sync from main state
    bigPosSlider.value = state.positionPct;
    bigSizeSlider.value = state.fontSizePx;
    document.getElementById('big-size-px-val').textContent = state.fontSizePx;
    updateBigPreview();
    syncBigButtons();
    previewModal.classList.remove('hidden');
}

function closePreviewModal() {
    // Sync back to main controls
    posSlider.value = state.positionPct;
    sizeSlider.value = state.fontSizePx;
    document.getElementById('pos-pct-val').textContent = state.positionPct;
    document.getElementById('size-px-val').textContent = state.fontSizePx;
    updatePhonePreview();
    syncPositionButtons();
    syncSizeButtons();
    previewModal.classList.add('hidden');
}

bigPosSlider.addEventListener('input', (e) => {
    state.positionPct = parseInt(e.target.value);
    updateBigPreview();
    syncBigButtons();
});

bigSizeSlider.addEventListener('input', (e) => {
    state.fontSizePx = parseInt(e.target.value);
    document.getElementById('big-size-px-val').textContent = state.fontSizePx;
    updateBigPreview();
    syncBigButtons();
});

// Big modal: align preset buttons
previewModal.querySelectorAll('.align-btn-mix').forEach(b => {
    b.addEventListener('click', () => {
        const pos = parseInt(b.dataset.pos);
        state.positionPct = pos;
        bigPosSlider.value = pos;
        updateBigPreview();
        syncBigButtons();
    });
});

// Big modal: size preset buttons
previewModal.querySelectorAll('.size-btn-mix').forEach(b => {
    b.addEventListener('click', () => {
        const px = parseInt(b.dataset.size);
        state.fontSizePx = px;
        bigSizeSlider.value = px;
        document.getElementById('big-size-px-val').textContent = px;
        updateBigPreview();
        syncBigButtons();
    });
});

// Big modal: −/+ buttons (step of 1)
document.getElementById('big-size-minus').addEventListener('click', () => {
    state.fontSizePx = Math.max(24, state.fontSizePx - 1);
    bigSizeSlider.value = state.fontSizePx;
    document.getElementById('big-size-px-val').textContent = state.fontSizePx;
    updateBigPreview();
    syncBigButtons();
});
document.getElementById('big-size-plus').addEventListener('click', () => {
    state.fontSizePx = Math.min(120, state.fontSizePx + 1);
    bigSizeSlider.value = state.fontSizePx;
    document.getElementById('big-size-px-val').textContent = state.fontSizePx;
    updateBigPreview();
    syncBigButtons();
});

// Click/drag on the phone screen to position the caption
let isDraggingCaption = false;
function setCaptionFromPointer(clientY) {
    const rect = bigPhoneScreen.getBoundingClientRect();
    const y = clientY - rect.top;
    const pct = Math.max(0, Math.min(100, (y / rect.height) * 100));
    state.positionPct = Math.round(pct);
    bigPosSlider.value = state.positionPct;
    updateBigPreview();
    syncBigButtons();
}
bigPhoneScreen.addEventListener('mousedown', (e) => {
    isDraggingCaption = true;
    setCaptionFromPointer(e.clientY);
});
window.addEventListener('mousemove', (e) => {
    if (isDraggingCaption) setCaptionFromPointer(e.clientY);
});
window.addEventListener('mouseup', () => { isDraggingCaption = false; });
// Touch support
bigPhoneScreen.addEventListener('touchstart', (e) => {
    if (e.touches.length) setCaptionFromPointer(e.touches[0].clientY);
}, { passive: true });
bigPhoneScreen.addEventListener('touchmove', (e) => {
    if (e.touches.length) setCaptionFromPointer(e.touches[0].clientY);
}, { passive: true });

function updateBigPreview() {
    if (!bigCaption) return;
    // Position: top = pct of screen height
    bigCaption.style.top = state.positionPct + '%';

    // Size: scale lines based on font size (24..120px)
    const lines = bigCaption.querySelectorAll('.big-caption-line');
    const scale = Math.max(0.5, Math.min(2.5, state.fontSizePx / 56));
    lines.forEach(l => {
        l.style.height = (8 * scale) + 'px';
    });

    // Update position % in main controls live
    document.getElementById('pos-pct-val').textContent = state.positionPct;

    // Safe zone tag
    const tag = document.getElementById('big-safe-tag');
    if (tag) {
        if (state.positionPct >= 12 && state.positionPct <= 85) {
            tag.textContent = '✓ Safe';
            tag.classList.remove('unsafe');
        } else {
            tag.textContent = '⚠ Hors zone';
            tag.classList.add('unsafe');
        }
    }
}

function syncBigButtons() {
    // Position presets
    previewModal.querySelectorAll('.align-btn-mix').forEach(x => x.classList.remove('active'));
    let closestPos = null, minDiff = 9999;
    for (const btn of previewModal.querySelectorAll('.align-btn-mix')) {
        const v = parseInt(btn.dataset.pos);
        const d = Math.abs(v - state.positionPct);
        if (d <= 5 && d < minDiff) { closestPos = btn; minDiff = d; }
    }
    if (closestPos) closestPos.classList.add('active');

    // Size presets
    previewModal.querySelectorAll('.size-btn-mix').forEach(x => x.classList.remove('active'));
    const map = [38, 46, 56, 72];
    let closestIdx = 0; minDiff = 9999;
    map.forEach((v, i) => {
        const d = Math.abs(v - state.fontSizePx);
        if (d < minDiff) { minDiff = d; closestIdx = i; }
    });
    if (minDiff <= 5) {
        const btn = previewModal.querySelector(`.size-btn-mix[data-size="${map[closestIdx]}"]`);
        if (btn) btn.classList.add('active');
    }
}

async function refreshMixCounts() {
    const r = await fetch(API + '/mixer/preview');
    const data = await r.json();

    document.getElementById('mix-tpl').textContent = data.templates;
    document.getElementById('mix-vid').textContent = data.videos;
    const max = data.max_possible;
    const result = Math.min(state.maxVariants, max);
    document.getElementById('mix-result').textContent = result;
    document.getElementById('max-possible').textContent = `Max possible: ${max} (${data.templates} templates × ${data.videos} vidéos)`;
    maxVariantsSlider.max = Math.max(1, max);

    document.getElementById('bb-tpl').textContent = data.templates;
    document.getElementById('bb-raw').textContent = data.videos;
    document.getElementById('bb-mix').textContent = result;

    const launch = document.getElementById('bb-launch');
    if (data.templates > 0 && data.videos > 0) {
        launch.classList.add('ready');
        launch.classList.remove('mixing');
        launch.textContent = 'GO →';
    } else {
        launch.classList.remove('ready', 'mixing');
        launch.textContent = '⏳ Prêt';
    }

    const warns = [];
    if (data.templates === 0) warns.push(`<div class="warn">⚠ Aucun template — <a onclick="goToStep(1)">extraire d'abord</a></div>`);
    if (data.videos === 0) warns.push(`<div class="warn">⚠ Aucune vidéo — <a onclick="goToStep(3)">ajouter du contenu</a></div>`);
    document.getElementById('mix-warnings').innerHTML = warns.join('');

    document.getElementById('btn-run-mix').disabled = (data.templates === 0 || data.videos === 0);

    // Audio warning (when "music" priority is selected but no music)
    refreshAudioWarning(data.music);

    // Selected templates list
    refreshSelectedTemplates();
}

function refreshAudioWarning(musicCount) {
    const warn = document.getElementById('audio-warning');
    const musicBtn = document.querySelector('.audio-btn[data-audio="music"]');
    if (!warn || !musicBtn) return;

    if (state.audioPriority === 'music' && musicCount === 0) {
        warn.classList.remove('hidden');
        warn.innerHTML = `⚠ Aucune musique — <a onclick="goToStep(4)">onglet Musique</a>`;
        musicBtn.classList.add('warn-active');
        musicBtn.classList.remove('active');
    } else {
        warn.classList.add('hidden');
        musicBtn.classList.remove('warn-active');
    }
}

async function refreshSelectedTemplates() {
    const card = document.getElementById('selected-templates-card');
    const list = document.getElementById('selected-templates-list');
    if (!card || !list) return;

    try {
        const r = await fetch(API + '/mixer/preview-selection?max_variants=' + state.maxVariants);
        const data = await r.json();
        const tpls = data.templates || [];

        if (tpls.length === 0) {
            card.style.display = 'none';
            return;
        }
        card.style.display = '';
        list.innerHTML = tpls.map(t => {
            const captionShort = (t.caption || '').substring(0, 120);
            const captionTrunc = captionShort + ((t.caption || '').length > 120 ? '…' : '');
            const thumb = t.image_url
                ? `<img class="selected-tpl-thumb" src="${t.image_url}" alt="">`
                : `<div class="selected-tpl-thumb">📋</div>`;
            const filename = t.original_name || (t.image ? t.image : 'manuel');
            return `
                <div class="selected-tpl-item" data-tpl-id="${t.id}" onclick="openEditModal('${t.id}')">
                    ${thumb}
                    <div class="selected-tpl-content">
                        <div class="selected-tpl-caption">${escapeHtml(captionTrunc).replace(/\n/g, '<br>')}</div>
                        <div class="selected-tpl-meta">📷 ${escapeHtml(filename)}</div>
                    </div>
                </div>`;
        }).join('');
    } catch (err) {
        console.error('refreshSelectedTemplates failed:', err);
    }
}

document.getElementById('btn-run-mix').addEventListener('click', startMix);
document.getElementById('bb-launch').addEventListener('click', () => {
    if (document.getElementById('bb-launch').classList.contains('ready')) startMix();
});

function startMix() {
    if (state.eventSource) {
        toast('Mix déjà en cours', true);
        return;
    }
    openModal();
    resetModalUI();
    state.mixStartTime = Date.now();

    const launch = document.getElementById('bb-launch');
    launch.classList.remove('ready');
    launch.classList.add('mixing');
    launch.textContent = '…';

    const spoofPayload = getSpoofPayload();
    const params = new URLSearchParams({
        max_variants: state.maxVariants,
        size_label: state.sizeLabel,
        audio_priority: state.audioPriority,
        position_pct: state.positionPct,
        font_size_px: state.fontSizePx,
        caption_style: state.captionStyle || 'outlined',
        device_choice: state.selectedDevice || 'iphone_random',
        va_name: state.selectedVA || '',
        team: state.selectedTeam || '',
        enabled_filters: JSON.stringify(spoofPayload.enabled_filters),
        custom_ranges: JSON.stringify(spoofPayload.custom_ranges),
    });
    if (state.selectedModelId) {
        params.set('model_id', state.selectedModelId);
    }
    if (state.durationEnabled) {
        params.append('max_duration', state.durationSec);
    }
    const es = new EventSource(API + '/mixer/run-stream?' + params.toString());
    state.eventSource = es;

    const allOutputs = [];

    es.onmessage = (e) => {
        try {
            const ev = JSON.parse(e.data);
            handleMixEvent(ev, allOutputs);
            if (ev.type === 'done') {
                closeStream();
                renderOutputs(ev.outputs);
                launch.classList.remove('mixing');
                launch.classList.add('ready');
                launch.textContent = 'GO →';
                setStepDone(5);
                toast(`✅ ${ev.outputs.length} vidéo(s) générée(s)`);
            }
        } catch (err) {
            console.error('Parse error:', err, e.data);
        }
    };

    es.onerror = () => {
        consoleLog('ERROR', 'Connexion perdue');
        closeStream();
        launch.classList.remove('mixing');
        launch.classList.add('ready');
        launch.textContent = 'GO →';
    };
}

function closeStream() {
    if (state.eventSource) {
        state.eventSource.close();
        state.eventSource = null;
    }
}

function handleMixEvent(ev, allOutputs) {
    switch (ev.type) {
        case 'init':
            document.getElementById('modal-total').textContent = ev.total;
            document.getElementById('modal-current').textContent = '0';
            document.getElementById('console-engine').textContent = (ev.engine || 'ffmpeg').replace(/^ffmpeg version /, 'ffmpeg ').substring(0, 30);
            break;
        case 'log':
            consoleLog(ev.level, ev.message);
            break;
        case 'item_start':
            document.getElementById('modal-current').textContent = ev.index;
            document.getElementById('modal-current-file').textContent = `🎬 (${ev.index}/${ev.total}) ${ev.filename}`;
            updateBar(ev.index - 1, ev.total, 0);
            break;
        case 'item_progress': {
            const total = parseInt(document.getElementById('modal-total').textContent || '1');
            updateBar(ev.index - 1, total, ev.percent);
            document.getElementById('modal-elapsed').textContent = `${ev.elapsed}s`;
            updateETA(ev.index - 1, total, ev.percent);
            break;
        }
        case 'item_done': {
            const total = parseInt(document.getElementById('modal-total').textContent || '1');
            updateBar(ev.index, total, 0);
            allOutputs.push(ev.output);
            break;
        }
        case 'item_error':
            consoleLog('ERROR', `Item ${ev.index} fail`);
            break;
        case 'done':
            document.getElementById('modal-pct').textContent = '100';
            document.getElementById('modal-bar').style.width = '100%';
            document.getElementById('modal-eta').textContent = '00:00';
            document.querySelector('.badge-mixing').textContent = '● DONE';
            document.querySelector('.badge-mixing').style.color = 'var(--green)';
            document.querySelector('.badge-mixing').style.borderColor = 'var(--green)';
            break;
    }
}

function updateBar(itemsDone, total, pctOfCurrent) {
    const overall = ((itemsDone + (pctOfCurrent / 100)) / total) * 100;
    const pct = Math.min(100, Math.max(0, overall));
    document.getElementById('modal-pct').textContent = Math.round(pct);
    document.getElementById('modal-bar').style.width = pct + '%';
}

function updateETA(itemsDone, total, pctOfCurrent) {
    const elapsedSec = (Date.now() - state.mixStartTime) / 1000;
    const overallFraction = (itemsDone + (pctOfCurrent / 100)) / total;
    if (overallFraction <= 0.01) return;
    const totalEst = elapsedSec / overallFraction;
    const remaining = Math.max(0, totalEst - elapsedSec);
    const m = Math.floor(remaining / 60);
    const s = Math.floor(remaining % 60);
    document.getElementById('modal-eta').textContent =
        `${String(m).padStart(2, '0')}:${String(s).padStart(2, '0')}`;
}

function consoleLog(level, message) {
    const body = document.getElementById('console-body');
    const t = new Date();
    const time = `${pad(t.getHours())}:${pad(t.getMinutes())}:${pad(t.getSeconds())}.${String(t.getMilliseconds()).padStart(3, '0')}`;
    const div = document.createElement('div');
    div.className = 'log-line';
    div.innerHTML = `
        <span class="log-time">[${time}]</span>
        <span class="log-tag log-tag-${escapeHtml(level)}">${escapeHtml(level)}</span>
        <span class="log-msg">${escapeHtml(message)}</span>
    `;
    body.appendChild(div);
    body.scrollTop = body.scrollHeight;
    while (body.children.length > 200) body.removeChild(body.firstChild);
}
function pad(n) { return String(n).padStart(2, '0'); }

function resetModalUI() {
    document.getElementById('modal-pct').textContent = '0';
    document.getElementById('modal-bar').style.width = '0%';
    document.getElementById('modal-current').textContent = '0';
    document.getElementById('modal-total').textContent = '0';
    document.getElementById('modal-eta').textContent = '--:--';
    document.getElementById('modal-elapsed').textContent = '0.0s';
    document.getElementById('modal-current-file').textContent = '—';
    document.getElementById('console-body').innerHTML = '';
    const badge = document.querySelector('.badge-mixing');
    badge.textContent = '● MIXING';
    badge.style.color = '';
    badge.style.borderColor = '';
}

function renderOutputs(outputs) {
    const container = document.getElementById('mix-results');
    if (!outputs.length) {
        container.innerHTML = '<div class="empty-state"><div>🎬</div><div>Aucune vidéo générée</div></div>';
        return;
    }
    container.innerHTML = outputs.map(o => `
        <div class="output-card">
            <video src="${o.url}" controls preload="metadata"></video>
            <div class="item-title">${escapeHtml(o.filename)}</div>
            <div class="item-actions">
                <a class="btn btn-cyan" href="${o.url}" download>⬇ Télécharger</a>
            </div>
        </div>`).join('');
}

function openModal() { document.getElementById('mix-modal').classList.remove('hidden'); }
function closeModal() { document.getElementById('mix-modal').classList.add('hidden'); }
document.getElementById('btn-close-modal').addEventListener('click', () => {
    if (state.eventSource && !confirm('Mix en cours — fermer la fenêtre ?')) return;
    closeStream();
    closeModal();
    refreshMixCounts();
});
document.getElementById('btn-minimize').addEventListener('click', closeModal);

// ============ UTILS ============
function escapeHtml(s) {
    if (s === undefined || s === null) return '';
    return String(s).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
}
function setStepDone(n) {
    const num = document.getElementById(`num-${n}`);
    if (num && !num.classList.contains('done')) {
        num.classList.add('done');
        num.textContent = '✓';
    }
}
async function refreshAll() {
    await Promise.all([refreshTemplates(), refreshVideos(), refreshMusic(), refreshMixCounts()]);
}

// ============ SPOOF PARAMS GRID (étape 5 Mix) ============
function renderSpoofGrid() {
    const grid = document.getElementById('spoof-grid');
    if (!grid) return;
    grid.innerHTML = '';
    Object.entries(state.spoof).forEach(([key, p]) => {
        const item = document.createElement('div');
        item.className = 'spoof-item' + (p.enabled ? '' : ' disabled');
        const stepStr = p.step;
        item.innerHTML = `
            <div class="spoof-item-header">
                <input type="checkbox" id="sp-en-${key}" ${p.enabled ? 'checked' : ''}>
                <label for="sp-en-${key}">${escapeHtml(p.label)}</label>
            </div>
            <div class="spoof-inputs">
                <span>Min</span>
                <input type="number" id="sp-min-${key}" value="${p.min}" step="${stepStr}">
                <span>Max</span>
                <input type="number" id="sp-max-${key}" value="${p.max}" step="${stepStr}">
            </div>
        `;
        grid.appendChild(item);

        const cb = item.querySelector(`#sp-en-${key}`);
        cb.addEventListener('change', () => {
            p.enabled = cb.checked;
            item.classList.toggle('disabled', !cb.checked);
        });
        const minInp = item.querySelector(`#sp-min-${key}`);
        const maxInp = item.querySelector(`#sp-max-${key}`);
        minInp.addEventListener('input', () => {
            const val = parseFloat(minInp.value);
            if (!isNaN(val)) p.min = p.isInt ? Math.round(val) : val;
        });
        maxInp.addEventListener('input', () => {
            const val = parseFloat(maxInp.value);
            if (!isNaN(val)) p.max = p.isInt ? Math.round(val) : val;
        });
    });
}

document.getElementById('spoof-reset')?.addEventListener('click', () => {
    Object.values(state.spoof).forEach(p => {
        p.enabled = true;
        p.min = p.default_min;
        p.max = p.default_max;
    });
    renderSpoofGrid();
    toast('↻ Paramètres remis aux valeurs par défaut');
});
document.getElementById('spoof-disable-all')?.addEventListener('click', () => {
    Object.values(state.spoof).forEach(p => { p.enabled = false; });
    renderSpoofGrid();
});
document.getElementById('spoof-enable-all')?.addEventListener('click', () => {
    Object.values(state.spoof).forEach(p => { p.enabled = true; });
    renderSpoofGrid();
});

function getSpoofPayload() {
    // Construit { enabled_filters: [...], custom_ranges: {...} } pour l'API
    const enabled = [];
    const ranges = {};
    Object.entries(state.spoof).forEach(([key, p]) => {
        if (!p.enabled) return;
        enabled.push(key);
        // Sécurité : min <= max
        const lo = Math.min(p.min, p.max);
        const hi = Math.max(p.min, p.max);
        ranges[key] = [lo, hi];
    });
    return { enabled_filters: enabled, custom_ranges: ranges };
}

// ============ VA ADMIN (gestion emails Drive) ============
async function loadVAAdminList() {
    const list = document.getElementById('va-admin-list');
    try {
        const res = await fetch(API + '/list-vas-admin');
        const data = await res.json();
        const vas = data.vas || [];
        // Peuple state.vaEmails (utilisé pour badge "pas d'email" dans le sélecteur Destination)
        state.vaEmails = {};
        vas.forEach(v => {
            if (v.discord_id) state.vaEmails[v.discord_id] = (v.email || '').trim();
        });
        // Re-render le sélecteur VA si déjà chargé (pour mettre à jour les badges)
        if (Object.keys(state.vasByTeam).some(t => state.vasByTeam[t].length > 0)) {
            renderVASelect();
        }
        if (list) renderVAAdminList(vas);
    } catch (e) {
        if (list) list.innerHTML = '<div class="muted small">Erreur de chargement</div>';
    }
}

function renderVAAdminList(vas) {
    const list = document.getElementById('va-admin-list');
    list.innerHTML = '';
    if (!vas || vas.length === 0) {
        list.innerHTML = '<div class="muted small">Aucun VA dans le cache. Click 🔄 Resync pour récupérer depuis Discord.</div>';
        return;
    }
    vas.forEach(v => {
        const row = document.createElement('div');
        row.className = 'va-admin-row';
        row.dataset.team = v.team || '';
        row.innerHTML = `
            <div class="va-admin-name">
                ${escapeHtml(v.name)}
                <span class="va-admin-team-badge">${escapeHtml(v.team || '—')}</span>
            </div>
            <input type="email" class="va-admin-email" placeholder="email@gmail.com"
                   value="${escapeHtml(v.email || '')}"
                   data-discord-id="${escapeHtml(v.discord_id)}">
            <button class="va-admin-save">💾 Save</button>
            <span class="va-admin-status"></span>
        `;
        list.appendChild(row);

        const saveBtn = row.querySelector('.va-admin-save');
        const input = row.querySelector('.va-admin-email');
        const status = row.querySelector('.va-admin-status');
        saveBtn.addEventListener('click', async () => {
            saveBtn.classList.add('saving');
            status.className = 'va-admin-status';
            status.textContent = '…';
            try {
                const fd = new FormData();
                fd.append('discord_id', input.dataset.discordId);
                fd.append('email', input.value.trim());
                const r = await fetch(API + '/save-va-email', { method: 'POST', body: fd });
                const data = await r.json();
                if (data.ok) {
                    status.className = 'va-admin-status ok';
                    status.textContent = '✓ saved';
                    // Met à jour le state local et le sélecteur de la card Destination
                    const newEmail = input.value.trim();
                    state.vaEmails[input.dataset.discordId] = newEmail;
                    renderVASelect();
                    setTimeout(() => { status.textContent = ''; }, 2000);
                } else {
                    status.className = 'va-admin-status err';
                    status.textContent = '✗ ' + (data.error || 'error').slice(0, 30);
                }
            } catch (e) {
                status.className = 'va-admin-status err';
                status.textContent = '✗ network';
            } finally {
                saveBtn.classList.remove('saving');
            }
        });
    });
    // Réapplique le filtre actif
    const activeFilter = document.querySelector('.va-filter-btn.active');
    if (activeFilter) applyVAFilter(activeFilter.dataset.team);
}

function applyVAFilter(team) {
    document.querySelectorAll('.va-admin-row').forEach(row => {
        if (team === 'all') {
            row.style.display = 'flex';
        } else {
            row.style.display = row.dataset.team === team ? 'flex' : 'none';
        }
    });
}

document.querySelectorAll('.va-filter-btn').forEach(btn => {
    btn.addEventListener('click', () => {
        document.querySelectorAll('.va-filter-btn').forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
        applyVAFilter(btn.dataset.team);
    });
});

document.getElementById('va-admin-resync')?.addEventListener('click', async (e) => {
    const btn = e.currentTarget;
    btn.disabled = true;
    btn.textContent = '🔄 Sync...';
    try {
        const r = await fetch(API + '/resync-vas', { method: 'POST' });
        const data = await r.json();
        if (data.ok) {
            toast(`✓ Resync : ${data.total || 0} VA(s)`);
            await loadVAAdminList();
            await loadVAs();  // refresh aussi le selecteur dans la card destination
        } else {
            toast('Resync échouée: ' + (data.error || ''), true);
        }
    } catch (e) {
        toast('Resync error: ' + e.message, true);
    } finally {
        btn.disabled = false;
        btn.textContent = '🔄 Resync';
    }
});

// ============ DESTINATION (Équipe + VA + Device) ============
async function loadVAs() {
    try {
        const res = await fetch(API + '/list-vas');
        if (!res.ok) return;
        const data = await res.json();
        state.vasByTeam = data.teams || { geelark: [], instagram: [] };
        renderVASelect();
    } catch (e) {
        console.warn('loadVAs failed', e);
    }
}

function renderVASelect() {
    const sel = document.getElementById('cf-va');
    const team = state.selectedTeam;
    const list = state.vasByTeam[team] || [];
    const countSpan = document.getElementById('cf-va-count');
    if (countSpan) countSpan.textContent = list.length ? `(${list.length})` : '';

    sel.innerHTML = '<option value="">— Aucun VA (juste mix) —</option>';
    list.forEach(v => {
        const opt = document.createElement('option');
        opt.value = v.name;
        // Affiche un badge ⚠️ si pas d'email enregistré pour ce VA
        const hasEmail = state.vaEmails[v.discord_id] && state.vaEmails[v.discord_id].length > 0;
        opt.textContent = hasEmail ? v.name : `${v.name} ⚠️ (pas d'email)`;
        if (!hasEmail) {
            opt.dataset.noEmail = 'true';
        }
        sel.appendChild(opt);
    });
    // Restaure la sélection si toujours valide
    if (state.selectedVA && list.some(v => v.name === state.selectedVA)) {
        sel.value = state.selectedVA;
    } else {
        state.selectedVA = '';
    }
}

document.getElementById('cf-team').addEventListener('change', (e) => {
    state.selectedTeam = e.target.value;
    renderVASelect();
});
document.getElementById('cf-va').addEventListener('change', (e) => {
    state.selectedVA = e.target.value;
});
document.getElementById('cf-device').addEventListener('change', (e) => {
    state.selectedDevice = e.target.value;
});
document.getElementById('cf-resync-vas').addEventListener('click', async (e) => {
    const btn = e.currentTarget;
    btn.classList.add('spinning');
    try {
        const res = await fetch(API + '/resync-vas', { method: 'POST' });
        const data = await res.json();
        if (data.ok) {
            toast(`✓ ${data.total || 0} VA(s) resynchro`);
            await loadVAs();
        } else {
            toast('Resync failed: ' + (data.error || ''), true);
        }
    } catch (e) {
        toast('Resync error: ' + e.message, true);
    } finally {
        btn.classList.remove('spinning');
    }
});

// Build manual emoji row on load
buildEmojiQuickRow('manual-emoji-quick', 'caption-text');

// Init mix previews
updatePhonePreview();
syncPositionButtons();
syncSizeButtons();

// ============ MODÈLES (créatrices) ============
async function loadModels() {
    try {
        const r = await fetch(API + '/models/');
        const data = await r.json();
        state.models = data.models || [];
        renderModelSelect();
        renderModelsList();
        renderContentModelSelect();
        renderCategoryFilterPills();
    } catch (e) {
        console.warn('loadModels failed', e);
    }
}

// ============ ÉTAPE 3 : SÉLECTEUR CATÉGORIE (upload obligatoire) ============
function renderContentModelSelect() {
    const sel = document.getElementById('content-model-select');
    if (!sel) return;
    const current = state.contentModelId;
    sel.innerHTML = '<option value="">⚠️ Choisis une catégorie avant d\'uploader des vidéos</option>';
    state.models.forEach(m => {
        const opt = document.createElement('option');
        opt.value = String(m.id);
        opt.textContent = `ID ${m.id} — ${m.label}`;
        sel.appendChild(opt);
    });
    if (current && state.models.some(m => String(m.id) === String(current))) {
        sel.value = String(current);
    } else {
        state.contentModelId = '';
    }
    updateUploadGuard();
}

function updateUploadGuard() {
    // Bloque visuellement les zones d'upload si pas de catégorie sélectionnée
    const card = document.querySelector('.category-selector-card');
    const dropZone = document.getElementById('video-drop-zone');
    const folderRow = document.querySelector('.folder-row');
    const filterSection = document.querySelector('.filter-section');
    const folderActions = document.querySelector('.folder-actions');

    const hasCategory = !!state.contentModelId;

    if (card) card.classList.toggle('invalid', !hasCategory);
    if (dropZone) dropZone.classList.toggle('upload-disabled', !hasCategory);
    [folderRow, filterSection, folderActions].forEach(el => {
        if (el) el.classList.toggle('upload-disabled', !hasCategory);
    });

    // Hint dynamique
    const hint = document.getElementById('category-hint');
    if (hint) {
        if (state.models.length === 0) {
            hint.innerHTML = '⚠️ <strong>Aucun modèle créé.</strong> Va à l\'étape 5 → "👤 Gestion des modèles" en bas pour en créer un.';
            hint.style.color = 'var(--red)';
        } else if (!hasCategory) {
            hint.textContent = '⚠️ Sélectionne une catégorie ci-dessus pour pouvoir uploader des vidéos.';
            hint.style.color = '';
        } else {
            const m = state.models.find(x => String(x.id) === String(state.contentModelId));
            hint.textContent = m
                ? `Les vidéos uploadées seront associées à : ID ${m.id} — ${m.label}`
                : '';
            hint.style.color = '';
        }
    }
}

document.getElementById('content-model-select')?.addEventListener('change', (e) => {
    state.contentModelId = e.target.value;
    updateUploadGuard();
    // Si on sélectionne une catégorie, on filtre aussi automatiquement la liste affichée sur celle-ci
    state.contentFilterModelId = e.target.value;
    renderCategoryFilterPills();
    refreshVideos();
});

document.getElementById('btn-refresh-models')?.addEventListener('click', () => {
    loadModels();
    toast('🔄 Modèles rechargés');
});

// ============ ÉTAPE 3 : FILTRE PAR CATÉGORIE (liste vidéos) ============
async function renderCategoryFilterPills() {
    const card = document.getElementById('category-filter-card');
    const host = document.getElementById('category-filter-pills');
    if (!host || !card) return;

    if (!state.models || state.models.length === 0) {
        card.style.display = 'none';
        return;
    }
    card.style.display = '';

    // Compteurs : récupère le nb de vidéos par catégorie via l'API
    let countsByModel = {};
    let totalAll = 0;
    try {
        const all = await fetch(API + '/content/').then(r => r.json());
        if (Array.isArray(all)) {
            totalAll = all.length;
            all.forEach(v => {
                const k = v.model_id != null ? String(v.model_id) : '_none';
                countsByModel[k] = (countsByModel[k] || 0) + 1;
            });
        }
    } catch {}

    const pills = [];
    pills.push(`
        <button class="category-filter-pill ${!state.contentFilterModelId ? 'active' : ''}" data-filter="">
            Toutes <span class="pill-count">${totalAll}</span>
        </button>
    `);
    state.models.forEach(m => {
        const count = countsByModel[String(m.id)] || 0;
        const isActive = String(state.contentFilterModelId) === String(m.id);
        pills.push(`
            <button class="category-filter-pill ${isActive ? 'active' : ''}" data-filter="${m.id}">
                ID ${m.id} — ${escapeHtml(m.label)} <span class="pill-count">${count}</span>
            </button>
        `);
    });
    host.innerHTML = pills.join('');

    host.querySelectorAll('.category-filter-pill').forEach(btn => {
        btn.addEventListener('click', () => {
            state.contentFilterModelId = btn.dataset.filter;
            renderCategoryFilterPills();
            refreshVideos();
        });
    });
}

function renderModelSelect() {
    const sel = document.getElementById('cf-model');
    if (!sel) return;
    const current = state.selectedModelId;
    sel.innerHTML = '<option value="">— Aucun —</option>';
    state.models.forEach(m => {
        const opt = document.createElement('option');
        opt.value = String(m.id);
        opt.textContent = m.label;
        sel.appendChild(opt);
    });
    if (current && state.models.some(m => String(m.id) === String(current))) {
        sel.value = String(current);
    } else {
        state.selectedModelId = '';
    }
}

function renderModelsList() {
    const list = document.getElementById('models-list');
    if (!list) return;
    if (!state.models || state.models.length === 0) {
        list.innerHTML = '<div class="muted small">Aucun modèle. Créé un nouveau ci-dessus pour commencer.</div>';
        return;
    }
    list.innerHTML = '';
    state.models.forEach(m => {
        const row = document.createElement('div');
        row.className = 'va-admin-row';
        row.innerHTML = `
            <div class="va-admin-name">
                ${escapeHtml(m.label)}
            </div>
            <div style="flex:1;"></div>
            <button class="hist-delete-btn" data-action="edit" data-id="${m.id}" data-label="${escapeHtml(m.label)}" title="Modifier ce modèle" style="background:#3b82f6;">✎ Modifier</button>
            <button class="hist-delete-btn" data-action="delete" data-id="${m.id}" title="Supprimer ce modèle">✕ Supprimer</button>
        `;
        list.appendChild(row);

        // Bouton MODIFIER
        row.querySelector('[data-action="edit"]').addEventListener('click', async () => {
            const newLabel = prompt(`Renommer le modèle "${m.label}" :`, m.label);
            if (newLabel === null) return; // annulé
            const clean = newLabel.trim();
            if (!clean) {
                toast('Le nom ne peut pas être vide', true);
                return;
            }
            if (clean === m.label) return; // pas changé
            try {
                const fd = new FormData();
                fd.append('label', clean);
                const r = await fetch(API + '/models/' + m.id, { method: 'PATCH', body: fd });
                if (r.ok) {
                    toast(`✓ Modèle renommé en "${clean}"`);
                    await loadModels();
                } else {
                    const err = await r.text();
                    toast('Modification échouée: ' + err, true);
                }
            } catch (e) {
                toast('Erreur: ' + e.message, true);
            }
        });

        // Bouton SUPPRIMER
        row.querySelector('[data-action="delete"]').addEventListener('click', async () => {
            if (!confirm(`Supprimer le modèle "${m.label}" ?\n(Les batches déjà créés gardent leur référence.)`)) return;
            try {
                const r = await fetch(API + '/models/' + m.id, { method: 'DELETE' });
                if (r.ok) {
                    toast(`🗑️ Modèle "${m.label}" supprimé`);
                    await loadModels();
                } else {
                    toast('Suppression échouée', true);
                }
            } catch (e) {
                toast('Erreur: ' + e.message, true);
            }
        });
    });
}

document.getElementById('cf-model')?.addEventListener('change', (e) => {
    state.selectedModelId = e.target.value;
});

document.getElementById('model-create')?.addEventListener('click', async () => {
    const labelInput = document.getElementById('model-new-label');
    const label = (labelInput.value || '').trim();
    try {
        const fd = new FormData();
        fd.append('label', label);
        const r = await fetch(API + '/models/', { method: 'POST', body: fd });
        const data = await r.json();
        if (data.ok && data.model) {
            labelInput.value = '';
            toast(`✓ Modèle "${data.model.label}" créé`);
            await loadModels();
        } else {
            toast('Création échouée: ' + (data.detail || 'erreur'), true);
        }
    } catch (e) {
        toast('Erreur: ' + e.message, true);
    }
});

// ============ HISTORIQUE (étape 6) ============
const histState = {
    period: 'all',
    startDate: null,
    endDate: null,
    team: '',
    vaName: '',
};

async function loadHistoryStats() {
    try {
        const r = await fetch(API + '/history/stats');
        const data = await r.json();
        document.getElementById('hist-stat-total').textContent = data.total || 0;
        document.getElementById('hist-stat-today').textContent = data.today || 0;
        document.getElementById('hist-stat-videos').textContent = data.videos_total || 0;
        document.getElementById('hist-stat-videos-today').textContent = data.videos_today || 0;
    } catch (e) {
        console.warn('history stats failed', e);
    }
}

async function loadHistory() {
    const list = document.getElementById('history-list');
    list.innerHTML = '<div class="history-empty"><div class="empty-icon">⏳</div><div>Chargement...</div></div>';

    const params = new URLSearchParams({
        period: histState.period,
        limit: 200,
    });
    if (histState.period === 'custom' && histState.startDate) {
        params.set('start_date', histState.startDate);
        if (histState.endDate) params.set('end_date', histState.endDate);
    }
    if (histState.team) params.set('team', histState.team);
    if (histState.vaName) params.set('va_name', histState.vaName);

    try {
        const r = await fetch(API + '/history/?' + params.toString());
        const data = await r.json();
        renderHistoryList(data.batches || []);
    } catch (e) {
        list.innerHTML = '<div class="history-empty"><div class="empty-icon">❌</div><div>Erreur de chargement</div></div>';
    }
}

function renderHistoryList(batches) {
    const list = document.getElementById('history-list');
    if (!batches || batches.length === 0) {
        list.innerHTML = '<div class="history-empty"><div class="empty-icon">📭</div><div>Aucun mix dans l\'historique pour cette période</div></div>';
        return;
    }
    // Header row
    const rows = ['<div class="history-row history-header">' +
        '<div>VA</div>' +
        '<div>Équipe</div>' +
        '<div>Date</div>' +
        '<div>Vidéos</div>' +
        '<div>Device</div>' +
        '<div>Drive</div>' +
        '<div></div>' +
        '</div>'];

    batches.forEach(b => {
        const date = b.created_at ? new Date(b.created_at) : null;
        const dateStr = date ? date.toLocaleString('fr-FR', {
            day: '2-digit', month: '2-digit', year: '2-digit',
            hour: '2-digit', minute: '2-digit'
        }) : '—';
        const team = (b.team || '').toLowerCase();
        const teamBadge = team
            ? `<span class="hist-team-badge ${team}">${escapeHtml(team)}</span>`
            : '<span class="muted small">—</span>';
        const vidNum = b.videos_uploaded || b.videos_count || 0;
        const totalNum = b.videos_count || 0;
        const vidDisplay = vidNum === totalNum
            ? `<span class="vid-num">${vidNum}</span>`
            : `<span class="vid-num">${vidNum}</span><span class="vid-label">/ ${totalNum}</span>`;
        const deviceLabel = b.device_choice || '—';
        const driveBtn = b.drive_folder_url
            ? `<a class="hist-drive-btn" href="${escapeHtml(b.drive_folder_url)}" target="_blank">📁 Voir Drive</a>`
            : `<span class="hist-drive-btn disabled">📁 Pas de Drive</span>`;

        const vaCell = b.va_name
            ? escapeHtml(b.va_name)
            : '<span class="muted small">— Aucun VA —</span>';

        rows.push(`
            <div class="history-row" data-id="${escapeHtml(b.id)}">
                <div class="hist-cell-va">${vaCell}</div>
                <div class="hist-cell-team">${teamBadge}</div>
                <div class="hist-cell-date">${escapeHtml(dateStr)}</div>
                <div class="hist-cell-videos">${vidDisplay}</div>
                <div class="hist-cell-device" title="${escapeHtml(deviceLabel)}">${escapeHtml(deviceLabel)}</div>
                <div>${driveBtn}</div>
                <div><button class="hist-delete-btn" data-id="${escapeHtml(b.id)}" title="Supprimer de l'historique">✕</button></div>
            </div>
        `);
    });
    list.innerHTML = rows.join('');

    // Handlers delete
    list.querySelectorAll('.hist-delete-btn').forEach(btn => {
        btn.addEventListener('click', async (e) => {
            e.stopPropagation();
            const id = btn.dataset.id;
            if (!confirm('Supprimer ce batch de l\'historique ? (le dossier Drive ne sera PAS effacé)')) return;
            try {
                const r = await fetch(API + '/history/' + id, { method: 'DELETE' });
                if (r.ok) {
                    btn.closest('.history-row').remove();
                    await loadHistoryStats();
                    toast('🗑️ Batch supprimé');
                } else {
                    toast('Suppression échouée', true);
                }
            } catch (err) {
                toast('Erreur: ' + err.message, true);
            }
        });
    });
}

// Period buttons handlers
document.querySelectorAll('.hist-period-btn').forEach(btn => {
    btn.addEventListener('click', () => {
        const period = btn.dataset.period;
        document.querySelectorAll('.hist-period-btn').forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
        const dateRange = document.getElementById('hist-date-range');
        if (period === 'custom') {
            dateRange.style.display = 'flex';
            // Ne charge pas tant qu'on a pas cliqué Appliquer
            return;
        }
        dateRange.style.display = 'none';
        histState.period = period;
        histState.startDate = null;
        histState.endDate = null;
        loadHistory();
    });
});

// Apply custom date
document.getElementById('hist-apply-custom')?.addEventListener('click', () => {
    const sd = document.getElementById('hist-start-date').value;
    const ed = document.getElementById('hist-end-date').value;
    if (!sd) {
        toast('Sélectionne une date de début', true);
        return;
    }
    histState.period = 'custom';
    histState.startDate = sd;
    histState.endDate = ed || sd;
    loadHistory();
});

// Filter team / VA
document.getElementById('hist-filter-team')?.addEventListener('change', (e) => {
    histState.team = e.target.value;
    populateHistVAFilter();
    loadHistory();
});
document.getElementById('hist-filter-va')?.addEventListener('change', (e) => {
    histState.vaName = e.target.value;
    loadHistory();
});

document.getElementById('hist-refresh')?.addEventListener('click', () => {
    loadHistoryStats();
    loadHistory();
});

function populateHistVAFilter() {
    const sel = document.getElementById('hist-filter-va');
    if (!sel) return;
    const team = histState.team;
    const list = team ? (state.vasByTeam[team] || [])
                      : [...(state.vasByTeam.geelark || []), ...(state.vasByTeam.instagram || [])];
    // Dédoublonne par nom
    const seen = new Set();
    const unique = list.filter(v => {
        if (seen.has(v.name)) return false;
        seen.add(v.name);
        return true;
    });
    sel.innerHTML = '<option value="">Tous VAs</option>';
    unique.forEach(v => {
        const opt = document.createElement('option');
        opt.value = v.name;
        opt.textContent = v.name;
        sel.appendChild(opt);
    });
    if (histState.vaName && unique.some(v => v.name === histState.vaName)) {
        sel.value = histState.vaName;
    } else {
        histState.vaName = '';
    }
}

// Charge l'historique quand on switch sur l'étape 6
const navSteps = document.querySelectorAll('.step[data-step]');
navSteps.forEach(s => {
    s.addEventListener('click', () => {
        if (s.dataset.step === '6') {
            populateHistVAFilter();
            loadHistoryStats();
            loadHistory();
        }
    });
});

// Initial load
refreshAll();
loadVAs();
loadVAAdminList();
loadModels();
renderSpoofGrid();
