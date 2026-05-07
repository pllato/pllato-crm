// ═══════════════════════════════════════════════════════════════
//   Pllato CRM Worker
//   Endpoints:
//     POST /api/whoami            — verify Firebase ID token, return user info
//     POST /api/test-bitrix       — test Bitrix24 webhook connection
//     POST /api/migrate-users     — migrate Bitrix24 users to Firebase Auth
//     POST /api/diagnose-pipeline — read-only анализ воронки
//     POST /api/migrate-pipeline  — миграция структуры воронки
//     POST /api/migrate-deals     — миграция сделок (батчи по 50)
//     POST /api/migrate-contacts   — миграция контактов (батчи по 50)
//     POST /api/migrate-companies  — миграция компаний (один батч, их мало)
//     POST /api/diagnose-tasks     — read-only диагностика модуля Задач
//     POST /api/diagnose-activities — read-only диагностика дел/активностей
//     POST /api/migrate-activities — миграция дел (CRM activity, батчи по 50)
//     POST /api/migrate-tasks      — миграция задач (батчи по 50)
//     POST /api/migrate-task-comments — комментарии задач (через bitrixBatch)
//     POST /api/migrate-sources       — справочники (источники, типы и т.п.)
//     POST /api/migrate-departments   — структура отделов компании
//     POST /api/diagnose-bizproc      — анализ бизнес-процессов (read-only)
//     POST /api/file/:id              — отдаёт файл из R2 по bitrixFileId
//
//   Required Worker secrets:
//     FIREBASE_SERVICE_ACCOUNT — JSON string of service account
//     BITRIX_WEBHOOK_URL       — Bitrix24 webhook URL
//     R2_ACCESS_KEY_ID         — (для будущих миграций файлов)
//     R2_SECRET_ACCESS_KEY     — (для будущих миграций файлов)
//     R2_ACCOUNT_ID            — (для будущих миграций файлов)
//
//   Required bindings:
//     BUCKET → R2 bucket pllato-crm-files
// ═══════════════════════════════════════════════════════════════

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type, Authorization',
  'Access-Control-Max-Age': '86400',
};

export default {
  async fetch(request, env, ctx) {
    if (request.method === 'OPTIONS') return new Response(null, { headers: CORS });

    const url = new URL(request.url);
    const path = url.pathname;

    try {
      if (path === '/' || path === '/health') {
        return json({ ok: true, service: 'pllato-crm-worker', ts: new Date().toISOString() });
      }
      if (path === '/api/whoami' && request.method === 'POST') {
        return await handleWhoami(request, env);
      }
      if (path === '/api/test-bitrix' && request.method === 'POST') {
        return await handleTestBitrix(request, env);
      }
      if (path === '/api/migrate-users' && request.method === 'POST') {
        const dryRun = url.searchParams.get('dryRun') === '1';
        return await handleMigrateUsers(request, env, dryRun);
      }
      if (path === '/api/diagnose-pipeline' && request.method === 'POST') {
        return await handleDiagnosePipeline(request, env);
      }
      if (path === '/api/migrate-pipeline' && request.method === 'POST') {
        return await handleMigratePipeline(request, env);
      }
      if (path === '/api/migrate-deals' && request.method === 'POST') {
        return await handleMigrateDeals(request, env);
      }
      if (path === '/api/migrate-contacts' && request.method === 'POST') {
        return await handleMigrateContacts(request, env);
      }
      if (path === '/api/migrate-companies' && request.method === 'POST') {
        return await handleMigrateCompanies(request, env);
      }
      if (path === '/api/diagnose-tasks' && request.method === 'POST') {
        return await handleDiagnoseTasks(request, env);
      }
      if (path === '/api/diagnose-activities' && request.method === 'POST') {
        return await handleDiagnoseActivities(request, env);
      }
      if (path === '/api/migrate-activities' && request.method === 'POST') {
        return await handleMigrateActivities(request, env);
      }
      if (path === '/api/migrate-tasks' && request.method === 'POST') {
        return await handleMigrateTasks(request, env);
      }
      if (path === '/api/migrate-task-comments' && request.method === 'POST') {
        return await handleMigrateTaskComments(request, env);
      }
      if (path === '/api/test-file-download' && request.method === 'POST') {
        return await handleTestFileDownload(request, env);
      }
      if (path === '/api/build-file-queue' && request.method === 'POST') {
        return await handleBuildFileQueue(request, env);
      }
      if (path === '/api/migrate-files' && request.method === 'POST') {
        return await handleMigrateFiles(request, env);
      }
      if (path === '/api/diagnose-chats' && request.method === 'POST') {
        return await handleDiagnoseChats(request, env);
      }
      if (path === '/api/test-chat-history' && request.method === 'POST') {
        return await handleTestChatHistory(request, env);
      }
      if (path === '/api/migrate-openlines' && request.method === 'POST') {
        return await handleMigrateOpenlines(request, env);
      }
      if (path === '/api/diagnose-group-chats' && request.method === 'POST') {
        return await handleDiagnoseGroupChats(request, env);
      }
      if (path === '/api/migrate-group-chats' && request.method === 'POST') {
        return await handleMigrateGroupChats(request, env);
      }
      if (path === '/api/migrate-sources' && request.method === 'POST') {
        return await handleMigrateSources(request, env);
      }
      if (path === '/api/migrate-departments' && request.method === 'POST') {
        return await handleMigrateDepartments(request, env);
      }
      if (path === '/api/diagnose-bizproc' && request.method === 'POST') {
        return await handleDiagnoseBizproc(request, env);
      }
      if (path === '/api/sync-delta' && request.method === 'POST') {
        return await handleSyncDelta(request, env);
      }
      if (path.startsWith('/api/file/') && request.method === 'POST') {
        const fileId = decodeURIComponent(path.slice('/api/file/'.length));
        return await handleGetFile(request, env, fileId, ctx);
      }
      return json({ ok: false, error: 'Not found' }, 404);
    } catch (e) {
      return json({ ok: false, error: e.message, stack: e.stack }, 500);
    }
  }
};

// ═══════════════════════════════════════════════════════════════
//   POST /api/file/:id
//   Body: { idToken }
//   Стримит файл из R2 по bitrixFileId. Метаданные (r2Key, fileName,
//   contentType) читает из /filesQueue/{id}. Используется фронтендом
//   для скачивания файлов задач/активностей в карточке.
// ═══════════════════════════════════════════════════════════════
async function handleGetFile(request, env, fileId, ctx) {
  if (!fileId || !/^[A-Za-z0-9_-]+$/.test(fileId)) {
    return json({ ok: false, error: 'invalid fileId' }, 400);
  }
  const { idToken } = await request.json().catch(() => ({}));
  if (!idToken) return json({ ok: false, error: 'idToken required' }, 400);

  const sa = JSON.parse(env.FIREBASE_SERVICE_ACCOUNT);
  const verified = await verifyFirebaseIdToken(idToken, sa);
  if (!verified) return json({ ok: false, error: 'Invalid token' }, 401);
  if (!env.BUCKET) return json({ ok: false, error: 'R2 BUCKET binding not set' }, 500);

  const accessToken = await getServiceAccountToken(sa, [
    'https://www.googleapis.com/auth/firebase.database',
    'https://www.googleapis.com/auth/userinfo.email',
  ]);
  const meta = await firebaseDbGet(sa.project_id, accessToken, `filesQueue/${fileId}`);

  // ── Если файл провалил миграцию (не в R2 / не в очереди) — пробуем
  // вытащить из Битрикса прямо сейчас. Метаданные миграции часто
  // не сохранились, но Битрикс может его всё ещё отдавать.
  const needsRescue = !meta || !meta.migrated || !meta.r2Key;
  if (needsRescue) {
    const webhook = env.BITRIX_WEBHOOK_URL;
    if (!webhook) return json({ ok: false, error: 'file not in R2 and BITRIX_WEBHOOK_URL not set' }, 404);

    // Пробуем оба метода Битрикса: для файлов задач (UF_TASK_WEBDAV_FILES)
    // ID — это AttachedObject ID, для файлов сделок — обычный File ID.
    // Сначала attachedObject, потом file.
    let bitrixMeta = await bitrixCall(webhook, 'disk.attachedObject.get', { id: fileId });
    let methodUsed = 'disk.attachedObject.get';
    if (bitrixMeta.error || !bitrixMeta.result?.DOWNLOAD_URL) {
      bitrixMeta = await bitrixCall(webhook, 'disk.file.get', { id: fileId });
      methodUsed = 'disk.file.get';
    }
    if (bitrixMeta.error || !bitrixMeta.result?.DOWNLOAD_URL) {
      return json({
        ok: false,
        error: 'file not in R2 and Bitrix не отдаёт ни через attachedObject, ни через file: ' + (bitrixMeta.error || 'no DOWNLOAD_URL'),
        meta: { migrated: !!meta?.migrated, permanentlyFailed: !!meta?.permanentlyFailed },
      }, 404);
    }
    const fileName = String(bitrixMeta.result.NAME || `file_${fileId}`);
    const fileSize = parseInt(bitrixMeta.result.SIZE) || 0;
    const fileType = String(bitrixMeta.result.TYPE || 'application/octet-stream');

    const dlRes = await fetch(bitrixMeta.result.DOWNLOAD_URL);
    if (!dlRes.ok) {
      return json({
        ok: false,
        error: `Bitrix download HTTP ${dlRes.status}`,
        bitrixName: fileName, bitrixSize: fileSize,
      }, 404);
    }
    const safeName = fileName.replace(/[\\/<>:"'|?*\x00-\x1f]/g, '_').slice(0, 200);
    const r2Key = `files/${fileId}/${safeName}`;
    const contentType = dlRes.headers.get('content-type') || fileType;
    const r2Meta = {
      httpMetadata: { contentType },
      customMetadata: {
        bitrixFileId: String(fileId),
        rescuedAt: new Date().toISOString(),
        originalName: fileName,
        rescueMethod: methodUsed,
      },
    };

    // Размер из метаданных (или Content-Length). Если файл большой —
    // нельзя буферить в arrayBuffer (Cloudflare Worker лимит 128MB RAM).
    // Для больших — стримим body.tee(): один поток в R2, второй
    // пользователю; обновление filesQueue в ctx.waitUntil после R2 PUT.
    const declaredLen = parseInt(dlRes.headers.get('content-length')) || fileSize || 0;
    const LARGE_THRESHOLD = 50 * 1024 * 1024; // 50MB — порог переключения на streaming

    if (declaredLen > LARGE_THRESHOLD || !dlRes.body) {
      // ── STREAMING (для файлов >50MB или unknown size) ──
      const [streamForR2, streamForUser] = dlRes.body.tee();
      const r2Promise = env.BUCKET.put(r2Key, streamForR2, r2Meta);

      const queueUpdates = {
        [`filesQueue/${fileId}/migrated`]: true,
        [`filesQueue/${fileId}/permanentlyFailed`]: false,
        [`filesQueue/${fileId}/r2Key`]: r2Key,
        [`filesQueue/${fileId}/fileName`]: fileName,
        [`filesQueue/${fileId}/fileSize`]: fileSize || declaredLen,
        [`filesQueue/${fileId}/contentType`]: contentType,
        [`filesQueue/${fileId}/migratedAt`]: new Date().toISOString(),
        [`filesQueue/${fileId}/lastError`]: null,
        [`filesQueue/${fileId}/rescued`]: true,
        [`filesQueue/${fileId}/rescueMode`]: 'streaming',
      };
      if (!meta) {
        queueUpdates[`filesQueue/${fileId}/bitrixFileId`] = String(fileId);
        queueUpdates[`filesQueue/${fileId}/source`] = 'rescue';
        queueUpdates[`filesQueue/${fileId}/addedAt`] = new Date().toISOString();
      }

      // Обновление filesQueue делаем после успешного R2 PUT в фоне
      if (ctx && ctx.waitUntil) {
        ctx.waitUntil((async () => {
          try {
            await r2Promise;
            await firebaseDbMultiUpdate(sa.project_id, accessToken, '/', queueUpdates);
          } catch (e) {
            console.error('streaming rescue post-R2 update failed:', e.message);
          }
        })());
      }

      const headers = {
        'Content-Type': contentType,
        'Content-Disposition': `inline; filename*=UTF-8''${encodeURIComponent(fileName)}`,
        'Cache-Control': 'private, max-age=300',
        'X-Pllato-Rescued': '1',
        'X-Pllato-Rescue-Mode': 'streaming',
        ...CORS,
      };
      if (declaredLen > 0) headers['Content-Length'] = String(declaredLen);
      return new Response(streamForUser, { headers });
    }

    // ── BUFFERED (для файлов <=50MB) — старый путь, проще ──
    const buf = await dlRes.arrayBuffer();
    await env.BUCKET.put(r2Key, buf, r2Meta);

    const queueUpdates = {
      [`filesQueue/${fileId}/migrated`]: true,
      [`filesQueue/${fileId}/permanentlyFailed`]: false,
      [`filesQueue/${fileId}/r2Key`]: r2Key,
      [`filesQueue/${fileId}/fileName`]: fileName,
      [`filesQueue/${fileId}/fileSize`]: fileSize || buf.byteLength,
      [`filesQueue/${fileId}/contentType`]: contentType,
      [`filesQueue/${fileId}/migratedAt`]: new Date().toISOString(),
      [`filesQueue/${fileId}/lastError`]: null,
      [`filesQueue/${fileId}/rescued`]: true,
    };
    if (!meta) {
      queueUpdates[`filesQueue/${fileId}/bitrixFileId`] = String(fileId);
      queueUpdates[`filesQueue/${fileId}/source`] = 'rescue';
      queueUpdates[`filesQueue/${fileId}/addedAt`] = new Date().toISOString();
    }
    await firebaseDbMultiUpdate(sa.project_id, accessToken, '/', queueUpdates);

    return new Response(buf, {
      headers: {
        'Content-Type': contentType,
        'Content-Disposition': `inline; filename*=UTF-8''${encodeURIComponent(fileName)}`,
        'Content-Length': String(buf.byteLength),
        'Cache-Control': 'private, max-age=300',
        'X-Pllato-Rescued': '1',
        ...CORS,
      },
    });
  }

  const obj = await env.BUCKET.get(meta.r2Key);
  if (!obj) return json({ ok: false, error: 'R2 object missing', r2Key: meta.r2Key }, 404);

  const fileName = meta.fileName || `file_${fileId}`;
  const safeName = encodeURIComponent(fileName);
  const headers = {
    'Content-Type': meta.contentType || obj.httpMetadata?.contentType || 'application/octet-stream',
    'Content-Disposition': `inline; filename*=UTF-8''${safeName}`,
    'Cache-Control': 'private, max-age=300',
    ...CORS,
  };
  if (obj.size != null) headers['Content-Length'] = String(obj.size);
  return new Response(obj.body, { headers });
}

// ═══════════════════════════════════════════════════════════════
//   POST /api/whoami
//   Body: { idToken }
//   Verifies Firebase ID token and returns user info
// ═══════════════════════════════════════════════════════════════
async function handleWhoami(request, env) {
  const { idToken } = await request.json();
  if (!idToken) return json({ ok: false, error: 'idToken required' }, 400);

  const sa = JSON.parse(env.FIREBASE_SERVICE_ACCOUNT);
  const verified = await verifyFirebaseIdToken(idToken, sa);
  if (!verified) return json({ ok: false, error: 'Invalid token' }, 401);

  return json({
    ok: true,
    project: sa.project_id,
    uid: verified.user_id || verified.sub,
    email: verified.email,
    name: verified.name || '',
  });
}

// ═══════════════════════════════════════════════════════════════
//   POST /api/test-bitrix
//   Verifies user is logged in, then makes a basic call to Bitrix
// ═══════════════════════════════════════════════════════════════
async function handleTestBitrix(request, env) {
  const { idToken } = await request.json();
  if (!idToken) return json({ ok: false, error: 'idToken required' }, 400);

  const sa = JSON.parse(env.FIREBASE_SERVICE_ACCOUNT);
  const verified = await verifyFirebaseIdToken(idToken, sa);
  if (!verified) return json({ ok: false, error: 'Invalid token' }, 401);

  const webhook = env.BITRIX_WEBHOOK_URL;
  if (!webhook) return json({ ok: false, error: 'BITRIX_WEBHOOK_URL not set' }, 500);

  const profile = await bitrixCall(webhook, 'profile', {});
  if (profile.error) {
    return json({ ok: false, error: 'Bitrix: ' + (profile.error_description || profile.error) });
  }

  let usersCount = 0;
  let start = 0;
  let safety = 20;
  while (safety-- > 0) {
    const r = await bitrixCall(webhook, 'user.get', { ACTIVE: true, start });
    if (r.error || !Array.isArray(r.result)) break;
    usersCount += r.result.length;
    if (r.next === undefined) break;
    start = r.next;
  }

  const portal = webhook.match(/https:\/\/([^/]+)/)?.[1] || '?';

  return json({
    ok: true,
    portal,
    user: {
      ID: profile.result.ID,
      NAME: profile.result.NAME,
      LAST_NAME: profile.result.LAST_NAME,
      EMAIL: profile.result.PERSONAL_EMAIL || profile.result.EMAIL,
    },
    usersCount,
  });
}

// ═══════════════════════════════════════════════════════════════
//   POST /api/migrate-pipeline
// ═══════════════════════════════════════════════════════════════
async function handleMigratePipeline(request, env) {
  const { idToken, categoryId } = await request.json();
  if (!idToken) return json({ ok: false, error: 'idToken required' }, 400);
  const catId = parseInt(categoryId ?? 3);

  const sa = JSON.parse(env.FIREBASE_SERVICE_ACCOUNT);
  const verified = await verifyFirebaseIdToken(idToken, sa);
  if (!verified) return json({ ok: false, error: 'Invalid token' }, 401);

  const webhook = env.BITRIX_WEBHOOK_URL;
  if (!webhook) return json({ ok: false, error: 'BITRIX_WEBHOOK_URL not set' }, 500);

  const accessToken = await getServiceAccountToken(sa, [
    'https://www.googleapis.com/auth/identitytoolkit',
    'https://www.googleapis.com/auth/firebase.database',
    'https://www.googleapis.com/auth/userinfo.email',
  ]);

  const result = await bitrixBatch(webhook, {
    cats: { method: 'crm.dealcategory.list', params: {} },
    stages: { method: 'crm.dealcategory.stage.list', params: { id: catId } },
    fields: { method: 'crm.deal.userfield.list', params: {} },
  });

  if (result.error) return json({ ok: false, error: 'Bitrix: ' + result.error });

  const cats = result.result?.result?.cats || [];
  const stages = result.result?.result?.stages || [];
  const userFields = result.result?.result?.fields || [];

  let category;
  if (catId === 0) {
    category = { ID: '0', NAME: 'Общая воронка' };
  } else {
    category = cats.find(c => parseInt(c.ID) === catId);
    if (!category) return json({ ok: false, error: `Category ${catId} not found` });
  }

  const pipelineId = `pipeline_${catId}`;
  const stagesObj = {};
  for (const s of stages) {
    stagesObj[s.STATUS_ID] = {
      name: s.NAME,
      sort: parseInt(s.SORT) || 0,
      statusId: s.STATUS_ID,
      semantics: s.SEMANTICS || null,
    };
  }

  const customFieldsMeta = {};
  for (const f of userFields) {
    customFieldsMeta[f.FIELD_NAME] = {
      label: extractLabel(f.EDIT_FORM_LABEL) || extractLabel(f.LIST_COLUMN_LABEL) || f.FIELD_NAME,
      type: f.USER_TYPE_ID,
      multiple: f.MULTIPLE === 'Y',
      mandatory: f.MANDATORY === 'Y',
      list: f.LIST || null,
      sort: parseInt(f.SORT) || 0,
    };
  }

  const pipelineData = {
    bitrixCategoryId: catId,
    name: category.NAME,
    isActive: category.IS_LOCKED !== 'Y',
    stages: stagesObj,
    stagesCount: stages.length,
    migratedAt: new Date().toISOString(),
  };

  await firebaseDbMultiUpdate(sa.project_id, accessToken, '/', {
    [`pipelines/${pipelineId}`]: pipelineData,
    [`customFieldsSchema/deal`]: customFieldsMeta,
  });

  return json({
    ok: true,
    pipelineId,
    name: category.NAME,
    stagesCount: stages.length,
    stages: Object.values(stagesObj).sort((a, b) => a.sort - b.sort),
    customFieldsCount: userFields.length,
  });
}

// ═══════════════════════════════════════════════════════════════
//   POST /api/migrate-deals
// ═══════════════════════════════════════════════════════════════
async function handleMigrateDeals(request, env) {
  const url = new URL(request.url);
  const { idToken, categoryId, offset = 0, limit = 50 } = await request.json();
  if (!idToken) return json({ ok: false, error: 'idToken required' }, 400);
  const catId = parseInt(categoryId ?? 3);

  const sa = JSON.parse(env.FIREBASE_SERVICE_ACCOUNT);
  const verified = await verifyFirebaseIdToken(idToken, sa);
  if (!verified) return json({ ok: false, error: 'Invalid token' }, 401);

  const webhook = env.BITRIX_WEBHOOK_URL;
  if (!webhook) return json({ ok: false, error: 'BITRIX_WEBHOOK_URL not set' }, 500);

  const accessToken = await getServiceAccountToken(sa, [
    'https://www.googleapis.com/auth/identitytoolkit',
    'https://www.googleapis.com/auth/firebase.database',
    'https://www.googleapis.com/auth/userinfo.email',
  ]);

  const userMappingData = await firebaseDbGet(sa.project_id, accessToken, 'userMapping/bitrix');
  const userMapping = userMappingData || {};

  const pipelineId = `pipeline_${catId}`;

  const dealsResult = await bitrixCall(webhook, 'crm.deal.list', {
    filter: { CATEGORY_ID: catId },
    select: ['*', 'UF_*'],
    order: { ID: 'ASC' },
    start: offset,
  });

  if (dealsResult.error) return json({ ok: false, error: 'Bitrix: ' + dealsResult.error });

  const deals = dealsResult.result || [];
  const totalDeals = dealsResult.total ?? 0;
  const isLast = offset + deals.length >= totalDeals;

  if (deals.length === 0) {
    return json({
      ok: true, total: totalDeals, offset, processed: 0, isLast: true,
      created: 0, updated: 0, errors: 0,
    });
  }

  const updates = {};
  const contactIds = new Set();
  const companyIds = new Set();
  let processed = 0;
  let errors = 0;
  const errorDetails = [];

  for (const d of deals) {
    try {
      const dealId = `deal_${d.ID}`;
      const responsibleUid = userMapping[d.ASSIGNED_BY_ID]?.firebaseUid || null;
      const createdByUid = userMapping[d.CREATED_BY_ID]?.firebaseUid || null;
      const modifyByUid = userMapping[d.MODIFY_BY_ID]?.firebaseUid || null;

      const customFields = {};
      for (const [k, v] of Object.entries(d)) {
        if (k.startsWith('UF_CRM_') && v !== null && v !== undefined && v !== '') {
          customFields[k] = v;
        }
      }

      if (d.CONTACT_ID && d.CONTACT_ID !== '0') contactIds.add(d.CONTACT_ID);
      if (d.COMPANY_ID && d.COMPANY_ID !== '0') companyIds.add(d.COMPANY_ID);

      const dealData = {
        bitrixId: String(d.ID),
        pipelineId,
        bitrixCategoryId: catId,
        stageId: d.STAGE_ID || null,
        title: d.TITLE || '',
        opportunity: parseFloat(d.OPPORTUNITY) || 0,
        currency: d.CURRENCY_ID || 'KZT',
        probability: parseInt(d.PROBABILITY) || null,
        contactId: d.CONTACT_ID && d.CONTACT_ID !== '0' ? `contact_${d.CONTACT_ID}` : null,
        companyId: d.COMPANY_ID && d.COMPANY_ID !== '0' ? `company_${d.COMPANY_ID}` : null,
        bitrixContactId: d.CONTACT_ID || null,
        bitrixCompanyId: d.COMPANY_ID || null,
        responsibleUid,
        createdByUid,
        modifyByUid,
        bitrixResponsibleId: d.ASSIGNED_BY_ID || null,
        bitrixCreatedById: d.CREATED_BY_ID || null,
        sourceId: d.SOURCE_ID || null,
        sourceDescription: d.SOURCE_DESCRIPTION || null,
        comments: d.COMMENTS || '',
        closed: d.CLOSED === 'Y',
        closeDate: d.CLOSEDATE || null,
        beginDate: d.BEGINDATE || null,
        bitrixDateCreate: d.DATE_CREATE || null,
        bitrixDateModify: d.DATE_MODIFY || null,
        customFields,
        migratedAt: new Date().toISOString(),
      };

      updates[`deals/${dealId}`] = dealData;
      processed++;
    } catch (e) {
      errors++;
      errorDetails.push({ bitrixId: d.ID, error: e.message });
    }
  }

  await firebaseDbMultiUpdate(sa.project_id, accessToken, '/', updates);

  await firebaseDbSet(sa.project_id, accessToken, 'migrationState/deals', {
    pipelineId,
    bitrixCategoryId: catId,
    total: totalDeals,
    processed: offset + deals.length,
    isLast,
    contactIdsToMigrate: Array.from(contactIds),
    companyIdsToMigrate: Array.from(companyIds),
    lastBatchAt: new Date().toISOString(),
  });

  return json({
    ok: true, total: totalDeals, offset, processed,
    nextOffset: isLast ? null : offset + deals.length, isLast, errors,
    errorDetails: errorDetails.slice(0, 5),
    contactIdsCollected: contactIds.size,
    companyIdsCollected: companyIds.size,
  });
}

// ═══════════════════════════════════════════════════════════════
//   POST /api/migrate-contacts
// ═══════════════════════════════════════════════════════════════
async function handleMigrateContacts(request, env) {
  const { idToken, offset = 0, limit = 50 } = await request.json();
  if (!idToken) return json({ ok: false, error: 'idToken required' }, 400);

  const sa = JSON.parse(env.FIREBASE_SERVICE_ACCOUNT);
  const verified = await verifyFirebaseIdToken(idToken, sa);
  if (!verified) return json({ ok: false, error: 'Invalid token' }, 401);

  const webhook = env.BITRIX_WEBHOOK_URL;
  if (!webhook) return json({ ok: false, error: 'BITRIX_WEBHOOK_URL not set' }, 500);

  const accessToken = await getServiceAccountToken(sa, [
    'https://www.googleapis.com/auth/identitytoolkit',
    'https://www.googleapis.com/auth/firebase.database',
    'https://www.googleapis.com/auth/userinfo.email',
  ]);

  const userMappingData = await firebaseDbGet(sa.project_id, accessToken, 'userMapping/bitrix');
  const userMapping = userMappingData || {};

  let customFieldsCount = null;
  let customFieldsMeta = null;
  if (offset === 0) {
    const ufResult = await bitrixCall(webhook, 'crm.contact.userfield.list', {});
    if (!ufResult.error) {
      const userFields = ufResult.result || [];
      customFieldsMeta = {};
      for (const f of userFields) {
        customFieldsMeta[f.FIELD_NAME] = {
          label: extractLabel(f.EDIT_FORM_LABEL) || extractLabel(f.LIST_COLUMN_LABEL) || f.FIELD_NAME,
          type: f.USER_TYPE_ID,
          multiple: f.MULTIPLE === 'Y',
          mandatory: f.MANDATORY === 'Y',
          list: f.LIST || null,
          sort: parseInt(f.SORT) || 0,
        };
      }
      customFieldsCount = userFields.length;
    }
  }

  let existingCompanyIds = new Set();
  if (offset > 0) {
    const existingState = await firebaseDbGet(sa.project_id, accessToken, 'migrationState/contacts');
    if (existingState?.companyIdsToMigrate) {
      existingCompanyIds = new Set(existingState.companyIdsToMigrate);
    }
  }

  const contactsResult = await bitrixCall(webhook, 'crm.contact.list', {
    select: ['*', 'UF_*', 'PHONE', 'EMAIL', 'WEB', 'IM'],
    order: { ID: 'ASC' },
    start: offset,
  });

  if (contactsResult.error) return json({ ok: false, error: 'Bitrix: ' + contactsResult.error });

  const contacts = contactsResult.result || [];
  const totalContacts = contactsResult.total ?? 0;
  const isLast = offset + contacts.length >= totalContacts;

  if (contacts.length === 0) {
    return json({ ok: true, total: totalContacts, offset, processed: 0, isLast: true, errors: 0 });
  }

  const updates = {};
  let processed = 0;
  let errors = 0;
  const errorDetails = [];

  for (const c of contacts) {
    try {
      const contactId = `contact_${c.ID}`;
      const responsibleUid = userMapping[c.ASSIGNED_BY_ID]?.firebaseUid || null;
      const createdByUid = userMapping[c.CREATED_BY_ID]?.firebaseUid || null;
      const modifyByUid = userMapping[c.MODIFY_BY_ID]?.firebaseUid || null;

      const phones = Array.isArray(c.PHONE)
        ? c.PHONE.map(p => ({ value: p.VALUE || '', type: p.VALUE_TYPE || 'WORK' })).filter(p => p.value) : [];
      const emails = Array.isArray(c.EMAIL)
        ? c.EMAIL.map(e => ({ value: e.VALUE || '', type: e.VALUE_TYPE || 'WORK' })).filter(e => e.value) : [];
      const websites = Array.isArray(c.WEB)
        ? c.WEB.map(w => ({ value: w.VALUE || '', type: w.VALUE_TYPE || 'WORK' })).filter(w => w.value) : [];
      const messengers = Array.isArray(c.IM)
        ? c.IM.map(m => ({ value: m.VALUE || '', type: m.VALUE_TYPE || 'OTHER' })).filter(m => m.value) : [];

      const customFields = {};
      for (const [k, v] of Object.entries(c)) {
        if (k.startsWith('UF_CRM_') && v !== null && v !== undefined && v !== '') {
          customFields[k] = v;
        }
      }

      if (c.COMPANY_ID && c.COMPANY_ID !== '0') existingCompanyIds.add(String(c.COMPANY_ID));

      const contactData = {
        bitrixId: String(c.ID),
        name: c.NAME || '',
        lastName: c.LAST_NAME || '',
        secondName: c.SECOND_NAME || '',
        honorific: c.HONORIFIC || null,
        position: c.POST || '',
        comments: c.COMMENTS || '',
        type: c.TYPE_ID || null,
        sourceId: c.SOURCE_ID || null,
        sourceDescription: c.SOURCE_DESCRIPTION || null,
        birthdate: c.BIRTHDATE || null,
        phones, emails, websites, messengers,
        address: c.ADDRESS || null,
        addressCity: c.ADDRESS_CITY || null,
        addressRegion: c.ADDRESS_REGION || null,
        addressProvince: c.ADDRESS_PROVINCE || null,
        addressCountry: c.ADDRESS_COUNTRY || null,
        addressPostalCode: c.ADDRESS_POSTAL_CODE || null,
        companyId: c.COMPANY_ID && c.COMPANY_ID !== '0' ? `company_${c.COMPANY_ID}` : null,
        bitrixCompanyId: c.COMPANY_ID || null,
        leadId: c.LEAD_ID || null,
        responsibleUid, createdByUid, modifyByUid,
        bitrixResponsibleId: c.ASSIGNED_BY_ID || null,
        bitrixCreatedById: c.CREATED_BY_ID || null,
        bitrixDateCreate: c.DATE_CREATE || null,
        bitrixDateModify: c.DATE_MODIFY || null,
        opened: c.OPENED === 'Y',
        export: c.EXPORT === 'Y',
        customFields,
        migratedAt: new Date().toISOString(),
      };

      updates[`contacts/${contactId}`] = contactData;
      processed++;
    } catch (e) {
      errors++;
      errorDetails.push({ bitrixId: c.ID, error: e.message });
    }
  }

  if (customFieldsMeta) updates['customFieldsSchema/contact'] = customFieldsMeta;

  await firebaseDbMultiUpdate(sa.project_id, accessToken, '/', updates);

  await firebaseDbSet(sa.project_id, accessToken, 'migrationState/contacts', {
    total: totalContacts,
    processed: offset + contacts.length,
    isLast,
    companyIdsToMigrate: Array.from(existingCompanyIds),
    lastBatchAt: new Date().toISOString(),
  });

  return json({
    ok: true, total: totalContacts, offset, processed,
    nextOffset: isLast ? null : offset + contacts.length, isLast, errors,
    errorDetails: errorDetails.slice(0, 5),
    companyIdsCollected: existingCompanyIds.size,
    customFieldsCount,
  });
}

// ═══════════════════════════════════════════════════════════════
//   POST /api/migrate-companies
// ═══════════════════════════════════════════════════════════════
async function handleMigrateCompanies(request, env) {
  const { idToken } = await request.json();
  if (!idToken) return json({ ok: false, error: 'idToken required' }, 400);

  const sa = JSON.parse(env.FIREBASE_SERVICE_ACCOUNT);
  const verified = await verifyFirebaseIdToken(idToken, sa);
  if (!verified) return json({ ok: false, error: 'Invalid token' }, 401);

  const webhook = env.BITRIX_WEBHOOK_URL;
  if (!webhook) return json({ ok: false, error: 'BITRIX_WEBHOOK_URL not set' }, 500);

  const accessToken = await getServiceAccountToken(sa, [
    'https://www.googleapis.com/auth/identitytoolkit',
    'https://www.googleapis.com/auth/firebase.database',
    'https://www.googleapis.com/auth/userinfo.email',
  ]);

  const userMappingData = await firebaseDbGet(sa.project_id, accessToken, 'userMapping/bitrix');
  const userMapping = userMappingData || {};

  const ufResult = await bitrixCall(webhook, 'crm.company.userfield.list', {});
  const customFieldsMeta = {};
  if (!ufResult.error) {
    for (const f of (ufResult.result || [])) {
      customFieldsMeta[f.FIELD_NAME] = {
        label: extractLabel(f.EDIT_FORM_LABEL) || extractLabel(f.LIST_COLUMN_LABEL) || f.FIELD_NAME,
        type: f.USER_TYPE_ID,
        multiple: f.MULTIPLE === 'Y',
        mandatory: f.MANDATORY === 'Y',
        list: f.LIST || null,
        sort: parseInt(f.SORT) || 0,
      };
    }
  }

  const allCompanies = [];
  let start = 0;
  let total = 0;
  while (true) {
    const r = await bitrixCall(webhook, 'crm.company.list', {
      select: ['*', 'UF_*', 'PHONE', 'EMAIL', 'WEB'],
      order: { ID: 'ASC' },
      start,
    });
    if (r.error) return json({ ok: false, error: 'Bitrix: ' + r.error });
    const batch = r.result || [];
    allCompanies.push(...batch);
    total = r.total ?? allCompanies.length;
    if (batch.length === 0 || allCompanies.length >= total) break;
    start = r.next ?? (start + batch.length);
    if (start === undefined || start === null) break;
    if (allCompanies.length > 5000) break;
  }

  const updates = {};
  let processed = 0;
  let errors = 0;
  const errorDetails = [];

  for (const c of allCompanies) {
    try {
      const companyId = `company_${c.ID}`;
      const responsibleUid = userMapping[c.ASSIGNED_BY_ID]?.firebaseUid || null;
      const createdByUid = userMapping[c.CREATED_BY_ID]?.firebaseUid || null;
      const modifyByUid = userMapping[c.MODIFY_BY_ID]?.firebaseUid || null;

      const phones = Array.isArray(c.PHONE)
        ? c.PHONE.map(p => ({ value: p.VALUE || '', type: p.VALUE_TYPE || 'WORK' })).filter(p => p.value) : [];
      const emails = Array.isArray(c.EMAIL)
        ? c.EMAIL.map(e => ({ value: e.VALUE || '', type: e.VALUE_TYPE || 'WORK' })).filter(e => e.value) : [];
      const websites = Array.isArray(c.WEB)
        ? c.WEB.map(w => ({ value: w.VALUE || '', type: w.VALUE_TYPE || 'WORK' })).filter(w => w.value) : [];

      const customFields = {};
      for (const [k, v] of Object.entries(c)) {
        if (k.startsWith('UF_CRM_') && v !== null && v !== undefined && v !== '') {
          customFields[k] = v;
        }
      }

      const companyData = {
        bitrixId: String(c.ID),
        title: c.TITLE || '',
        companyType: c.COMPANY_TYPE || null,
        industry: c.INDUSTRY || null,
        employees: c.EMPLOYEES || null,
        revenue: c.REVENUE ? parseFloat(c.REVENUE) : null,
        currency: c.CURRENCY_ID || null,
        comments: c.COMMENTS || '',
        phones, emails, websites,
        address: c.ADDRESS || null,
        addressCity: c.ADDRESS_CITY || null,
        addressRegion: c.ADDRESS_REGION || null,
        addressCountry: c.ADDRESS_COUNTRY || null,
        addressPostalCode: c.ADDRESS_POSTAL_CODE || null,
        responsibleUid, createdByUid, modifyByUid,
        bitrixResponsibleId: c.ASSIGNED_BY_ID || null,
        bitrixCreatedById: c.CREATED_BY_ID || null,
        bitrixDateCreate: c.DATE_CREATE || null,
        bitrixDateModify: c.DATE_MODIFY || null,
        opened: c.OPENED === 'Y',
        customFields,
        migratedAt: new Date().toISOString(),
      };

      updates[`companies/${companyId}`] = companyData;
      processed++;
    } catch (e) {
      errors++;
      errorDetails.push({ bitrixId: c.ID, error: e.message });
    }
  }

  updates['customFieldsSchema/company'] = customFieldsMeta;
  await firebaseDbMultiUpdate(sa.project_id, accessToken, '/', updates);

  await firebaseDbSet(sa.project_id, accessToken, 'migrationState/companies', {
    total: allCompanies.length, processed, isLast: true,
    migratedAt: new Date().toISOString(),
  });

  return json({
    ok: true, total: allCompanies.length, processed, errors,
    errorDetails: errorDetails.slice(0, 5),
    customFieldsCount: Object.keys(customFieldsMeta).length,
    sampleNames: allCompanies.slice(0, 10).map(c => c.TITLE).filter(Boolean),
  });
}

// ═══════════════════════════════════════════════════════════════
//   POST /api/diagnose-tasks
// ═══════════════════════════════════════════════════════════════
async function handleDiagnoseTasks(request, env) {
  const { idToken } = await request.json();
  if (!idToken) return json({ ok: false, error: 'idToken required' }, 400);

  const sa = JSON.parse(env.FIREBASE_SERVICE_ACCOUNT);
  const verified = await verifyFirebaseIdToken(idToken, sa);
  if (!verified) return json({ ok: false, error: 'Invalid token' }, 401);

  const webhook = env.BITRIX_WEBHOOK_URL;
  if (!webhook) return json({ ok: false, error: 'BITRIX_WEBHOOK_URL not set' }, 500);

  const totalRes = await bitrixCall(webhook, 'tasks.task.list', { filter: {}, select: ['ID'], start: 0 });
  if (totalRes.error) return json({ ok: false, error: 'Bitrix tasks.task.list: ' + totalRes.error });

  const totalTasks = totalRes.total ?? 0;
  const STATUS_NAMES = {
    '1': 'Ожидает выполнения', '2': 'Выполняется', '3': 'Ждёт контроля',
    '4': 'Завершена (ждёт)', '5': 'Завершена', '6': 'Отложена', '7': 'Отклонена',
  };

  const detailed = await bitrixCall(webhook, 'tasks.task.list', {
    filter: {},
    select: ['ID', 'TITLE', 'STATUS', 'RESPONSIBLE_ID', 'CREATED_BY', 'CREATED_DATE',
             'DEADLINE', 'PRIORITY', 'GROUP_ID', 'PARENT_ID',
             'UF_CRM_TASK', 'TIME_ESTIMATE', 'COMMENTS_COUNT', 'CHECKLIST'],
    start: 0,
  });
  const sample = detailed.result?.tasks || detailed.result || [];

  const byStatus = {};
  let withDeadline = 0, withCrmLink = 0, withComments = 0, withChecklist = 0, withGroup = 0;
  const responsibleSet = new Set();

  for (const t of sample) {
    const st = String(t.status ?? t.STATUS ?? '');
    byStatus[st] = (byStatus[st] || 0) + 1;
    if (t.deadline ?? t.DEADLINE) withDeadline++;
    const crmLink = t.ufCrmTask ?? t.UF_CRM_TASK;
    if (crmLink && (Array.isArray(crmLink) ? crmLink.length > 0 : true)) withCrmLink++;
    const cc = parseInt(t.commentsCount ?? t.COMMENTS_COUNT ?? 0);
    if (cc > 0) withComments++;
    const cl = t.checklist ?? t.CHECKLIST;
    if (cl && (Array.isArray(cl) ? cl.length > 0 : Object.keys(cl).length > 0)) withChecklist++;
    if ((t.groupId ?? t.GROUP_ID) && parseInt(t.groupId ?? t.GROUP_ID) > 0) withGroup++;
    const resp = t.responsibleId ?? t.RESPONSIBLE_ID;
    if (resp) responsibleSet.add(String(resp));
  }

  let groupsCount = 0;
  try {
    const groupsRes = await bitrixCall(webhook, 'sonet_group.get', { ORDER: { ID: 'ASC' } });
    if (!groupsRes.error) groupsCount = (groupsRes.result || []).length;
  } catch {}

  let tasksWithFiles = 0;
  let estimatedTotalFiles = 0;
  try {
    const fileSample = await bitrixCall(webhook, 'tasks.task.list', {
      filter: {}, select: ['ID', 'UF_TASK_WEBDAV_FILES'], start: 0,
    });
    const fs = fileSample.result?.tasks || fileSample.result || [];
    for (const t of fs) {
      const files = t.ufTaskWebdavFiles ?? t.UF_TASK_WEBDAV_FILES;
      if (files && Array.isArray(files) && files.length > 0) {
        tasksWithFiles++;
        estimatedTotalFiles += files.length;
      }
    }
    if (fs.length > 0 && totalTasks > 0) {
      const ratio = totalTasks / fs.length;
      estimatedTotalFiles = Math.round(estimatedTotalFiles * ratio);
    }
  } catch {}

  return json({
    ok: true, totalTasks, sampleSize: sample.length,
    byStatus: Object.entries(byStatus).map(([k, v]) => ({
      status: k, name: STATUS_NAMES[k] || `Status ${k}`, count: v,
    })),
    withDeadline, withCrmLink, withComments, withChecklist, withGroup,
    uniqueResponsibles: responsibleSet.size, groupsCount,
    tasksWithFilesInSample: tasksWithFiles, estimatedTotalFiles,
    estimatedMigrationMinutes: Math.ceil((totalTasks / 50) * 3 / 60),
  });
}

// ═══════════════════════════════════════════════════════════════
//   POST /api/diagnose-activities
// ═══════════════════════════════════════════════════════════════
async function handleDiagnoseActivities(request, env) {
  const { idToken } = await request.json();
  if (!idToken) return json({ ok: false, error: 'idToken required' }, 400);

  const sa = JSON.parse(env.FIREBASE_SERVICE_ACCOUNT);
  const verified = await verifyFirebaseIdToken(idToken, sa);
  if (!verified) return json({ ok: false, error: 'Invalid token' }, 401);

  const webhook = env.BITRIX_WEBHOOK_URL;
  if (!webhook) return json({ ok: false, error: 'BITRIX_WEBHOOK_URL not set' }, 500);

  const allRes = await bitrixCall(webhook, 'crm.activity.list', { select: ['ID'], start: 0 });
  if (allRes.error) return json({ ok: false, error: 'Bitrix: ' + allRes.error });
  const totalAll = allRes.total ?? 0;

  const dealActivitiesRes = await bitrixCall(webhook, 'crm.activity.list', {
    filter: { OWNER_TYPE_ID: 2 }, select: ['ID'], start: 0,
  });
  const totalDealActivities = dealActivitiesRes.total ?? 0;

  const contactActivitiesRes = await bitrixCall(webhook, 'crm.activity.list', {
    filter: { OWNER_TYPE_ID: 3 }, select: ['ID'], start: 0,
  });
  const totalContactActivities = contactActivitiesRes.total ?? 0;

  const sampleRes = await bitrixCall(webhook, 'crm.activity.list', {
    select: ['ID', 'TYPE_ID', 'PROVIDER_ID', 'PROVIDER_TYPE_ID', 'DIRECTION',
             'COMPLETED', 'OWNER_TYPE_ID', 'OWNER_ID', 'RESPONSIBLE_ID',
             'SUBJECT', 'DESCRIPTION_TYPE', 'CREATED', 'FILES'],
    order: { ID: 'DESC' }, start: 0,
  });
  const sample = sampleRes.result || [];

  const TYPE_NAMES = { '1': 'Встреча', '2': 'Звонок', '3': 'Задача', '4': 'Email', '5': 'Активность', '6': 'Провайдер (мессенджер)' };
  const byType = {}, byProvider = {}, byOwnerType = {};
  let withFiles = 0, totalFilesInSample = 0, completed = 0;

  for (const a of sample) {
    const t = String(a.TYPE_ID || '');
    byType[t] = (byType[t] || 0) + 1;
    if (a.PROVIDER_ID) byProvider[a.PROVIDER_ID] = (byProvider[a.PROVIDER_ID] || 0) + 1;
    const ot = String(a.OWNER_TYPE_ID || '');
    byOwnerType[ot] = (byOwnerType[ot] || 0) + 1;
    if (a.FILES && Array.isArray(a.FILES) && a.FILES.length > 0) {
      withFiles++;
      totalFilesInSample += a.FILES.length;
    }
    if (a.COMPLETED === 'Y') completed++;
  }

  const filesRatio = sample.length > 0 ? totalFilesInSample / sample.length : 0;
  const estimatedTotalFiles = Math.round(filesRatio * totalAll);

  let totalDealsAll = 0;
  try {
    const allDealsRes = await bitrixCall(webhook, 'crm.deal.list', { select: ['ID'], start: 0 });
    totalDealsAll = allDealsRes.total ?? 0;
  } catch {}

  const uniqBookShare = totalDealsAll > 0 ? Math.round(totalDealActivities * (22609 / totalDealsAll)) : 0;

  return json({
    ok: true, totalAll, totalDealActivities, totalContactActivities,
    estimatedUniqBookActivities: uniqBookShare, totalDealsAll,
    sampleSize: sample.length,
    byType: Object.entries(byType).map(([k, v]) => ({ type: k, name: TYPE_NAMES[k] || `Type ${k}`, count: v })),
    byProvider: Object.entries(byProvider).map(([k, v]) => ({ provider: k, count: v })),
    byOwnerType: Object.entries(byOwnerType).map(([k, v]) => ({
      ownerType: k, name: { '1': 'Lead', '2': 'Deal', '3': 'Contact', '4': 'Company' }[k] || k, count: v,
    })),
    completedInSample: completed,
    activitiesWithFilesInSample: withFiles,
    avgFilesPerActivityWithFiles: withFiles > 0 ? (totalFilesInSample / withFiles).toFixed(1) : 0,
    estimatedTotalFiles,
    estimatedMigrationMinutes: Math.ceil((totalAll / 50) * 3 / 60),
  });
}

// ═══════════════════════════════════════════════════════════════
//   POST /api/migrate-activities
// ═══════════════════════════════════════════════════════════════
async function handleMigrateActivities(request, env) {
  const { idToken, offset = 0, limit = 50 } = await request.json();
  if (!idToken) return json({ ok: false, error: 'idToken required' }, 400);

  const sa = JSON.parse(env.FIREBASE_SERVICE_ACCOUNT);
  const verified = await verifyFirebaseIdToken(idToken, sa);
  if (!verified) return json({ ok: false, error: 'Invalid token' }, 401);

  const webhook = env.BITRIX_WEBHOOK_URL;
  if (!webhook) return json({ ok: false, error: 'BITRIX_WEBHOOK_URL not set' }, 500);

  const accessToken = await getServiceAccountToken(sa, [
    'https://www.googleapis.com/auth/identitytoolkit',
    'https://www.googleapis.com/auth/firebase.database',
    'https://www.googleapis.com/auth/userinfo.email',
  ]);

  const userMappingData = await firebaseDbGet(sa.project_id, accessToken, 'userMapping/bitrix');
  const userMapping = userMappingData || {};

  const result = await bitrixCall(webhook, 'crm.activity.list', {
    select: ['*'], order: { ID: 'ASC' }, start: offset,
  });

  if (result.error) return json({ ok: false, error: 'Bitrix: ' + result.error });

  const activities = result.result || [];
  const totalActivities = result.total ?? 0;
  const isLast = offset + activities.length >= totalActivities;

  if (activities.length === 0) {
    return json({ ok: true, total: totalActivities, offset, processed: 0, isLast: true, errors: 0 });
  }

  const updates = {};
  let processed = 0, errors = 0;
  const errorDetails = [];
  const fileIdsCollected = [];
  const ownerTypeStats = {};

  const OWNER_PREFIX = { '1': 'lead', '2': 'deal', '3': 'contact', '4': 'company' };
  const TYPE_NAMES = { '1': 'meeting', '2': 'call', '3': 'task', '4': 'email', '5': 'activity', '6': 'provider' };

  for (const a of activities) {
    try {
      const activityId = `activity_${a.ID}`;
      const ownerTypeId = String(a.OWNER_TYPE_ID || '0');
      const ownerPrefix = OWNER_PREFIX[ownerTypeId] || 'unknown';
      const ownerKey = a.OWNER_ID && a.OWNER_ID !== '0' ? `${ownerPrefix}_${a.OWNER_ID}` : 'orphan';

      ownerTypeStats[ownerPrefix] = (ownerTypeStats[ownerPrefix] || 0) + 1;

      const responsibleUid = userMapping[a.RESPONSIBLE_ID]?.firebaseUid || null;
      const authorUid = userMapping[a.AUTHOR_ID]?.firebaseUid || null;
      const editorUid = userMapping[a.EDITOR_ID]?.firebaseUid || null;

      const files = [];
      const bitrixFileIds = [];
      if (Array.isArray(a.FILES)) {
        for (const f of a.FILES) {
          if (f && typeof f === 'object') {
            const fileId = f.id || f.ID || f.fileId || null;
            if (fileId) {
              bitrixFileIds.push(String(fileId));
              files.push({
                bitrixFileId: String(fileId),
                name: f.name || f.NAME || '',
                size: parseInt(f.size || f.SIZE) || 0,
                type: f.type || f.TYPE || '',
                url: f.url || f.URL || null,
                migratedToR2: false, r2Key: null,
              });
              fileIdsCollected.push(String(fileId));
            }
          } else if (typeof f === 'string' || typeof f === 'number') {
            bitrixFileIds.push(String(f));
            files.push({ bitrixFileId: String(f), migratedToR2: false, r2Key: null });
            fileIdsCollected.push(String(f));
          }
        }
      }

      if (a.RECORD_FILE_ID && a.RECORD_FILE_ID !== '0') {
        bitrixFileIds.push(String(a.RECORD_FILE_ID));
        fileIdsCollected.push(String(a.RECORD_FILE_ID));
      }

      const activityData = {
        bitrixId: String(a.ID),
        type: TYPE_NAMES[String(a.TYPE_ID)] || 'unknown',
        bitrixTypeId: a.TYPE_ID || null,
        subject: a.SUBJECT || '',
        description: a.DESCRIPTION || '',
        descriptionType: a.DESCRIPTION_TYPE || null,
        ownerType: ownerPrefix,
        ownerId: a.OWNER_ID && a.OWNER_ID !== '0' ? `${ownerPrefix}_${a.OWNER_ID}` : null,
        bitrixOwnerType: a.OWNER_TYPE_ID || null,
        bitrixOwnerId: a.OWNER_ID || null,
        responsibleUid, authorUid, editorUid,
        bitrixResponsibleId: a.RESPONSIBLE_ID || null,
        bitrixAuthorId: a.AUTHOR_ID || null,
        completed: a.COMPLETED === 'Y',
        direction: a.DIRECTION || null,
        priority: parseInt(a.PRIORITY) || null,
        start: a.START_TIME || null,
        end: a.END_TIME || null,
        deadline: a.DEADLINE || null,
        bitrixCreated: a.CREATED || null,
        bitrixLastUpdated: a.LAST_UPDATED || null,
        providerId: a.PROVIDER_ID || null,
        providerTypeId: a.PROVIDER_TYPE_ID || null,
        providerGroupId: a.PROVIDER_GROUP_ID || null,
        providerParams: a.PROVIDER_PARAMS || null,
        associatedEntityId: a.ASSOCIATED_ENTITY_ID || null,
        callDuration: a.SETTINGS?.DURATION || a.CALL_DURATION || null,
        recordFileId: a.RECORD_FILE_ID && a.RECORD_FILE_ID !== '0' ? String(a.RECORD_FILE_ID) : null,
        settings: a.SETTINGS || null,
        files, bitrixFileIds,
        hasFiles: bitrixFileIds.length > 0,
        filesMigrated: bitrixFileIds.length === 0,
        migratedAt: new Date().toISOString(),
      };

      updates[`timeline/${ownerKey}/${activityId}`] = activityData;
      updates[`activitiesIndex/${a.ID}`] = { ownerKey, activityId, type: activityData.type };

      processed++;
    } catch (e) {
      errors++;
      errorDetails.push({ bitrixId: a.ID, error: e.message });
    }
  }

  await firebaseDbMultiUpdate(sa.project_id, accessToken, '/', updates);

  let existingFileIds = new Set();
  if (offset > 0) {
    const existing = await firebaseDbGet(sa.project_id, accessToken, 'migrationState/activities');
    if (existing?.fileIdsCollected && Array.isArray(existing.fileIdsCollected)) {
      if (existing.fileIdsCount && existing.fileIdsCount > 5000) {
        existingFileIds = null;
      } else {
        existingFileIds = new Set(existing.fileIdsCollected);
      }
    }
  }

  let stateUpdate;
  if (existingFileIds === null) {
    const existing = await firebaseDbGet(sa.project_id, accessToken, 'migrationState/activities');
    stateUpdate = {
      total: totalActivities,
      processed: offset + activities.length,
      isLast,
      fileIdsCount: (existing?.fileIdsCount || 0) + fileIdsCollected.length,
      fileIdsCollected: null,
      lastBatchAt: new Date().toISOString(),
    };
  } else {
    for (const id of fileIdsCollected) existingFileIds.add(id);
    stateUpdate = {
      total: totalActivities,
      processed: offset + activities.length,
      isLast,
      fileIdsCount: existingFileIds.size,
      fileIdsCollected: existingFileIds.size <= 5000 ? Array.from(existingFileIds) : null,
      lastBatchAt: new Date().toISOString(),
    };
  }

  await firebaseDbSet(sa.project_id, accessToken, 'migrationState/activities', stateUpdate);

  return json({
    ok: true, total: totalActivities, offset, processed,
    nextOffset: isLast ? null : offset + activities.length, isLast, errors,
    errorDetails: errorDetails.slice(0, 5),
    fileIdsInBatch: fileIdsCollected.length,
    totalFileIds: stateUpdate.fileIdsCount,
    ownerTypeStats,
  });
}

// ═══════════════════════════════════════════════════════════════
//   POST /api/migrate-tasks
// ═══════════════════════════════════════════════════════════════
async function handleMigrateTasks(request, env) {
  const { idToken, offset = 0, limit = 50 } = await request.json();
  if (!idToken) return json({ ok: false, error: 'idToken required' }, 400);

  const sa = JSON.parse(env.FIREBASE_SERVICE_ACCOUNT);
  const verified = await verifyFirebaseIdToken(idToken, sa);
  if (!verified) return json({ ok: false, error: 'Invalid token' }, 401);

  const webhook = env.BITRIX_WEBHOOK_URL;
  if (!webhook) return json({ ok: false, error: 'BITRIX_WEBHOOK_URL not set' }, 500);

  const accessToken = await getServiceAccountToken(sa, [
    'https://www.googleapis.com/auth/identitytoolkit',
    'https://www.googleapis.com/auth/firebase.database',
    'https://www.googleapis.com/auth/userinfo.email',
  ]);

  const userMappingData = await firebaseDbGet(sa.project_id, accessToken, 'userMapping/bitrix');
  const userMapping = userMappingData || {};

  const result = await bitrixCall(webhook, 'tasks.task.list', {
    filter: {},
    select: ['ID', 'TITLE', 'DESCRIPTION', 'STATUS', 'PRIORITY',
             'RESPONSIBLE_ID', 'CREATED_BY', 'CHANGED_BY', 'AUDITORS', 'ACCOMPLICES',
             'CREATED_DATE', 'CHANGED_DATE', 'CLOSED_DATE',
             'DEADLINE', 'START_DATE_PLAN', 'END_DATE_PLAN',
             'GROUP_ID', 'PARENT_ID', 'STAGE_ID',
             'UF_CRM_TASK', 'UF_TASK_WEBDAV_FILES',
             'TIME_ESTIMATE', 'TIME_SPENT_IN_LOGS',
             'COMMENTS_COUNT', 'TAGS', 'ALLOW_CHANGE_DEADLINE',
             'TASK_CONTROL', 'ADD_IN_REPORT', 'MARK', 'STATUS_CHANGED_DATE'],
    order: { ID: 'ASC' }, start: offset,
  });

  if (result.error) return json({ ok: false, error: 'Bitrix tasks.task.list: ' + result.error });

  const tasks = result.result?.tasks || result.result || [];
  const totalTasks = result.total ?? 0;
  const isLast = offset + tasks.length >= totalTasks;

  if (tasks.length === 0) {
    return json({ ok: true, total: totalTasks, offset, processed: 0, isLast: true, errors: 0 });
  }

  const updates = {};
  let processed = 0, errors = 0;
  const errorDetails = [];
  const fileIdsCollected = [];

  for (const t of tasks) {
    try {
      const taskId = `task_${t.id}`;
      const responsibleUid = userMapping[t.responsibleId]?.firebaseUid || null;
      const createdByUid = userMapping[t.createdBy]?.firebaseUid || null;
      const changedByUid = userMapping[t.changedBy]?.firebaseUid || null;

      const bitrixFileIds = [];
      const filesRaw = t.ufTaskWebdavFiles || t.UF_TASK_WEBDAV_FILES;
      if (Array.isArray(filesRaw)) {
        for (const f of filesRaw) {
          const fid = typeof f === 'object' ? (f.id || f.ID) : f;
          if (fid) {
            bitrixFileIds.push(String(fid));
            fileIdsCollected.push(String(fid));
          }
        }
      }

      const accomplices = Array.isArray(t.accomplices)
        ? t.accomplices.map(uid => ({ bitrixId: String(uid), firebaseUid: userMapping[uid]?.firebaseUid || null })) : [];
      const auditors = Array.isArray(t.auditors)
        ? t.auditors.map(uid => ({ bitrixId: String(uid), firebaseUid: userMapping[uid]?.firebaseUid || null })) : [];

      const crmLinksRaw = t.ufCrmTask || t.UF_CRM_TASK;
      const crmLinks = [];
      const PREFIX_MAP = { 'L': 'lead', 'D': 'deal', 'C': 'contact', 'CO': 'company' };
      if (Array.isArray(crmLinksRaw)) {
        for (const link of crmLinksRaw) {
          const m = String(link).match(/^([A-Z]+)_(\d+)$/);
          if (m) {
            const prefix = PREFIX_MAP[m[1]];
            if (prefix) crmLinks.push(`${prefix}_${m[2]}`);
          }
        }
      }

      const taskData = {
        bitrixId: String(t.id),
        title: t.title || '',
        description: t.description || '',
        status: parseInt(t.status) || null,
        priority: parseInt(t.priority) || null,
        responsibleUid, createdByUid, changedByUid,
        bitrixResponsibleId: t.responsibleId || null,
        bitrixCreatedBy: t.createdBy || null,
        accomplices, auditors,
        bitrixCreatedDate: t.createdDate || null,
        bitrixChangedDate: t.changedDate || null,
        bitrixClosedDate: t.closedDate || null,
        bitrixStatusChangedDate: t.statusChangedDate || null,
        deadline: t.deadline || null,
        startDatePlan: t.startDatePlan || null,
        endDatePlan: t.endDatePlan || null,
        groupId: t.groupId && parseInt(t.groupId) > 0 ? String(t.groupId) : null,
        parentId: t.parentId && parseInt(t.parentId) > 0 ? `task_${t.parentId}` : null,
        bitrixParentId: t.parentId || null,
        stageId: t.stageId || null,
        crmLinks,
        bitrixCrmLinks: crmLinksRaw || [],
        timeEstimate: parseInt(t.timeEstimate) || 0,
        timeSpent: parseInt(t.timeSpentInLogs) || 0,
        commentsCount: parseInt(t.commentsCount) || 0,
        taskControl: t.taskControl === 'Y',
        addInReport: t.addInReport === 'Y',
        allowChangeDeadline: t.allowChangeDeadline === 'Y',
        mark: t.mark || null,
        tags: Array.isArray(t.tags) ? t.tags : [],
        bitrixFileIds,
        hasFiles: bitrixFileIds.length > 0,
        filesMigrated: bitrixFileIds.length === 0,
        commentsMigrated: false,
        migratedAt: new Date().toISOString(),
      };

      updates[`tasks/${taskId}`] = taskData;
      processed++;
    } catch (e) {
      errors++;
      errorDetails.push({ bitrixId: t.id, error: e.message });
    }
  }

  await firebaseDbMultiUpdate(sa.project_id, accessToken, '/', updates);

  const existing = offset > 0 ? await firebaseDbGet(sa.project_id, accessToken, 'migrationState/tasks') : null;
  const newFileCount = (existing?.fileIdsCount || 0) + fileIdsCollected.length;

  await firebaseDbSet(sa.project_id, accessToken, 'migrationState/tasks', {
    total: totalTasks, processed: offset + tasks.length, isLast,
    fileIdsCount: newFileCount, lastBatchAt: new Date().toISOString(),
  });

  return json({
    ok: true, total: totalTasks, offset, processed,
    nextOffset: isLast ? null : offset + tasks.length, isLast, errors,
    errorDetails: errorDetails.slice(0, 5),
    fileIdsInBatch: fileIdsCollected.length, totalFileIds: newFileCount,
  });
}

// ═══════════════════════════════════════════════════════════════
//   POST /api/migrate-task-comments
// ═══════════════════════════════════════════════════════════════
async function handleMigrateTaskComments(request, env) {
  const { idToken, offset = 0, limit = 25 } = await request.json();
  if (!idToken) return json({ ok: false, error: 'idToken required' }, 400);

  const safeLimit = Math.max(1, Math.min(parseInt(limit) || 25, 40));

  const sa = JSON.parse(env.FIREBASE_SERVICE_ACCOUNT);
  const verified = await verifyFirebaseIdToken(idToken, sa);
  if (!verified) return json({ ok: false, error: 'Invalid token' }, 401);

  const webhook = env.BITRIX_WEBHOOK_URL;
  if (!webhook) return json({ ok: false, error: 'BITRIX_WEBHOOK_URL not set' }, 500);

  const accessToken = await getServiceAccountToken(sa, [
    'https://www.googleapis.com/auth/identitytoolkit',
    'https://www.googleapis.com/auth/firebase.database',
    'https://www.googleapis.com/auth/userinfo.email',
  ]);

  const userMappingData = await firebaseDbGet(sa.project_id, accessToken, 'userMapping/bitrix');
  const userMapping = userMappingData || {};

  const tasksRes = await bitrixCall(webhook, 'tasks.task.list', {
    filter: { '!COMMENTS_COUNT': 0 },
    select: ['ID', 'COMMENTS_COUNT'],
    order: { ID: 'ASC' }, start: offset,
  });

  if (tasksRes.error) return json({ ok: false, error: 'Bitrix tasks.task.list: ' + tasksRes.error });

  const allTasks = tasksRes.result?.tasks || tasksRes.result || [];
  const totalTasks = tasksRes.total ?? 0;

  const tasks = allTasks.slice(0, safeLimit);
  const isLast = offset + tasks.length >= totalTasks;

  if (tasks.length === 0) {
    return json({ ok: true, total: totalTasks, offset, processed: 0, isLast: true, errors: 0, limitUsed: safeLimit });
  }

  const commands = {};
  for (const t of tasks) {
    commands[`c${t.id}`] = { method: 'task.commentitem.getlist', params: { TASKID: t.id } };
  }

  const batchRes = await bitrixBatch(webhook, commands);
  if (batchRes.error) {
    return json({
      ok: false, error: 'Bitrix batch: ' + batchRes.error,
      shouldRetryWithSmallerLimit: safeLimit > 1, currentLimit: safeLimit,
    });
  }

  const batchResults = batchRes.result?.result || {};
  const batchErrors = batchRes.result?.result_error || {};

  const errKeys = Object.keys(batchErrors);
  const resultKeys = Object.keys(batchResults);
  if (errKeys.length === tasks.length && resultKeys.length === 0) {
    const firstErr = batchErrors[errKeys[0]];
    const code = typeof firstErr === 'object' ? (firstErr.error || '') : '';
    const desc = typeof firstErr === 'object' ? (firstErr.error_description || JSON.stringify(firstErr)) : String(firstErr);
    if (code === 'ERROR_METHOD_NOT_FOUND' || code === 'ACCESS_DENIED' || code === 'INSUFFICIENT_RIGHTS') {
      return json({
        ok: false,
        error: `Bitrix: ${code} — ${desc}. Метод недоступен в этом Битриксе. Останавливаемся, чтобы не пометить все задачи как failed.`,
        globalBitrixError: code, offset,
      });
    }
  }

  const updates = {};
  let totalComments = 0, processed = 0, errors = 0, totalAttachments = 0;
  const failedTaskIds = [];

  for (const t of tasks) {
    const taskId = `task_${t.id}`;
    const errKey = `c${t.id}`;
    if (batchErrors[errKey]) {
      errors++;
      failedTaskIds.push(t.id);
      const err = batchErrors[errKey];
      updates[`tasks/${taskId}/commentsMigrated`] = false;
      updates[`tasks/${taskId}/commentsMigrationFailed`] = true;
      updates[`tasks/${taskId}/commentsMigrationError`] =
        String(typeof err === 'object' ? (err.error_description || err.error || JSON.stringify(err)) : err).slice(0, 500);
      continue;
    }
    const comments = batchResults[errKey] || [];

    const commentsObj = {};
    for (const c of comments) {
      const cId = c.ID || c.id;
      if (!cId) continue;
      const authorId = c.AUTHOR_ID || c.authorId;
      const authorUid = userMapping[authorId]?.firebaseUid || null;
      const attachedFileIds = [];
      const attached = c.ATTACHED_OBJECTS || c.attachedObjects;
      if (Array.isArray(attached)) {
        for (const ao of attached) {
          const fid = ao.id || ao.ID;
          if (fid) {
            attachedFileIds.push(String(fid));
            totalAttachments++;
          }
        }
      }
      commentsObj[`comment_${cId}`] = {
        bitrixId: String(cId),
        text: c.POST_MESSAGE || c.postMessage || '',
        authorUid,
        bitrixAuthorId: authorId || null,
        authorName: c.AUTHOR_NAME || c.authorName || null,
        bitrixPostDate: c.POST_DATE || c.postDate || null,
        attachedFileIds,
        hasFiles: attachedFileIds.length > 0,
        filesMigrated: attachedFileIds.length === 0,
      };
      totalComments++;
    }

    updates[`tasks/${taskId}/comments`] = commentsObj;
    updates[`tasks/${taskId}/commentsMigrated`] = true;
    updates[`tasks/${taskId}/commentsActualCount`] = comments.length;
    processed++;
  }

  if (Object.keys(updates).length > 0) {
    await firebaseDbMultiUpdate(sa.project_id, accessToken, '/', updates);
  }

  const existing = offset > 0 ? await firebaseDbGet(sa.project_id, accessToken, 'migrationState/taskComments') : null;
  const cumulativeComments = (existing?.totalComments || 0) + totalComments;
  const cumulativeAttachments = (existing?.totalAttachments || 0) + totalAttachments;
  const cumulativeFailed = (existing?.failedCount || 0) + failedTaskIds.length;

  await firebaseDbSet(sa.project_id, accessToken, 'migrationState/taskComments', {
    total: totalTasks, processed: offset + tasks.length, isLast,
    totalComments: cumulativeComments,
    totalAttachments: cumulativeAttachments,
    failedCount: cumulativeFailed,
    lastBatchAt: new Date().toISOString(),
  });

  return json({
    ok: true, total: totalTasks, offset, processed,
    nextOffset: isLast ? null : offset + tasks.length, isLast, errors,
    failedTaskIds: failedTaskIds.slice(0, 5),
    commentsInBatch: totalComments,
    attachmentsInBatch: totalAttachments,
    cumulativeComments, cumulativeAttachments, cumulativeFailed,
    limitUsed: safeLimit,
  });
}

// ═══════════════════════════════════════════════════════════════
//   POST /api/test-file-download
// ═══════════════════════════════════════════════════════════════
async function handleTestFileDownload(request, env) {
  const { idToken, bitrixFileId: providedFileId } = await request.json();
  if (!idToken) return json({ ok: false, error: 'idToken required' }, 400);

  const sa = JSON.parse(env.FIREBASE_SERVICE_ACCOUNT);
  const verified = await verifyFirebaseIdToken(idToken, sa);
  if (!verified) return json({ ok: false, error: 'Invalid token' }, 401);

  const webhook = env.BITRIX_WEBHOOK_URL;
  if (!webhook) return json({ ok: false, error: 'BITRIX_WEBHOOK_URL not set' }, 500);
  if (!env.BUCKET) return json({ ok: false, error: 'R2 BUCKET binding not set' }, 500);

  const webhookMatch = webhook.match(/^(https?:\/\/[^/]+)\/rest\/(\d+)\/([^/]+)\/?$/);
  if (!webhookMatch) {
    return json({ ok: false, error: 'BITRIX_WEBHOOK_URL has unexpected format' });
  }
  const [, portalBase, webhookUserId, webhookKey] = webhookMatch;

  let fileId = providedFileId;
  let foundIn = null;
  if (!fileId) {
    const r = await bitrixCall(webhook, 'crm.activity.list', {
      filter: { '!FILES': '' },
      select: ['ID', 'FILES', 'RECORD_FILE_ID', 'OWNER_ID', 'OWNER_TYPE_ID'],
      start: 0,
    });
    const sample = (r.result || []).slice(0, 5);
    for (const a of sample) {
      if (a.RECORD_FILE_ID && a.RECORD_FILE_ID !== '0') {
        fileId = String(a.RECORD_FILE_ID);
        foundIn = `activity ${a.ID} (RECORD_FILE_ID — запись звонка)`;
        break;
      }
      if (Array.isArray(a.FILES)) {
        for (const f of a.FILES) {
          const fid = (typeof f === 'object') ? (f.id || f.ID || f.fileId) : f;
          if (fid) { fileId = String(fid); foundIn = `activity ${a.ID} (FILES array)`; break; }
        }
        if (fileId) break;
      }
    }
  }

  if (!fileId) {
    return json({ ok: false, error: 'Не нашёл ни одного fileId в первых 50 делах с файлами. Попробуй передать bitrixFileId явно.' });
  }

  const attempts = [];

  try {
    const r = await bitrixCall(webhook, 'disk.attachedObject.get', { id: fileId });
    attempts.push({
      method: 'disk.attachedObject.get',
      ok: !r.error,
      result: r.error ? { error: r.error } : {
        keys: Object.keys(r.result || {}),
        NAME: r.result?.NAME, SIZE: r.result?.SIZE, TYPE: r.result?.TYPE,
        DOWNLOAD_URL: r.result?.DOWNLOAD_URL, OBJECT_ID: r.result?.OBJECT_ID,
      },
    });
  } catch (e) {
    attempts.push({ method: 'disk.attachedObject.get', ok: false, error: e.message });
  }

  try {
    const r = await bitrixCall(webhook, 'disk.file.get', { id: fileId });
    attempts.push({
      method: 'disk.file.get',
      ok: !r.error,
      result: r.error ? { error: r.error } : {
        keys: Object.keys(r.result || {}),
        NAME: r.result?.NAME, SIZE: r.result?.SIZE, TYPE: r.result?.TYPE,
        DOWNLOAD_URL: r.result?.DOWNLOAD_URL,
      },
    });
  } catch (e) {
    attempts.push({ method: 'disk.file.get', ok: false, error: e.message });
  }

  const directUrl1 = `${portalBase}/disk/downloadFile/${fileId}/?auth=${webhookKey}`;
  try {
    const r = await fetch(directUrl1, { method: 'HEAD' });
    attempts.push({
      method: 'GET /disk/downloadFile (HEAD)',
      url: directUrl1.replace(webhookKey, '***'),
      ok: r.ok, status: r.status,
      contentType: r.headers.get('content-type'),
      contentLength: r.headers.get('content-length'),
      contentDisposition: r.headers.get('content-disposition'),
    });
  } catch (e) {
    attempts.push({ method: 'GET /disk/downloadFile', ok: false, error: e.message });
  }

  const goodAttempt = attempts.find(a => a.ok && a.result?.DOWNLOAD_URL);
  let downloadResult = null;
  if (goodAttempt) {
    try {
      const dlUrl = goodAttempt.result.DOWNLOAD_URL;
      const dlRes = await fetch(dlUrl);
      const buf = await dlRes.arrayBuffer();
      downloadResult = {
        from: goodAttempt.method,
        url: typeof dlUrl === 'string' ? dlUrl.replace(webhookKey, '***') : null,
        ok: dlRes.ok, status: dlRes.status,
        sizeBytes: buf.byteLength,
        contentType: dlRes.headers.get('content-type'),
      };

      if (dlRes.ok && buf.byteLength > 0) {
        const r2Key = `test/file_${fileId}`;
        await env.BUCKET.put(r2Key, buf, {
          httpMetadata: { contentType: dlRes.headers.get('content-type') || 'application/octet-stream' },
        });
        downloadResult.r2Key = r2Key;
        downloadResult.r2Status = 'uploaded';
      }
    } catch (e) {
      downloadResult = { from: goodAttempt.method, ok: false, error: e.message };
    }
  }

  return json({
    ok: true, fileId, foundIn, portalBase, webhookUserId,
    attempts, downloadResult,
    summary: downloadResult?.r2Status === 'uploaded'
      ? `✓ Скачивание работает через ${downloadResult.from}, файл загружен в R2 как ${downloadResult.r2Key}`
      : `✗ Не удалось скачать. Смотри attempts.`,
  });
}

// ═══════════════════════════════════════════════════════════════
//   POST /api/build-file-queue
// ═══════════════════════════════════════════════════════════════
async function handleBuildFileQueue(request, env) {
  const { idToken, source, offset = 0, limit = 15 } = await request.json();
  if (!idToken) return json({ ok: false, error: 'idToken required' }, 400);
  if (source !== 'activities' && source !== 'tasks') {
    return json({ ok: false, error: 'source must be "activities" or "tasks"' }, 400);
  }

  const safeLimit = Math.max(1, Math.min(parseInt(limit) || 15, 25));

  const sa = JSON.parse(env.FIREBASE_SERVICE_ACCOUNT);
  const verified = await verifyFirebaseIdToken(idToken, sa);
  if (!verified) return json({ ok: false, error: 'Invalid token' }, 401);

  const accessToken = await getServiceAccountToken(sa, [
    'https://www.googleapis.com/auth/identitytoolkit',
    'https://www.googleapis.com/auth/firebase.database',
    'https://www.googleapis.com/auth/userinfo.email',
  ]);

  const projectId = sa.project_id;
  const dbBase = `https://${projectId}-default-rtdb.firebaseio.com`;
  const headers = { 'Authorization': `Bearer ${accessToken}` };

  const rootPath = source === 'activities' ? 'timeline' : 'tasks';
  const listRes = await fetch(`${dbBase}/${rootPath}.json?shallow=true`, { headers });
  if (!listRes.ok) {
    return json({ ok: false, error: `Firebase shallow list failed: ${listRes.status}` });
  }
  const allKeysObj = await listRes.json();
  const allKeys = allKeysObj ? Object.keys(allKeysObj).sort() : [];
  const totalKeys = allKeys.length;
  const slice = allKeys.slice(offset, offset + safeLimit);
  const isLast = offset + slice.length >= totalKeys;

  if (slice.length === 0) {
    return json({ ok: true, source, total: totalKeys, offset, processed: 0, isLast: true, filesAdded: 0 });
  }

  const queueUpdates = {};
  let filesAdded = 0, recordsScanned = 0, errors = 0;

  for (const ownerKey of slice) {
    try {
      const r = await fetch(`${dbBase}/${rootPath}/${encodeURIComponent(ownerKey)}.json`, { headers });
      if (!r.ok) { errors++; continue; }
      const node = await r.json();
      if (!node) continue;

      if (source === 'activities') {
        for (const [actKey, act] of Object.entries(node)) {
          recordsScanned++;
          const fileIds = Array.isArray(act?.bitrixFileIds) ? act.bitrixFileIds : [];
          if (act?.recordFileId && !fileIds.includes(act.recordFileId)) {
            fileIds.push(act.recordFileId);
          }
          for (const fid of fileIds) {
            if (!fid) continue;
            const sfid = String(fid);
            queueUpdates[`filesQueue/${sfid}`] = {
              bitrixFileId: sfid,
              source: 'activity',
              ownerKey,
              activityKey: actKey,
              activityType: act?.type || null,
              isCallRecord: act?.recordFileId === sfid,
              migrated: false,
              addedAt: new Date().toISOString(),
            };
            filesAdded++;
          }
        }
      } else {
        recordsScanned++;
        const fileIds = Array.isArray(node?.bitrixFileIds) ? node.bitrixFileIds : [];
        for (const fid of fileIds) {
          if (!fid) continue;
          const sfid = String(fid);
          queueUpdates[`filesQueue/${sfid}`] = {
            bitrixFileId: sfid, source: 'task', ownerKey,
            migrated: false, addedAt: new Date().toISOString(),
          };
          filesAdded++;
        }
      }
    } catch (e) { errors++; }
  }

  if (Object.keys(queueUpdates).length > 0) {
    await firebaseDbMultiUpdate(projectId, accessToken, '/', queueUpdates);
  }

  const stateKey = source === 'activities' ? 'fileQueueActivities' : 'fileQueueTasks';
  const existing = offset > 0 ? await firebaseDbGet(projectId, accessToken, `migrationState/${stateKey}`) : null;
  const cumulativeFiles = (existing?.cumulativeFiles || 0) + filesAdded;
  const cumulativeRecords = (existing?.cumulativeRecords || 0) + recordsScanned;

  await firebaseDbSet(projectId, accessToken, `migrationState/${stateKey}`, {
    total: totalKeys, processed: offset + slice.length, isLast,
    cumulativeFiles, cumulativeRecords,
    lastBatchAt: new Date().toISOString(),
  });

  return json({
    ok: true, source, total: totalKeys, offset, processed: slice.length,
    nextOffset: isLast ? null : offset + slice.length, isLast, errors,
    filesAdded, recordsScanned, cumulativeFiles, cumulativeRecords,
  });
}

// ═══════════════════════════════════════════════════════════════
//   POST /api/migrate-files
// ═══════════════════════════════════════════════════════════════
async function handleMigrateFiles(request, env) {
  const { idToken, batchSize = 8 } = await request.json();
  if (!idToken) return json({ ok: false, error: 'idToken required' }, 400);

  const safeBatch = Math.max(1, Math.min(parseInt(batchSize) || 8, 12));

  const sa = JSON.parse(env.FIREBASE_SERVICE_ACCOUNT);
  const verified = await verifyFirebaseIdToken(idToken, sa);
  if (!verified) return json({ ok: false, error: 'Invalid token' }, 401);

  const webhook = env.BITRIX_WEBHOOK_URL;
  if (!webhook) return json({ ok: false, error: 'BITRIX_WEBHOOK_URL not set' }, 500);
  if (!env.BUCKET) return json({ ok: false, error: 'R2 BUCKET binding not set' }, 500);

  const accessToken = await getServiceAccountToken(sa, [
    'https://www.googleapis.com/auth/identitytoolkit',
    'https://www.googleapis.com/auth/firebase.database',
    'https://www.googleapis.com/auth/userinfo.email',
  ]);

  const projectId = sa.project_id;
  const dbBase = `https://${projectId}-default-rtdb.firebaseio.com`;
  const fbHeaders = { 'Authorization': `Bearer ${accessToken}` };

  const queryUrl = `${dbBase}/filesQueue.json?orderBy="migrated"&equalTo=false&limitToFirst=${safeBatch}`;
  let queueData = null;
  try {
    const r = await fetch(queryUrl, { headers: fbHeaders });
    if (r.ok) queueData = await r.json();
  } catch {}

  if (queueData === null) {
    const r = await fetch(`${dbBase}/filesQueue.json?shallow=true`, { headers: fbHeaders });
    if (!r.ok) return json({ ok: false, error: `Firebase queue list failed: ${r.status}` });
    const allKeysObj = await r.json();
    const allKeys = allKeysObj ? Object.keys(allKeysObj) : [];
    queueData = {};
    let found = 0;
    for (const k of allKeys) {
      if (found >= safeBatch) break;
      const fr = await fetch(`${dbBase}/filesQueue/${encodeURIComponent(k)}.json`, { headers: fbHeaders });
      if (!fr.ok) continue;
      const item = await fr.json();
      if (item && !item.migrated) { queueData[k] = item; found++; }
    }
    if (allKeys.length > 100 && Object.keys(queueData).length < safeBatch) {
      return json({
        ok: false,
        error: 'Firebase индекс не настроен на /filesQueue. Добавь правило: { "rules": { "filesQueue": { ".indexOn": "migrated" } } }',
        needsIndex: true,
      });
    }
  }

  const items = Object.entries(queueData || {});
  if (items.length === 0) {
    const stateRes = await firebaseDbGet(projectId, accessToken, 'migrationState/files');
    return json({ ok: true, done: true, message: 'Все файлы из очереди обработаны', cumulative: stateRes || {} });
  }

  const updates = {};
  let migrated = 0, errors = 0, totalBytes = 0;
  const errorDetails = [];

  for (const [fileId, item] of items) {
    try {
      const meta = await bitrixCall(webhook, 'disk.file.get', { id: fileId });
      if (meta.error || !meta.result?.DOWNLOAD_URL) {
        errors++;
        errorDetails.push({ fileId, stage: 'meta', error: meta.error || 'no DOWNLOAD_URL' });
        const newAttempts = (item.attempts || 0) + 1;
        if (newAttempts >= 3) {
          updates[`filesQueue/${fileId}/migrated`] = true;
          updates[`filesQueue/${fileId}/permanentlyFailed`] = true;
        } else {
          updates[`filesQueue/${fileId}/migrated`] = false;
        }
        updates[`filesQueue/${fileId}/lastError`] = String(meta.error || 'no DOWNLOAD_URL').slice(0, 300);
        updates[`filesQueue/${fileId}/lastAttemptAt`] = new Date().toISOString();
        updates[`filesQueue/${fileId}/attempts`] = newAttempts;
        continue;
      }

      const fileName = String(meta.result.NAME || `file_${fileId}`);
      const fileSize = parseInt(meta.result.SIZE) || 0;
      const fileType = String(meta.result.TYPE || 'application/octet-stream');

      const dlRes = await fetch(meta.result.DOWNLOAD_URL);
      if (!dlRes.ok) {
        errors++;
        errorDetails.push({ fileId, stage: 'download', status: dlRes.status });
        const newAttempts = (item.attempts || 0) + 1;
        if (newAttempts >= 3) {
          updates[`filesQueue/${fileId}/migrated`] = true;
          updates[`filesQueue/${fileId}/permanentlyFailed`] = true;
        } else {
          updates[`filesQueue/${fileId}/migrated`] = false;
        }
        updates[`filesQueue/${fileId}/lastError`] = `download HTTP ${dlRes.status}`;
        updates[`filesQueue/${fileId}/lastAttemptAt`] = new Date().toISOString();
        updates[`filesQueue/${fileId}/attempts`] = newAttempts;
        continue;
      }
      const buf = await dlRes.arrayBuffer();
      totalBytes += buf.byteLength;

      const safeName = fileName.replace(/[\\/<>:"'|?*\x00-\x1f]/g, '_').slice(0, 200);
      const r2Key = `files/${fileId}/${safeName}`;
      await env.BUCKET.put(r2Key, buf, {
        httpMetadata: { contentType: dlRes.headers.get('content-type') || fileType },
        customMetadata: {
          bitrixFileId: String(fileId),
          source: item.source || 'unknown',
          ownerKey: item.ownerKey || '',
          activityKey: item.activityKey || '',
          originalName: fileName,
          uploadedAt: new Date().toISOString(),
        },
      });

      updates[`filesQueue/${fileId}/migrated`] = true;
      updates[`filesQueue/${fileId}/permanentlyFailed`] = false;
      updates[`filesQueue/${fileId}/r2Key`] = r2Key;
      updates[`filesQueue/${fileId}/fileName`] = fileName;
      updates[`filesQueue/${fileId}/fileSize`] = fileSize;
      updates[`filesQueue/${fileId}/contentType`] = dlRes.headers.get('content-type') || fileType;
      updates[`filesQueue/${fileId}/migratedAt`] = new Date().toISOString();
      updates[`filesQueue/${fileId}/lastError`] = null;

      if (item.source === 'activity' && item.ownerKey && item.activityKey) {
        updates[`timeline/${item.ownerKey}/${item.activityKey}/filesMigrated`] = true;
      } else if (item.source === 'task' && item.ownerKey) {
        updates[`tasks/${item.ownerKey}/filesMigrated`] = true;
      }

      migrated++;
    } catch (e) {
      errors++;
      errorDetails.push({ fileId, stage: 'exception', error: e.message });
      const newAttempts = (item.attempts || 0) + 1;
      if (newAttempts >= 3) {
        updates[`filesQueue/${fileId}/migrated`] = true;
        updates[`filesQueue/${fileId}/permanentlyFailed`] = true;
      }
      updates[`filesQueue/${fileId}/lastError`] = e.message.slice(0, 300);
      updates[`filesQueue/${fileId}/lastAttemptAt`] = new Date().toISOString();
      updates[`filesQueue/${fileId}/attempts`] = newAttempts;
    }
  }

  await firebaseDbMultiUpdate(projectId, accessToken, '/', updates);

  const existing = await firebaseDbGet(projectId, accessToken, 'migrationState/files');
  const cumulativeMigrated = (existing?.cumulativeMigrated || 0) + migrated;
  const cumulativeBytes = (existing?.cumulativeBytes || 0) + totalBytes;
  const cumulativeErrors = (existing?.cumulativeErrors || 0) + errors;

  await firebaseDbSet(projectId, accessToken, 'migrationState/files', {
    cumulativeMigrated, cumulativeBytes, cumulativeErrors,
    lastBatchAt: new Date().toISOString(),
  });

  return json({
    ok: true, batchSize: safeBatch,
    processedInBatch: items.length,
    migratedInBatch: migrated,
    errorsInBatch: errors, bytesInBatch: totalBytes,
    cumulativeMigrated, cumulativeBytes, cumulativeErrors,
    errorDetails: errorDetails.slice(0, 5),
    done: items.length < safeBatch,
  });
}

// ═══════════════════════════════════════════════════════════════
//   POST /api/diagnose-chats
// ═══════════════════════════════════════════════════════════════
async function handleDiagnoseChats(request, env) {
  const { idToken } = await request.json();
  if (!idToken) return json({ ok: false, error: 'idToken required' }, 400);

  const sa = JSON.parse(env.FIREBASE_SERVICE_ACCOUNT);
  const verified = await verifyFirebaseIdToken(idToken, sa);
  if (!verified) return json({ ok: false, error: 'Invalid token' }, 401);

  const webhook = env.BITRIX_WEBHOOK_URL;
  if (!webhook) return json({ ok: false, error: 'BITRIX_WEBHOOK_URL not set' }, 500);

  const result = {
    openlinesSessions: { error: null, total: null, last6Months: null, sample: [] },
    openlinesMessagesSample: { error: null, perSessionAvg: null, samples: [] },
    imChats: { error: null, total: null, byType: {}, sample: [] },
    imGroupChats: { error: null, total: null, sample: [] },
    estimates: {},
  };

  const sixMonthsAgo = new Date(Date.now() - 180 * 24 * 60 * 60 * 1000).toISOString().slice(0, 10);

  try {
    const allSessions = await bitrixCall(webhook, 'crm.activity.list', {
      filter: { 'PROVIDER_ID': 'IMOPENLINES_SESSION' },
      select: ['ID'], start: 0,
    });
    if (!allSessions.error) result.openlinesSessions.total = allSessions.total ?? 0;
    else result.openlinesSessions.error = allSessions.error;
  } catch (e) { result.openlinesSessions.error = e.message; }

  try {
    const recentSessions = await bitrixCall(webhook, 'crm.activity.list', {
      filter: { 'PROVIDER_ID': 'IMOPENLINES_SESSION', '>=CREATED': sixMonthsAgo },
      select: ['ID', 'SUBJECT', 'CREATED', 'OWNER_ID', 'OWNER_TYPE_ID', 'ASSOCIATED_ENTITY_ID'],
      order: { ID: 'DESC' }, start: 0,
    });
    if (!recentSessions.error) {
      result.openlinesSessions.last6Months = recentSessions.total ?? 0;
      result.openlinesSessions.sample = (recentSessions.result || []).slice(0, 5).map(s => ({
        id: s.ID, subject: s.SUBJECT, created: s.CREATED,
        ownerType: s.OWNER_TYPE_ID, sessionId: s.ASSOCIATED_ENTITY_ID,
      }));
    } else result.openlinesSessions.error = recentSessions.error;
  } catch (e) { result.openlinesSessions.error = e.message; }

  if (result.openlinesSessions.sample.length > 0) {
    let totalMessagesInSamples = 0, successfulSamples = 0;
    for (const s of result.openlinesSessions.sample.slice(0, 3)) {
      if (!s.sessionId) continue;
      try {
        let h = await bitrixCall(webhook, 'imopenlines.session.history.get', { SESSION_ID: s.sessionId });
        if (h.error) {
          h = await bitrixCall(webhook, 'im.dialog.messages.get', {
            DIALOG_ID: 'chat' + s.sessionId, LIMIT: 200,
          });
        }
        if (!h.error) {
          const messages = Array.isArray(h.result?.messages) ? h.result.messages : (Array.isArray(h.result) ? h.result : []);
          totalMessagesInSamples += messages.length;
          successfulSamples++;
          result.openlinesMessagesSample.samples.push({
            sessionId: s.sessionId, messagesCount: messages.length,
            method: h.error ? 'failed' : 'ok',
            firstMessage: messages[0] ? {
              id: messages[0].id || messages[0].ID,
              text: String(messages[0].text || messages[0].MESSAGE || '').slice(0, 100),
              authorId: messages[0].author_id || messages[0].AUTHOR_ID,
              date: messages[0].date || messages[0].DATE_CREATE,
            } : null,
          });
        } else {
          result.openlinesMessagesSample.samples.push({ sessionId: s.sessionId, error: h.error });
        }
      } catch (e) {
        result.openlinesMessagesSample.samples.push({ sessionId: s.sessionId, error: e.message });
      }
    }
    if (successfulSamples > 0) {
      result.openlinesMessagesSample.perSessionAvg = Math.round(totalMessagesInSamples / successfulSamples);
    }
  }

  try {
    const recent = await bitrixCall(webhook, 'im.recent.get', {
      OFFSET: 0, LIMIT: 50, SKIP_OPENLINES: 'Y',
    });
    if (!recent.error) {
      const items = Array.isArray(recent.result?.items) ? recent.result.items : [];
      result.imChats.total = items.length;
      result.imChats.totalRecent = recent.result?.recent_count || items.length;
      const byType = {};
      for (const item of items) { const t = item.type || 'unknown'; byType[t] = (byType[t] || 0) + 1; }
      result.imChats.byType = byType;
      result.imChats.sample = items.slice(0, 5).map(i => ({
        id: i.id, type: i.type, title: i.title,
        chatType: i.chat?.type, memberCount: i.chat?.user_counter,
      }));
    } else result.imChats.error = recent.error;
  } catch (e) { result.imChats.error = e.message; }

  try {
    const groups = await bitrixCall(webhook, 'im.search.chat.list', { FIND: '', LIMIT: 100 });
    if (!groups.error) {
      const items = Array.isArray(groups.result) ? groups.result : (groups.result?.items || []);
      result.imGroupChats.total = items.length;
      result.imGroupChats.sample = items.slice(0, 5).map(c => ({
        id: c.id || c.ID, title: c.title || c.TITLE,
        type: c.type || c.TYPE, userCount: c.user_count || c.USER_COUNT,
      }));
    } else result.imGroupChats.error = groups.error;
  } catch (e) { result.imGroupChats.error = e.message; }

  const sessionsLast6 = result.openlinesSessions.last6Months || 0;
  const avgMsgs = result.openlinesMessagesSample.perSessionAvg || 0;
  result.estimates.openlinesMessagesEstimated = sessionsLast6 * avgMsgs;
  result.estimates.openlinesMigrationMinutes = avgMsgs > 0
    ? Math.ceil((sessionsLast6 / 5) * 2 / 60) : null;

  return json({ ok: true, sixMonthsAgo, ...result });
}

// ═══════════════════════════════════════════════════════════════
//   POST /api/test-chat-history
// ═══════════════════════════════════════════════════════════════
async function handleTestChatHistory(request, env) {
  const { idToken, sessionActivityId } = await request.json();
  if (!idToken) return json({ ok: false, error: 'idToken required' }, 400);

  const sa = JSON.parse(env.FIREBASE_SERVICE_ACCOUNT);
  const verified = await verifyFirebaseIdToken(idToken, sa);
  if (!verified) return json({ ok: false, error: 'Invalid token' }, 401);

  const webhook = env.BITRIX_WEBHOOK_URL;

  let activity;
  if (sessionActivityId) {
    const r = await bitrixCall(webhook, 'crm.activity.get', { id: sessionActivityId });
    activity = r.result;
  } else {
    const r = await bitrixCall(webhook, 'crm.activity.list', {
      filter: { 'PROVIDER_ID': 'IMOPENLINES_SESSION' },
      select: ['*'], order: { ID: 'DESC' }, start: 0,
    });
    activity = (r.result || [])[0];
  }

  if (!activity) return json({ ok: false, error: 'No openline activity found' });

  const sessionAssocId = activity.ASSOCIATED_ENTITY_ID;
  const allKeysOfActivity = Object.keys(activity);
  const providerParams = activity.PROVIDER_PARAMS || {};
  const settings = activity.SETTINGS || {};
  const possibleChatId = providerParams.CHAT_ID || settings.CHAT_ID || providerParams.chatId || null;

  const tests = [];

  try {
    const r = await bitrixCall(webhook, 'imopenlines.session.get', { SESSION_ID: sessionAssocId });
    tests.push({
      method: 'imopenlines.session.get', params: { SESSION_ID: sessionAssocId },
      ok: !r.error, error: r.error,
      resultKeys: r.result ? Object.keys(r.result) : null, sample: r.result,
    });
  } catch (e) { tests.push({ method: 'imopenlines.session.get', ok: false, error: e.message }); }

  try {
    const r = await bitrixCall(webhook, 'imopenlines.session.history.get', { SESSION_ID: sessionAssocId });
    tests.push({
      method: 'imopenlines.session.history.get', params: { SESSION_ID: sessionAssocId },
      ok: !r.error, error: r.error,
      resultType: Array.isArray(r.result) ? 'array' : typeof r.result,
      messagesCount: Array.isArray(r.result) ? r.result.length : (Array.isArray(r.result?.messages) ? r.result.messages.length : null),
      firstMessage: Array.isArray(r.result) ? r.result[0] : (r.result?.messages?.[0] || null),
    });
  } catch (e) { tests.push({ method: 'imopenlines.session.history.get', ok: false, error: e.message }); }

  if (possibleChatId) {
    try {
      const r = await bitrixCall(webhook, 'im.dialog.messages.get', {
        DIALOG_ID: 'chat' + possibleChatId, LIMIT: 10,
      });
      tests.push({
        method: 'im.dialog.messages.get (chat' + possibleChatId + ')',
        ok: !r.error, error: r.error,
        messagesCount: Array.isArray(r.result?.messages) ? r.result.messages.length : null,
        firstMessage: r.result?.messages?.[0] || null,
      });
    } catch (e) { tests.push({ method: 'im.dialog.messages.get', ok: false, error: e.message }); }
  }

  try {
    const r = await bitrixCall(webhook, 'im.message.list.get', {
      CHAT_ID: possibleChatId || sessionAssocId, LIMIT: 10,
    });
    tests.push({
      method: 'im.message.list.get', params: { CHAT_ID: possibleChatId || sessionAssocId },
      ok: !r.error, error: r.error,
      sample: r.result ? (Array.isArray(r.result) ? r.result.slice(0, 1) : r.result) : null,
    });
  } catch (e) { tests.push({ method: 'im.message.list.get', ok: false, error: e.message }); }

  try {
    const r = await bitrixCall(webhook, 'imopenlines.message.get', { SESSION_ID: sessionAssocId });
    tests.push({ method: 'imopenlines.message.get', ok: !r.error, error: r.error, sample: r.result });
  } catch (e) { tests.push({ method: 'imopenlines.message.get', ok: false, error: e.message }); }

  return json({
    ok: true, activityId: activity.ID, activitySubject: activity.SUBJECT,
    sessionAssocId, possibleChatId,
    activityKeys: allKeysOfActivity,
    providerParamsKeys: Object.keys(providerParams),
    settingsKeys: Object.keys(settings),
    tests,
  });
}

// ═══════════════════════════════════════════════════════════════
//   POST /api/migrate-openlines
// ═══════════════════════════════════════════════════════════════
async function handleMigrateOpenlines(request, env) {
  const { idToken, offset = 0, limit = 20 } = await request.json();
  if (!idToken) return json({ ok: false, error: 'idToken required' }, 400);

  const safeLimit = Math.max(1, Math.min(parseInt(limit) || 20, 30));

  const sa = JSON.parse(env.FIREBASE_SERVICE_ACCOUNT);
  const verified = await verifyFirebaseIdToken(idToken, sa);
  if (!verified) return json({ ok: false, error: 'Invalid token' }, 401);

  const webhook = env.BITRIX_WEBHOOK_URL;
  if (!webhook) return json({ ok: false, error: 'BITRIX_WEBHOOK_URL not set' }, 500);

  const accessToken = await getServiceAccountToken(sa, [
    'https://www.googleapis.com/auth/identitytoolkit',
    'https://www.googleapis.com/auth/firebase.database',
    'https://www.googleapis.com/auth/userinfo.email',
  ]);

  const userMappingData = await firebaseDbGet(sa.project_id, accessToken, 'userMapping/bitrix');
  const userMapping = userMappingData || {};

  const sixMonthsAgo = new Date(Date.now() - 180 * 24 * 60 * 60 * 1000).toISOString().slice(0, 10);

  const r = await bitrixCall(webhook, 'crm.activity.list', {
    filter: { 'PROVIDER_ID': 'IMOPENLINES_SESSION', '>=CREATED': sixMonthsAgo },
    select: ['ID', 'SUBJECT', 'CREATED', 'OWNER_ID', 'OWNER_TYPE_ID',
             'ASSOCIATED_ENTITY_ID', 'PROVIDER_TYPE_ID', 'PROVIDER_PARAMS', 'RESPONSIBLE_ID'],
    order: { ID: 'ASC' }, start: offset,
  });
  if (r.error) return json({ ok: false, error: 'Bitrix activity.list: ' + r.error });

  const allActivities = r.result || [];
  const totalActivities = r.total ?? 0;
  const activities = allActivities.slice(0, safeLimit);
  const isLast = offset + activities.length >= totalActivities;

  if (activities.length === 0) {
    return json({ ok: true, total: totalActivities, offset, processed: 0, isLast: true, errors: 0 });
  }

  const commands = {};
  for (const a of activities) {
    if (a.ASSOCIATED_ENTITY_ID) {
      commands[`s${a.ID}`] = {
        method: 'imopenlines.session.history.get',
        params: { SESSION_ID: a.ASSOCIATED_ENTITY_ID },
      };
    }
  }

  const batchRes = await bitrixBatch(webhook, commands);
  if (batchRes.error) {
    return json({
      ok: false, error: 'Bitrix batch: ' + batchRes.error,
      shouldRetryWithSmallerLimit: safeLimit > 1, currentLimit: safeLimit,
    });
  }

  const batchResults = batchRes.result?.result || {};
  const batchErrors = batchRes.result?.result_error || {};

  const PROVIDER_TYPE_NAMES = {
    'WAZZUP': 'wazzup', 'WHATSAPP': 'whatsapp', 'WHATSAPPBYTWILIO': 'whatsapp_twilio',
    'GREENAPI': 'whatsapp_green_api', 'TELEGRAMBOT': 'telegram', 'INSTAGRAM': 'instagram',
    'AVITO': 'avito', 'VKGROUP': 'vk', 'FACEBOOK': 'facebook',
    'NETWORK': 'network', 'LIVECHAT': 'livechat',
  };

  const updates = {};
  let processed = 0, errors = 0, totalMessages = 0, totalAttachments = 0;
  const errorDetails = [];
  const fileIdsCollected = [];

  for (const a of activities) {
    try {
      const errKey = `s${a.ID}`;
      if (batchErrors[errKey] || !commands[errKey]) {
        errors++;
        const err = batchErrors[errKey];
        errorDetails.push({
          activityId: a.ID, sessionId: a.ASSOCIATED_ENTITY_ID,
          error: typeof err === 'object' ? (err.error_description || err.error || JSON.stringify(err)) : String(err || 'no session id'),
        });
        continue;
      }
      const sessionData = batchResults[errKey];
      if (!sessionData || typeof sessionData !== 'object') {
        errors++;
        errorDetails.push({ activityId: a.ID, error: 'empty session data' });
        continue;
      }

      const sessionId = sessionData.sessionId || a.ASSOCIATED_ENTITY_ID;
      const chatId = sessionData.chatId || null;
      const messagesObj = sessionData.message || {};
      const usersObj = sessionData.users || {};
      const orderedMessageIds = (sessionData.usersMessage?.[`chat${chatId}`] || []).slice().reverse();

      const OWNER_PREFIX = { '1': 'lead', '2': 'deal', '3': 'contact', '4': 'company' };
      const ownerType = OWNER_PREFIX[String(a.OWNER_TYPE_ID)] || 'unknown';
      const ownerKey = a.OWNER_ID && a.OWNER_ID !== '0' ? `${ownerType}_${a.OWNER_ID}` : null;

      const providerTypeId = String(a.PROVIDER_TYPE_ID || '').toUpperCase();
      const provider = PROVIDER_TYPE_NAMES[providerTypeId] || providerTypeId.toLowerCase() || 'unknown';

      const parsedMessages = {};
      let messageCount = 0, firstMsgDate = null, lastMsgDate = null;

      const messageIds = orderedMessageIds.length > 0 ? orderedMessageIds : Object.keys(messagesObj);

      for (const mid of messageIds) {
        const m = messagesObj[mid];
        if (!m) continue;

        const senderId = m.senderid;
        const senderUser = usersObj[senderId];
        const isSystem = senderId === '0' || senderId === 0;
        const isFromClient = senderUser?.connector === true || senderUser?.extranet === true;
        const senderName = senderUser
          ? `${senderUser.firstName || senderUser.name || ''} ${senderUser.lastName || ''}`.trim()
          : (isSystem ? 'Система' : 'Неизвестный');
        const senderFirebaseUid = (!isSystem && !isFromClient && senderId)
          ? (userMapping[senderId]?.firebaseUid || null) : null;

        const attachedFileIds = [];
        const params = m.params || {};
        if (Array.isArray(params.FILE_ID)) {
          for (const fid of params.FILE_ID) {
            attachedFileIds.push(String(fid));
            fileIdsCollected.push(String(fid));
          }
        }

        parsedMessages[`msg_${mid}`] = {
          bitrixMessageId: String(mid),
          senderId: String(senderId || '0'),
          senderName, senderFirebaseUid, isSystem, isFromClient,
          text: String(m.text || ''),
          textLegacy: String(m.textlegacy || ''),
          date: m.date || null,
          hasParams: Object.keys(params).length > 0,
          attachedFileIds,
          rawParams: Object.keys(params).length > 0 && Object.keys(params).length < 30 ? params : null,
        };
        messageCount++;
        totalMessages++;
        if (attachedFileIds.length > 0) totalAttachments += attachedFileIds.length;

        if (m.date) {
          if (!firstMsgDate || m.date < firstMsgDate) firstMsgDate = m.date;
          if (!lastMsgDate || m.date > lastMsgDate) lastMsgDate = m.date;
        }
      }

      const usersSnapshot = {};
      for (const [uid, u] of Object.entries(usersObj)) {
        usersSnapshot[uid] = {
          name: `${u.firstName || u.name || ''} ${u.lastName || ''}`.trim(),
          isClient: u.connector === true || u.extranet === true,
          isBot: u.bot === true,
          gender: u.gender || null,
          firebaseUid: userMapping[uid]?.firebaseUid || null,
        };
      }

      const responsibleUid = userMapping[a.RESPONSIBLE_ID]?.firebaseUid || null;

      const sessionDoc = {
        bitrixSessionId: String(sessionId),
        bitrixChatId: chatId ? String(chatId) : null,
        bitrixActivityId: String(a.ID),
        ownerType, ownerKey,
        bitrixOwnerType: a.OWNER_TYPE_ID || null,
        bitrixOwnerId: a.OWNER_ID || null,
        subject: a.SUBJECT || '',
        provider,
        bitrixProviderTypeId: a.PROVIDER_TYPE_ID || null,
        userCode: a.PROVIDER_PARAMS?.USER_CODE || null,
        responsibleUid,
        bitrixResponsibleId: a.RESPONSIBLE_ID || null,
        bitrixCreated: a.CREATED || null,
        firstMessageAt: firstMsgDate, lastMessageAt: lastMsgDate,
        messageCount,
        messages: parsedMessages,
        users: usersSnapshot,
        migratedAt: new Date().toISOString(),
      };

      updates[`openlinesSessions/session_${sessionId}`] = sessionDoc;

      if (ownerKey) {
        updates[`timeline/${ownerKey}/activity_${a.ID}/openlinesSessionId`] = `session_${sessionId}`;
        updates[`timeline/${ownerKey}/activity_${a.ID}/openlinesMessageCount`] = messageCount;
        updates[`timeline/${ownerKey}/activity_${a.ID}/openlinesProvider`] = provider;
      }

      processed++;
    } catch (e) {
      errors++;
      errorDetails.push({ activityId: a.ID, error: e.message });
    }
  }

  if (Object.keys(updates).length > 0) {
    await firebaseDbMultiUpdate(sa.project_id, accessToken, '/', updates);
  }

  const existing = offset > 0 ? await firebaseDbGet(sa.project_id, accessToken, 'migrationState/openlines') : null;
  const cumulativeMessages = (existing?.cumulativeMessages || 0) + totalMessages;
  const cumulativeAttachments = (existing?.cumulativeAttachments || 0) + totalAttachments;

  await firebaseDbSet(sa.project_id, accessToken, 'migrationState/openlines', {
    total: totalActivities,
    processed: offset + activities.length,
    isLast,
    cumulativeMessages, cumulativeAttachments,
    sixMonthsAgoUsed: sixMonthsAgo,
    lastBatchAt: new Date().toISOString(),
  });

  return json({
    ok: true, total: totalActivities, offset, processed,
    nextOffset: isLast ? null : offset + activities.length, isLast, errors,
    errorDetails: errorDetails.slice(0, 5),
    messagesInBatch: totalMessages,
    attachmentsInBatch: totalAttachments,
    cumulativeMessages, cumulativeAttachments,
    limitUsed: safeLimit, sixMonthsAgoUsed: sixMonthsAgo,
  });
}

// ═══════════════════════════════════════════════════════════════
//   POST /api/diagnose-group-chats
// ═══════════════════════════════════════════════════════════════
async function handleDiagnoseGroupChats(request, env) {
  const { idToken } = await request.json();
  if (!idToken) return json({ ok: false, error: 'idToken required' }, 400);

  const sa = JSON.parse(env.FIREBASE_SERVICE_ACCOUNT);
  const verified = await verifyFirebaseIdToken(idToken, sa);
  if (!verified) return json({ ok: false, error: 'Invalid token' }, 401);

  const webhook = env.BITRIX_WEBHOOK_URL;
  if (!webhook) return json({ ok: false, error: 'BITRIX_WEBHOOK_URL not set' }, 500);

  const tests = [];

  try {
    const r = await bitrixCall(webhook, 'im.recent.get', { OFFSET: 0, LIMIT: 50 });
    const items = Array.isArray(r.result?.items) ? r.result.items
                  : Array.isArray(r.result) ? r.result : [];
    tests.push({
      method: 'im.recent.get', ok: !r.error, error: r.error,
      itemsCount: items.length,
      types: [...new Set(items.map(i => i.type || 'unknown'))],
      sample: items.slice(0, 3).map(i => ({
        id: i.id, type: i.type, title: i.title,
        chatType: i.chat?.type, members: i.chat?.user_counter,
      })),
    });
  } catch (e) { tests.push({ method: 'im.recent.get', ok: false, error: e.message }); }

  try {
    const r = await bitrixCall(webhook, 'im.recent.list', {});
    const items = Array.isArray(r.result?.items) ? r.result.items
                  : Array.isArray(r.result) ? r.result : [];
    tests.push({
      method: 'im.recent.list', ok: !r.error, error: r.error,
      itemsCount: items.length,
      sample: items.slice(0, 3).map(i => ({ id: i.id, type: i.type, title: i.title })),
    });
  } catch (e) { tests.push({ method: 'im.recent.list', ok: false, error: e.message }); }

  try {
    const r = await bitrixCall(webhook, 'im.search.chat.list', { FIND: 'a' });
    const items = Array.isArray(r.result) ? r.result : (r.result?.items || []);
    tests.push({
      method: 'im.search.chat.list (FIND="a")', ok: !r.error, error: r.error,
      itemsCount: items.length, sample: items.slice(0, 3),
    });
  } catch (e) { tests.push({ method: 'im.search.chat.list', ok: false, error: e.message }); }

  try {
    const r = await bitrixCall(webhook, 'im.chat.search', { FIND: 'a' });
    tests.push({ method: 'im.chat.search (FIND="a")', ok: !r.error, error: r.error, result: r.result });
  } catch (e) { tests.push({ method: 'im.chat.search', ok: false, error: e.message }); }

  try {
    const r = await bitrixCall(webhook, 'sonet_group.get', { ORDER: { ID: 'ASC' } });
    const items = Array.isArray(r.result) ? r.result : [];
    tests.push({
      method: 'sonet_group.get', ok: !r.error, error: r.error,
      itemsCount: items.length,
      sample: items.slice(0, 3).map(g => ({
        id: g.ID, name: g.NAME, description: (g.DESCRIPTION || '').slice(0, 100),
      })),
    });
  } catch (e) { tests.push({ method: 'sonet_group.get', ok: false, error: e.message }); }

  try {
    const r = await bitrixCall(webhook, 'department.get', {});
    tests.push({ method: 'department.get', ok: !r.error, error: r.error, total: r.total });
  } catch (e) { tests.push({ method: 'department.get', ok: false, error: e.message }); }

  try {
    const r = await bitrixCall(webhook, 'im.chat.get', { CHAT_ID: 161234 });
    tests.push({
      method: 'im.chat.get (CHAT_ID=161234)', ok: !r.error, error: r.error,
      keys: r.result ? Object.keys(r.result).slice(0, 20) : null,
      title: r.result?.title || r.result?.name, type: r.result?.type,
    });
  } catch (e) { tests.push({ method: 'im.chat.get', ok: false, error: e.message }); }

  try {
    const r = await bitrixCall(webhook, 'user.current', {});
    tests.push({
      method: 'user.current', ok: !r.error, error: r.error,
      user: r.result ? { id: r.result.ID, name: r.result.NAME, lastName: r.result.LAST_NAME, email: r.result.EMAIL } : null,
    });
  } catch (e) { tests.push({ method: 'user.current', ok: false, error: e.message }); }

  return json({
    ok: true,
    note: 'Webhook работает от имени конкретного пользователя. Чаты которые видны зависят от того, в каких чатах он состоит.',
    tests,
  });
}

// ═══════════════════════════════════════════════════════════════
//   POST /api/migrate-group-chats
// ═══════════════════════════════════════════════════════════════
async function handleMigrateGroupChats(request, env) {
  const { idToken, mode = 'list', chatId, lastId } = await request.json();
  if (!idToken) return json({ ok: false, error: 'idToken required' }, 400);

  const sa = JSON.parse(env.FIREBASE_SERVICE_ACCOUNT);
  const verified = await verifyFirebaseIdToken(idToken, sa);
  if (!verified) return json({ ok: false, error: 'Invalid token' }, 401);

  const webhook = env.BITRIX_WEBHOOK_URL;
  if (!webhook) return json({ ok: false, error: 'BITRIX_WEBHOOK_URL not set' }, 500);

  const accessToken = await getServiceAccountToken(sa, [
    'https://www.googleapis.com/auth/identitytoolkit',
    'https://www.googleapis.com/auth/firebase.database',
    'https://www.googleapis.com/auth/userinfo.email',
  ]);

  const userMappingData = await firebaseDbGet(sa.project_id, accessToken, 'userMapping/bitrix');
  const userMapping = userMappingData || {};

  if (mode === 'list') {
    const r = await bitrixCall(webhook, 'im.recent.list', {});
    const items = Array.isArray(r.result?.items) ? r.result.items
                  : Array.isArray(r.result) ? r.result : [];

    const groupChats = [];
    const updates = {};
    for (const item of items) {
      if (item.type !== 'chat') continue;
      const chatType = item.chat?.type || item.chatType;
      if (chatType !== 'chat') continue;

      const chatId = String(item.id).replace(/^chat/, '');
      const responsibleUid = item.chat?.owner ? userMapping[item.chat.owner]?.firebaseUid || null : null;

      const chatDoc = {
        bitrixChatId: chatId,
        title: item.title || item.chat?.name || '(без названия)',
        ownerBitrixId: item.chat?.owner ? String(item.chat.owner) : null,
        ownerFirebaseUid: responsibleUid,
        memberCount: item.chat?.user_counter || null,
        avatar: item.chat?.avatar || null,
        color: item.chat?.color || null,
        bitrixDateCreate: item.chat?.date_create || null,
        bitrixDateMessage: item.message?.date || null,
        lastMessageText: String(item.message?.text || '').slice(0, 200),
        migrationStatus: 'pending', messageCount: 0,
        addedAt: new Date().toISOString(),
      };

      updates[`groupChats/${chatId}`] = chatDoc;
      groupChats.push({ chatId, title: chatDoc.title, members: chatDoc.memberCount });
    }

    if (Object.keys(updates).length > 0) {
      await firebaseDbMultiUpdate(sa.project_id, accessToken, '/', updates);
    }

    await firebaseDbSet(sa.project_id, accessToken, 'migrationState/groupChats', {
      total: groupChats.length, processedChats: 0, isLast: false,
      listBuiltAt: new Date().toISOString(),
    });

    return json({
      ok: true, mode: 'list',
      totalRecentItems: items.length,
      groupChatsFound: groupChats.length,
      chats: groupChats,
    });
  }

  if (mode === 'fetch') {
    if (!chatId) return json({ ok: false, error: 'chatId required for mode=fetch' }, 400);

    const params = { DIALOG_ID: 'chat' + chatId, LIMIT: 100 };
    if (lastId) params.LAST_ID = lastId;

    const r = await bitrixCall(webhook, 'im.dialog.messages.get', params);
    if (r.error) return json({ ok: false, error: 'Bitrix: ' + r.error });

    const result = r.result || {};
    const messages = Array.isArray(result.messages) ? result.messages : [];
    const usersFromMsg = result.users || {};

    if (messages.length === 0) {
      await firebaseDbSet(sa.project_id, accessToken,
        `groupChats/${chatId}/migrationStatus`, 'done');
      return json({ ok: true, mode: 'fetch', chatId, done: true, messagesInBatch: 0 });
    }

    const updates = {};
    let totalAttachments = 0, oldestId = null;

    for (const m of messages) {
      const mid = m.id || m.ID;
      if (!mid) continue;
      if (!oldestId || mid < oldestId) oldestId = mid;

      const senderId = m.author_id || m.AUTHOR_ID || m.senderId || '0';
      const senderUser = usersFromMsg[senderId] || {};
      const senderName = senderUser.firstName || senderUser.name
        ? `${senderUser.firstName || senderUser.name || ''} ${senderUser.lastName || ''}`.trim()
        : 'Неизвестный';
      const senderFirebaseUid = userMapping[senderId]?.firebaseUid || null;

      const attachedFileIds = [];
      const params = m.params || {};
      if (Array.isArray(params.FILE_ID)) {
        for (const fid of params.FILE_ID) attachedFileIds.push(String(fid));
      }
      totalAttachments += attachedFileIds.length;

      updates[`groupChats/${chatId}/messages/msg_${mid}`] = {
        bitrixMessageId: String(mid), senderId: String(senderId), senderName,
        senderFirebaseUid, text: String(m.text || ''),
        date: m.date || null,
        attachedFileIds,
        rawParams: Object.keys(params).length > 0 && Object.keys(params).length < 30 ? params : null,
      };
    }

    const chatMeta = await firebaseDbGet(sa.project_id, accessToken, `groupChats/${chatId}`);
    const currentCount = chatMeta?.messageCount || 0;
    updates[`groupChats/${chatId}/messageCount`] = currentCount + messages.length;
    updates[`groupChats/${chatId}/migrationStatus`] = 'in_progress';
    updates[`groupChats/${chatId}/lastMigratedAt`] = new Date().toISOString();

    await firebaseDbMultiUpdate(sa.project_id, accessToken, '/', updates);

    const hasMore = messages.length === 100;

    return json({
      ok: true, mode: 'fetch', chatId,
      messagesInBatch: messages.length,
      attachmentsInBatch: totalAttachments,
      nextLastId: hasMore ? oldestId : null,
      done: !hasMore,
      totalSoFar: currentCount + messages.length,
    });
  }

  return json({ ok: false, error: `Unknown mode: ${mode}. Use 'list' or 'fetch'.` });
}

// ═══════════════════════════════════════════════════════════════
//   POST /api/diagnose-pipeline
// ═══════════════════════════════════════════════════════════════
async function handleDiagnosePipeline(request, env) {
  const { idToken, categoryId } = await request.json();
  if (!idToken) return json({ ok: false, error: 'idToken required' }, 400);
  const catId = parseInt(categoryId ?? 3);

  const sa = JSON.parse(env.FIREBASE_SERVICE_ACCOUNT);
  const verified = await verifyFirebaseIdToken(idToken, sa);
  if (!verified) return json({ ok: false, error: 'Invalid token' }, 401);

  const webhook = env.BITRIX_WEBHOOK_URL;
  if (!webhook) return json({ ok: false, error: 'BITRIX_WEBHOOK_URL not set' }, 500);

  const cats = await bitrixCall(webhook, 'crm.dealcategory.list', {});
  const category = (cats.result || []).find(c => parseInt(c.ID) === catId);
  const stages = await bitrixCall(webhook, 'crm.dealcategory.stage.list', { id: catId });

  const dealsCount = await bitrixCall(webhook, 'crm.deal.list', {
    filter: { CATEGORY_ID: catId }, select: ['ID'], start: 0,
  });
  const totalDeals = dealsCount.total ?? 0;

  const sample = await bitrixCall(webhook, 'crm.deal.list', {
    filter: { CATEGORY_ID: catId },
    select: ['ID', 'STAGE_ID', 'CONTACT_ID', 'COMPANY_ID', 'OPPORTUNITY', 'DATE_CREATE', 'CLOSED'],
    start: 0,
  });

  const byStage = {};
  const contactIds = new Set();
  const companyIds = new Set();
  let closedCount = 0, withContact = 0, withCompany = 0;

  for (const d of (sample.result || [])) {
    byStage[d.STAGE_ID] = (byStage[d.STAGE_ID] || 0) + 1;
    if (d.CONTACT_ID) { contactIds.add(d.CONTACT_ID); withContact++; }
    if (d.COMPANY_ID) { companyIds.add(d.COMPANY_ID); withCompany++; }
    if (d.CLOSED === 'Y') closedCount++;
  }

  const userFields = await bitrixCall(webhook, 'crm.deal.userfield.list', {});
  const ufList = userFields.result || [];

  const ufSample = await bitrixCall(webhook, 'crm.deal.list', {
    filter: { CATEGORY_ID: catId },
    select: ['ID', ...ufList.map(f => f.FIELD_NAME)],
    start: 0,
  });
  const sampleSize = (ufSample.result || []).length;
  const ufStats = ufList.map(f => {
    let filled = 0;
    for (const d of (ufSample.result || [])) {
      const v = d[f.FIELD_NAME];
      if (v !== null && v !== undefined && v !== '' && !(Array.isArray(v) && v.length === 0)) filled++;
    }
    return {
      fieldName: f.FIELD_NAME,
      label: extractLabel(f.EDIT_FORM_LABEL) || extractLabel(f.LIST_COLUMN_LABEL) || f.FIELD_NAME,
      type: f.USER_TYPE_ID,
      multiple: f.MULTIPLE === 'Y',
      filled, sampleSize,
      pct: sampleSize > 0 ? Math.round(filled / sampleSize * 100) : 0,
    };
  });

  const activitiesSample = await bitrixCall(webhook, 'crm.activity.list', {
    filter: { OWNER_TYPE_ID: 2 }, order: { ID: 'DESC' }, start: 0,
  });

  const allActivitiesCount = await bitrixCall(webhook, 'crm.activity.list', {
    filter: { OWNER_TYPE_ID: 2 }, select: ['ID'], start: 0,
  });
  const allActivitiesTotal = allActivitiesCount.total ?? 0;

  const fileFields = ufList.filter(f => f.USER_TYPE_ID === 'file').map(f => f.FIELD_NAME);

  return json({
    ok: true,
    pipeline: {
      categoryId: catId,
      name: category?.NAME || 'Общая воронка',
      stagesCount: (stages.result || []).length,
      stages: (stages.result || []).map(s => ({
        statusId: s.STATUS_ID, name: s.NAME, sort: s.SORT, count: byStage[s.STATUS_ID] || 0,
      })),
    },
    deals: {
      total: totalDeals, sampleSize, closed: closedCount,
      withContact, withCompany,
      sampledByStageDistribution: byStage,
    },
    contacts: {
      uniqueInSample: contactIds.size,
      withContactPct: sampleSize > 0 ? Math.round(withContact / sampleSize * 100) : 0,
      estimatedTotal: sampleSize > 0 ? Math.round(contactIds.size * totalDeals / sampleSize) : 0,
    },
    companies: {
      uniqueInSample: companyIds.size,
      withCompanyPct: sampleSize > 0 ? Math.round(withCompany / sampleSize * 100) : 0,
    },
    customFields: {
      total: ufList.length, fileFields: fileFields.length,
      fields: ufStats.sort((a, b) => b.pct - a.pct),
    },
    activities: {
      totalAllPipelinesDeals: allActivitiesTotal,
      estimatedForThisPipeline: allActivitiesTotal > 0 && totalDeals > 0
        ? Math.round(allActivitiesTotal * (totalDeals / 111098)) : 0,
      sampleRecent: (activitiesSample.result || []).slice(0, 5).map(a => ({
        id: a.ID, type: a.TYPE_ID,
        subject: (a.SUBJECT || '').slice(0, 80),
        ownerType: a.OWNER_TYPE_ID, ownerId: a.OWNER_ID, date: a.CREATED,
      })),
    },
  });
}

// ═══════════════════════════════════════════════════════════════
//   POST /api/migrate-users
// ═══════════════════════════════════════════════════════════════
async function handleMigrateUsers(request, env, dryRun = false) {
  const url = new URL(request.url);
  const batchMode = url.searchParams.get('batch') === '1';
  const offset = parseInt(url.searchParams.get('offset') || '0');
  const limit = parseInt(url.searchParams.get('limit') || '8');

  const { idToken } = await request.json();
  if (!idToken) return json({ ok: false, error: 'idToken required' }, 400);

  const sa = JSON.parse(env.FIREBASE_SERVICE_ACCOUNT);
  const verified = await verifyFirebaseIdToken(idToken, sa);
  if (!verified) return json({ ok: false, error: 'Invalid token' }, 401);

  const webhook = env.BITRIX_WEBHOOK_URL;
  if (!webhook) return json({ ok: false, error: 'BITRIX_WEBHOOK_URL not set' }, 500);

  const accessToken = await getServiceAccountToken(sa, [
    'https://www.googleapis.com/auth/identitytoolkit',
    'https://www.googleapis.com/auth/firebase.database',
    'https://www.googleapis.com/auth/userinfo.email',
  ]);

  if (dryRun) {
    const allUsers = await loadAllBitrixUsers(webhook);
    if (allUsers.error) return json({ ok: false, error: allUsers.error });

    const active = allUsers.filter(u => u.ACTIVE).length;
    const inactive = allUsers.length - active;
    const withEmail = allUsers.filter(u => u.EMAIL).length;
    const withoutEmail = allUsers.length - withEmail;

    await firebaseDbSet(sa.project_id, accessToken, 'migrationCache/users', {
      cachedAt: new Date().toISOString(),
      total: allUsers.length,
      users: allUsers.map(u => ({
        ID: u.ID, NAME: u.NAME || '', LAST_NAME: u.LAST_NAME || '',
        EMAIL: u.EMAIL || '', WORK_POSITION: u.WORK_POSITION || '',
        UF_DEPARTMENT: u.UF_DEPARTMENT || [],
        ACTIVE: !!u.ACTIVE,
        PERSONAL_PHOTO: u.PERSONAL_PHOTO || '',
        TIME_ZONE_OFFSET: u.TIME_ZONE_OFFSET || null,
      })),
    });

    return json({
      ok: true, total: allUsers.length, active, inactive, withEmail, withoutEmail,
      preview: allUsers.map(u => ({
        id: u.ID, name: u.NAME || '', lastName: u.LAST_NAME || '',
        email: u.EMAIL || '', position: u.WORK_POSITION || '',
        active: !!u.ACTIVE,
      })),
    });
  }

  let allUsers;
  let needsCache = offset === 0;

  if (!needsCache) {
    const cached = await firebaseDbGet(sa.project_id, accessToken, 'migrationCache/users');
    if (cached && Array.isArray(cached.users) && cached.users.length > 0) {
      const cachedAt = new Date(cached.cachedAt).getTime();
      if (Date.now() - cachedAt < 60 * 60 * 1000) allUsers = cached.users;
      else needsCache = true;
    } else needsCache = true;
  }

  if (needsCache) {
    const fresh = await loadAllBitrixUsers(webhook);
    if (fresh.error) return json({ ok: false, error: fresh.error });
    allUsers = fresh.map(u => ({
      ID: u.ID, NAME: u.NAME || '', LAST_NAME: u.LAST_NAME || '',
      EMAIL: u.EMAIL || '', WORK_POSITION: u.WORK_POSITION || '',
      UF_DEPARTMENT: u.UF_DEPARTMENT || [],
      ACTIVE: !!u.ACTIVE,
      PERSONAL_PHOTO: u.PERSONAL_PHOTO || '',
      TIME_ZONE_OFFSET: u.TIME_ZONE_OFFSET || null,
    }));
    await firebaseDbSet(sa.project_id, accessToken, 'migrationCache/users', {
      cachedAt: new Date().toISOString(), total: allUsers.length, users: allUsers,
    });
  }

  allUsers.sort((a, b) => parseInt(a.ID) - parseInt(b.ID));
  const batch = allUsers.slice(offset, offset + limit);
  const isLast = offset + batch.length >= allUsers.length;

  let created = 0, alreadyExisted = 0, skipped = 0, errors = 0;
  const errorDetails = [];
  const processedNames = [];

  for (const u of batch) {
    const email = (u.EMAIL || '').trim().toLowerCase();

    if (!email) {
      skipped++;
      processedNames.push({ id: u.ID, name: `${u.LAST_NAME||''} ${u.NAME||''}`.trim(), status: 'skipped (no email)' });
      continue;
    }

    try {
      const existing = await firebaseGetUserByEmail(sa.project_id, accessToken, email);
      let firebaseUid;

      if (existing) { firebaseUid = existing.localId; alreadyExisted++; }
      else {
        const newUser = await firebaseCreateUser(sa.project_id, accessToken, {
          email, password: generateTempPassword(),
          displayName: [u.LAST_NAME, u.NAME].filter(Boolean).join(' ') || u.NAME || email,
          disabled: !u.ACTIVE,
        });
        firebaseUid = newUser.localId;
        created++;
      }

      const profile = {
        bitrixId: String(u.ID), email,
        name: u.NAME || '', lastName: u.LAST_NAME || '',
        position: u.WORK_POSITION || '',
        department: Array.isArray(u.UF_DEPARTMENT) ? u.UF_DEPARTMENT : [],
        active: !!u.ACTIVE,
        photo: u.PERSONAL_PHOTO || '',
        timezone: u.TIME_ZONE_OFFSET || null,
        createdFromBitrix: true,
        migratedAt: new Date().toISOString(),
      };

      await firebaseDbMultiUpdate(sa.project_id, accessToken, '/', {
        [`users/${firebaseUid}`]: profile,
        [`userMapping/bitrix/${u.ID}`]: {
          firebaseUid, email, name: profile.name, lastName: profile.lastName,
        },
        [`userMapping/firebase/${firebaseUid}`]: {
          bitrixId: String(u.ID), email,
        },
      });

      processedNames.push({
        id: u.ID, name: `${u.LAST_NAME||''} ${u.NAME||''}`.trim(),
        email, status: existing ? 'updated' : 'created',
      });
    } catch (e) {
      errors++;
      errorDetails.push({ email, bitrixId: u.ID, error: e.message });
      processedNames.push({
        id: u.ID, name: `${u.LAST_NAME||''} ${u.NAME||''}`.trim(),
        email, status: 'error: ' + e.message,
      });
    }
  }

  await firebaseDbSet(sa.project_id, accessToken, 'migrationState/users', {
    total: allUsers.length, processed: offset + batch.length, isLast,
    lastBatchAt: new Date().toISOString(),
  });

  return json({
    ok: true, batch: true, total: allUsers.length, offset,
    processed: batch.length,
    nextOffset: isLast ? null : offset + batch.length, isLast,
    created, alreadyExisted, skipped, errors,
    errorDetails: errorDetails.slice(0, 10),
    processedNames,
  });
}

// ═══════════════════════════════════════════════════════════════
//   POST /api/migrate-sources
// ═══════════════════════════════════════════════════════════════
async function handleMigrateSources(request, env) {
  const { idToken } = await request.json();
  if (!idToken) return json({ ok: false, error: 'idToken required' }, 400);

  const sa = JSON.parse(env.FIREBASE_SERVICE_ACCOUNT);
  const verified = await verifyFirebaseIdToken(idToken, sa);
  if (!verified) return json({ ok: false, error: 'Invalid token' }, 401);

  const webhook = env.BITRIX_WEBHOOK_URL;
  if (!webhook) return json({ ok: false, error: 'BITRIX_WEBHOOK_URL not set' }, 500);

  const accessToken = await getServiceAccountToken(sa, [
    'https://www.googleapis.com/auth/identitytoolkit',
    'https://www.googleapis.com/auth/firebase.database',
    'https://www.googleapis.com/auth/userinfo.email',
  ]);

  const ENTITY_TYPES = [
    { entity: 'SOURCE',       label: 'Источники' },
    { entity: 'CONTACT_TYPE', label: 'Типы контактов' },
    { entity: 'COMPANY_TYPE', label: 'Типы компаний' },
    { entity: 'INDUSTRY',     label: 'Отрасли' },
    { entity: 'EMPLOYEES',    label: 'Размер компании' },
    { entity: 'DEAL_TYPE',    label: 'Типы сделок' },
  ];

  const commands = {};
  for (const t of ENTITY_TYPES) {
    commands[t.entity] = {
      method: 'crm.status.list',
      params: { filter: { ENTITY_ID: t.entity }, order: { SORT: 'ASC' } },
    };
  }

  const batchRes = await bitrixBatch(webhook, commands);
  if (batchRes.error) return json({ ok: false, error: 'Bitrix batch: ' + batchRes.error });

  const results = batchRes.result?.result || {};
  const errors = batchRes.result?.result_error || {};

  const updates = {};
  const summary = {};

  for (const t of ENTITY_TYPES) {
    if (errors[t.entity]) {
      summary[t.entity] = { error: String(errors[t.entity]?.error_description || errors[t.entity]) };
      continue;
    }
    const items = results[t.entity] || [];
    const map = {};
    for (const item of items) {
      map[item.STATUS_ID] = {
        statusId: item.STATUS_ID,
        name: extractLabel(item.NAME) || item.STATUS_ID,
        sort: parseInt(item.SORT) || 0,
        color: item.COLOR || null,
        system: item.SYSTEM === 'Y',
      };
    }
    updates[`referenceLists/${t.entity}`] = map;
    summary[t.entity] = { count: items.length, label: t.label };
  }

  await firebaseDbMultiUpdate(sa.project_id, accessToken, '/', updates);
  await firebaseDbSet(sa.project_id, accessToken, 'migrationState/sources', {
    migratedAt: new Date().toISOString(), summary,
  });

  return json({ ok: true, summary, totalEntities: ENTITY_TYPES.length });
}

// ═══════════════════════════════════════════════════════════════
//   POST /api/migrate-departments
// ═══════════════════════════════════════════════════════════════
async function handleMigrateDepartments(request, env) {
  const { idToken } = await request.json();
  if (!idToken) return json({ ok: false, error: 'idToken required' }, 400);

  const sa = JSON.parse(env.FIREBASE_SERVICE_ACCOUNT);
  const verified = await verifyFirebaseIdToken(idToken, sa);
  if (!verified) return json({ ok: false, error: 'Invalid token' }, 401);

  const webhook = env.BITRIX_WEBHOOK_URL;
  if (!webhook) return json({ ok: false, error: 'BITRIX_WEBHOOK_URL not set' }, 500);

  const accessToken = await getServiceAccountToken(sa, [
    'https://www.googleapis.com/auth/identitytoolkit',
    'https://www.googleapis.com/auth/firebase.database',
    'https://www.googleapis.com/auth/userinfo.email',
  ]);

  const all = [];
  let start = 0;
  let safety = 20;
  while (safety-- > 0) {
    const r = await bitrixCall(webhook, 'department.get', { start });
    if (r.error) return json({ ok: false, error: 'Bitrix: ' + r.error });
    if (!Array.isArray(r.result) || !r.result.length) break;
    all.push(...r.result);
    if (r.next === undefined) break;
    start = r.next;
  }

  const userMappingData = await firebaseDbGet(sa.project_id, accessToken, 'userMapping/bitrix');
  const userMapping = userMappingData || {};

  const updates = {};
  const tree = {};

  for (const d of all) {
    const id = String(d.ID);
    const headBitrixId = d.UF_HEAD ? String(d.UF_HEAD) : null;
    const headUid = headBitrixId ? userMapping[headBitrixId]?.firebaseUid || null : null;
    const parentId = d.PARENT ? String(d.PARENT) : null;

    updates[`departments/${id}`] = {
      bitrixId: id,
      name: extractLabel(d.NAME) || `Отдел ${id}`,
      parentId, sort: parseInt(d.SORT) || 0,
      headBitrixId, headUid,
    };

    if (parentId) {
      if (!tree[parentId]) tree[parentId] = [];
      tree[parentId].push(id);
    }
  }

  const usersData = await firebaseDbGet(sa.project_id, accessToken, 'users');
  const users = usersData || {};
  const empCount = {};
  for (const u of Object.values(users)) {
    if (Array.isArray(u.department)) {
      for (const dep of u.department) {
        const dStr = String(dep);
        empCount[dStr] = (empCount[dStr] || 0) + 1;
      }
    }
  }
  for (const [depId, count] of Object.entries(empCount)) {
    if (updates[`departments/${depId}`]) {
      updates[`departments/${depId}`].employeesCount = count;
    }
  }

  await firebaseDbMultiUpdate(sa.project_id, accessToken, '/', updates);

  await firebaseDbSet(sa.project_id, accessToken, 'migrationState/departments', {
    total: all.length, migratedAt: new Date().toISOString(),
  });

  const sorted = all.map(d => ({
    id: d.ID, name: extractLabel(d.NAME) || `Отдел ${d.ID}`,
    parent: d.PARENT, head: d.UF_HEAD,
    employees: empCount[String(d.ID)] || 0,
  })).sort((a, b) => parseInt(a.id) - parseInt(b.id));

  return json({ ok: true, total: all.length, departments: sorted });
}

// ═══════════════════════════════════════════════════════════════
//   POST /api/diagnose-bizproc
// ═══════════════════════════════════════════════════════════════
async function handleDiagnoseBizproc(request, env) {
  const { idToken } = await request.json();
  if (!idToken) return json({ ok: false, error: 'idToken required' }, 400);

  const sa = JSON.parse(env.FIREBASE_SERVICE_ACCOUNT);
  const verified = await verifyFirebaseIdToken(idToken, sa);
  if (!verified) return json({ ok: false, error: 'Invalid token' }, 401);

  const webhook = env.BITRIX_WEBHOOK_URL;
  if (!webhook) return json({ ok: false, error: 'BITRIX_WEBHOOK_URL not set' }, 500);

  const accessToken = await getServiceAccountToken(sa, [
    'https://www.googleapis.com/auth/identitytoolkit',
    'https://www.googleapis.com/auth/firebase.database',
    'https://www.googleapis.com/auth/userinfo.email',
  ]);

  const result = {
    workflows: { total: 0, error: null, templates: [] },
    automation: { error: null, byEntity: {} },
    triggers: { error: null, byEntity: {} },
    note: null,
  };

  try {
    const r = await bitrixCall(webhook, 'bizproc.workflow.template.list', {});
    if (r.error) result.workflows.error = r.error;
    else {
      const items = Array.isArray(r.result) ? r.result : [];
      result.workflows.total = items.length;
      result.workflows.templates = items.map(t => ({
        id: t.ID,
        name: extractLabel(t.NAME) || `Шаблон ${t.ID}`,
        description: extractLabel(t.DESCRIPTION) || '',
        moduleId: t.MODULE_ID, entity: t.ENTITY,
        documentType: t.DOCUMENT_TYPE,
        autoExecute: t.AUTO_EXECUTE, userId: t.USER_ID,
      }));
    }
  } catch (e) { result.workflows.error = e.message; }

  const ENTITIES = ['DEAL', 'LEAD', 'CONTACT'];
  for (const ent of ENTITIES) {
    try {
      const r = await bitrixCall(webhook, 'crm.automation.trigger.list', { CODE: ent });
      if (!r.error) {
        const triggers = Array.isArray(r.result) ? r.result : (r.result?.triggers || []);
        result.triggers.byEntity[ent] = {
          count: triggers.length,
          sample: triggers.slice(0, 5).map(t => ({ code: t.CODE, name: t.NAME })),
        };
      } else result.triggers.byEntity[ent] = { error: r.error };
    } catch (e) {
      result.triggers.byEntity[ent] = { error: e.message };
    }
  }

  result.note = 'Бизнес-процессы и автоматизации НЕ переносятся автоматически в новый портал. ' +
    'Используй этот вывод как чек-лист: каждый процесс/триггер нужно вручную пересоздать в Pllato CRM. ' +
    'Шаблоны bizproc — это XML-конструкции которые работают только в Битрикс24.';

  return json({ ok: true, ...result });
}

// ═══════════════════════════════════════════════════════════════
//   POST /api/sync-delta
// ═══════════════════════════════════════════════════════════════
async function handleSyncDelta(request, env) {
  const body = await request.json();
  const idToken = body.idToken;
  if (!idToken) return json({ ok: false, error: 'idToken required' }, 400);

  const sa = JSON.parse(env.FIREBASE_SERVICE_ACCOUNT);
  const verified = await verifyFirebaseIdToken(idToken, sa);
  if (!verified) return json({ ok: false, error: 'Invalid token' }, 401);

  const webhook = env.BITRIX_WEBHOOK_URL;
  if (!webhook) return json({ ok: false, error: 'BITRIX_WEBHOOK_URL not set' }, 500);

  const accessToken = await getServiceAccountToken(sa, [
    'https://www.googleapis.com/auth/identitytoolkit',
    'https://www.googleapis.com/auth/firebase.database',
    'https://www.googleapis.com/auth/userinfo.email',
  ]);

  let since = body.since;
  if (!since) {
    const state = await firebaseDbGet(sa.project_id, accessToken, 'migrationState/lastDeltaSync');
    since = state?.timestamp || new Date(Date.now() - 24 * 3600 * 1000).toISOString();
  }
  const sinceBitrix = since.replace(/\.\d{3}Z?$/, '');

  const result = {
    since,
    deals: { updated: 0, errors: [] },
    tasks: { updated: 0, errors: [] },
    contacts: { updated: 0, errors: [] },
  };

  const updates = {};

  try {
    let start = 0, fetched = 0, safety = 50;
    while (safety-- > 0) {
      const r = await bitrixCall(webhook, 'crm.deal.list', {
        filter: { '>DATE_MODIFY': sinceBitrix, CATEGORY_ID: 3 },
        select: ['*', 'UF_*'],
        order: { DATE_MODIFY: 'DESC' }, start,
      });
      if (r.error) { result.deals.errors.push(r.error); break; }
      const items = Array.isArray(r.result) ? r.result : [];
      if (!items.length) break;
      for (const d of items) {
        const dealKey = `deal_${d.ID}`;
        updates[`deals/${dealKey}`] = mapBitrixDealToFirebase(d);
        fetched++;
      }
      if (r.next === undefined) break;
      start = r.next;
      if (fetched > 500) break;
    }
    result.deals.updated = fetched;
  } catch (e) { result.deals.errors.push(e.message); }

  try {
    let start = 0, fetched = 0, safety = 50;
    while (safety-- > 0) {
      const r = await bitrixCall(webhook, 'tasks.task.list', {
        filter: { '>CHANGED_DATE': sinceBitrix },
        select: ['ID', 'TITLE', 'DESCRIPTION', 'STATUS', 'PRIORITY', 'CREATED_BY', 'RESPONSIBLE_ID',
                 'DEADLINE', 'CREATED_DATE', 'CHANGED_DATE', 'CLOSED_DATE', 'GROUP_ID', 'PARENT_ID',
                 'COMMENTS_COUNT', 'UF_CRM_TASK'],
        order: { CHANGED_DATE: 'DESC' }, start,
      });
      if (r.error) { result.tasks.errors.push(r.error); break; }
      const tasks = r.result?.tasks || [];
      if (!tasks.length) break;
      for (const t of tasks) {
        const taskKey = `task_${t.id}`;
        updates[`tasks/${taskKey}`] = mapBitrixTaskToFirebase(t);
        fetched++;
      }
      if (r.next === undefined) break;
      start = r.next;
      if (fetched > 300) break;
    }
    result.tasks.updated = fetched;
  } catch (e) { result.tasks.errors.push(e.message); }

  try {
    let start = 0, fetched = 0, safety = 50;
    while (safety-- > 0) {
      const r = await bitrixCall(webhook, 'crm.contact.list', {
        filter: { '>DATE_MODIFY': sinceBitrix },
        select: ['*', 'UF_*', 'PHONE', 'EMAIL'],
        order: { DATE_MODIFY: 'DESC' }, start,
      });
      if (r.error) { result.contacts.errors.push(r.error); break; }
      const items = Array.isArray(r.result) ? r.result : [];
      if (!items.length) break;
      for (const c of items) {
        updates[`contacts/${c.ID}`] = mapBitrixContactToFirebase(c);
        fetched++;
      }
      if (r.next === undefined) break;
      start = r.next;
      if (fetched > 500) break;
    }
    result.contacts.updated = fetched;
  } catch (e) { result.contacts.errors.push(e.message); }

  if (Object.keys(updates).length > 0) {
    await firebaseDbMultiUpdate(sa.project_id, accessToken, '/', updates);
  }

  await firebaseDbSet(sa.project_id, accessToken, 'migrationState/lastDeltaSync', {
    timestamp: new Date().toISOString(), previous: since,
    summary: {
      deals: result.deals.updated,
      tasks: result.tasks.updated,
      contacts: result.contacts.updated,
    },
  });

  return json({ ok: true, ...result });
}

// ═══════════════════════════════════════════════════════════════
//   Helpers: маппинг Bitrix → Firebase для дельты
// ═══════════════════════════════════════════════════════════════
function mapBitrixDealToFirebase(d) {
  return {
    bitrixId: String(d.ID), title: d.TITLE || '',
    stageId: d.STAGE_ID || null, pipeline: d.CATEGORY_ID || null,
    opportunity: parseFloat(d.OPPORTUNITY) || 0,
    currency: d.CURRENCY_ID || 'KZT',
    probability: d.PROBABILITY ? parseInt(d.PROBABILITY) : null,
    closed: d.CLOSED === 'Y',
    closeDate: d.CLOSEDATE || null,
    sourceId: d.SOURCE_ID || null,
    sourceDescription: d.SOURCE_DESCRIPTION || null,
    contactId: d.CONTACT_ID || null,
    companyId: d.COMPANY_ID || null,
    responsibleUid: d.ASSIGNED_BY_ID ? `bitrix_${d.ASSIGNED_BY_ID}` : null,
    createdByUid: d.CREATED_BY_ID ? `bitrix_${d.CREATED_BY_ID}` : null,
    bitrixDateCreate: d.DATE_CREATE || null,
    bitrixDateModify: d.DATE_MODIFY || null,
    comments: d.COMMENTS || null,
    customFields: extractCustomFields(d),
  };
}

function mapBitrixTaskToFirebase(t) {
  return {
    bitrixId: String(t.id),
    title: t.title || '', description: t.description || '',
    status: String(t.status || '1'),
    priority: String(t.priority || '1'),
    responsibleUid: t.responsibleId ? `bitrix_${t.responsibleId}` : null,
    createdByUid: t.createdBy ? `bitrix_${t.createdBy}` : null,
    deadline: t.deadline || null,
    bitrixCreated: t.createdDate || null,
    bitrixDateClosed: t.closedDate || null,
    commentsCount: parseInt(t.commentsCount) || 0,
    groupId: t.groupId || null,
    parentId: t.parentId && parseInt(t.parentId) > 0 ? `task_${t.parentId}` : null,
    bitrixParentId: t.parentId || null,
  };
}

function mapBitrixContactToFirebase(c) {
  const phones = (c.PHONE || []).map(p => ({ value: p.VALUE, type: p.VALUE_TYPE }));
  const emails = (c.EMAIL || []).map(e => ({ value: e.VALUE, type: e.VALUE_TYPE }));
  return {
    bitrixId: String(c.ID),
    name: c.NAME || '', lastName: c.LAST_NAME || '', secondName: c.SECOND_NAME || '',
    phones, emails,
    typeId: c.TYPE_ID || null, sourceId: c.SOURCE_ID || null,
    responsibleUid: c.ASSIGNED_BY_ID ? `bitrix_${c.ASSIGNED_BY_ID}` : null,
    createdByUid: c.CREATED_BY_ID ? `bitrix_${c.CREATED_BY_ID}` : null,
    bitrixDateCreate: c.DATE_CREATE || null,
    bitrixDateModify: c.DATE_MODIFY || null,
    customFields: extractCustomFields(c),
  };
}

function extractCustomFields(d) {
  const cf = {};
  for (const [k, v] of Object.entries(d)) {
    if (k.startsWith('UF_CRM_')) cf[k] = v;
  }
  return cf;
}

async function loadAllBitrixUsers(webhook) {
  const all = [];
  let start = 0;
  let safety = 30;
  while (safety-- > 0) {
    const r = await bitrixCall(webhook, 'user.get', { ACTIVE: true, start });
    if (r.error) return { error: 'Bitrix: ' + r.error };
    if (!Array.isArray(r.result) || !r.result.length) break;
    all.push(...r.result);
    if (r.next === undefined) break;
    start = r.next;
  }
  return all;
}

// ═══════════════════════════════════════════════════════════════
//   Bitrix24 helpers
// ═══════════════════════════════════════════════════════════════
function extractLabel(label) {
  if (!label) return null;
  if (typeof label === 'string') return label;
  if (typeof label === 'object') {
    return label.ru || label.RU || label.en || label.EN || Object.values(label)[0] || null;
  }
  return String(label);
}

async function bitrixCall(webhookBase, method, params = {}) {
  let url = webhookBase;
  if (!url.endsWith('/')) url += '/';
  url += method + '.json';

  const body = new URLSearchParams();
  function flatten(obj, prefix = '') {
    for (const [k, v] of Object.entries(obj)) {
      const key = prefix ? `${prefix}[${k}]` : k;
      if (v === null || v === undefined) continue;
      if (Array.isArray(v)) {
        v.forEach((item, i) => {
          if (typeof item === 'object') flatten(item, `${key}[${i}]`);
          else body.append(`${key}[${i}]`, item);
        });
      } else if (typeof v === 'object') {
        flatten(v, key);
      } else {
        body.append(key, v);
      }
    }
  }
  flatten(params);

  const r = await fetch(url, { method: 'POST', body });
  if (!r.ok) return { error: `HTTP ${r.status}` };
  return r.json().catch(e => ({ error: 'parse_error: ' + e.message }));
}

async function bitrixBatch(webhookBase, commands) {
  const cmd = {};
  for (const [name, op] of Object.entries(commands)) {
    const queryParts = [];
    function build(obj, prefix = '') {
      for (const [k, v] of Object.entries(obj)) {
        const key = prefix ? `${prefix}[${k}]` : k;
        if (v === null || v === undefined) continue;
        if (Array.isArray(v)) {
          v.forEach((item, i) => {
            if (typeof item === 'object') build(item, `${key}[${i}]`);
            else queryParts.push(`${encodeURIComponent(`${key}[${i}]`)}=${encodeURIComponent(item)}`);
          });
        } else if (typeof v === 'object') {
          build(v, key);
        } else {
          queryParts.push(`${encodeURIComponent(key)}=${encodeURIComponent(v)}`);
        }
      }
    }
    build(op.params || {});
    cmd[name] = `${op.method}?${queryParts.join('&')}`;
  }
  return bitrixCall(webhookBase, 'batch', { halt: 0, cmd });
}

// ═══════════════════════════════════════════════════════════════
//   Firebase Admin: helpers using service account
// ═══════════════════════════════════════════════════════════════
const _tokenCache = new Map();

async function getServiceAccountToken(sa, scopes) {
  const now = Math.floor(Date.now() / 1000);
  const key = scopes.slice().sort().join(' ');
  const cached = _tokenCache.get(key);
  if (cached && cached.exp - now > 60) return cached.token;

  const header = base64UrlEncode(JSON.stringify({ alg: 'RS256', typ: 'JWT' }));
  const payload = base64UrlEncode(JSON.stringify({
    iss: sa.client_email, scope: scopes.join(' '),
    aud: 'https://oauth2.googleapis.com/token',
    exp: now + 3600, iat: now,
  }));
  const data = `${header}.${payload}`;
  const signature = await rsaSign(data, sa.private_key);
  const jwt = `${data}.${signature}`;

  const r = await fetch('https://oauth2.googleapis.com/token', {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({
      grant_type: 'urn:ietf:params:oauth:grant-type:jwt-bearer',
      assertion: jwt,
    }),
  });
  const result = await r.json();
  if (!result.access_token) throw new Error('No access_token: ' + JSON.stringify(result));

  _tokenCache.set(key, { token: result.access_token, exp: now + (result.expires_in || 3600) });
  return result.access_token;
}

async function rsaSign(data, pem) {
  const key = await importPrivateKey(pem);
  const sig = await crypto.subtle.sign(
    { name: 'RSASSA-PKCS1-v1_5' }, key,
    new TextEncoder().encode(data)
  );
  return base64UrlEncodeBuffer(new Uint8Array(sig));
}

async function importPrivateKey(pem) {
  const cleaned = pem
    .replace(/-----BEGIN PRIVATE KEY-----/, '')
    .replace(/-----END PRIVATE KEY-----/, '')
    .replace(/\\n/g, '\n')
    .replace(/\s/g, '');
  const der = Uint8Array.from(atob(cleaned), c => c.charCodeAt(0));
  return crypto.subtle.importKey(
    'pkcs8', der,
    { name: 'RSASSA-PKCS1-v1_5', hash: 'SHA-256' },
    false, ['sign']
  );
}

function base64UrlEncode(s) {
  return btoa(s).replace(/=+$/, '').replace(/\+/g, '-').replace(/\//g, '_');
}
function base64UrlEncodeBuffer(buf) {
  let s = '';
  for (const b of buf) s += String.fromCharCode(b);
  return base64UrlEncode(s);
}

let _googleCertsCache = { certs: null, exp: 0 };
async function getGoogleCerts() {
  const now = Math.floor(Date.now() / 1000);
  if (_googleCertsCache.certs && _googleCertsCache.exp > now) return _googleCertsCache.certs;
  const r = await fetch('https://www.googleapis.com/robot/v1/metadata/x509/securetoken@system.gserviceaccount.com');
  const certs = await r.json();
  _googleCertsCache = { certs, exp: now + 3600 };
  return certs;
}

async function verifyFirebaseIdToken(idToken, sa) {
  try {
    const [headerB64, payloadB64, sigB64] = idToken.split('.');
    if (!headerB64 || !payloadB64 || !sigB64) return null;

    const header = JSON.parse(atob(headerB64.replace(/-/g, '+').replace(/_/g, '/')));
    const payload = JSON.parse(atob(payloadB64.replace(/-/g, '+').replace(/_/g, '/')));

    const now = Math.floor(Date.now() / 1000);
    if (payload.exp < now) return null;
    if (payload.iat > now + 60) return null;
    if (payload.aud !== sa.project_id) return null;
    if (payload.iss !== `https://securetoken.google.com/${sa.project_id}`) return null;
    if (!payload.sub) return null;

    const certs = await getGoogleCerts();
    const cert = certs[header.kid];
    if (!cert) return null;

    const pubKey = await importX509(cert);
    const data = new TextEncoder().encode(`${headerB64}.${payloadB64}`);
    const sig = Uint8Array.from(
      atob(sigB64.replace(/-/g, '+').replace(/_/g, '/').padEnd(Math.ceil(sigB64.length / 4) * 4, '=')),
      c => c.charCodeAt(0)
    );
    const valid = await crypto.subtle.verify(
      { name: 'RSASSA-PKCS1-v1_5' }, pubKey, sig, data
    );
    if (!valid) return null;
    return payload;
  } catch (e) {
    console.error('verify error:', e.message);
    return null;
  }
}

async function importX509(pem) {
  const cleaned = pem
    .replace(/-----BEGIN CERTIFICATE-----/, '')
    .replace(/-----END CERTIFICATE-----/, '')
    .replace(/\s/g, '');
  const der = Uint8Array.from(atob(cleaned), c => c.charCodeAt(0));
  return crypto.subtle.importKey(
    'spki', extractSpkiFromX509(der),
    { name: 'RSASSA-PKCS1-v1_5', hash: 'SHA-256' },
    false, ['verify']
  );
}

function extractSpkiFromX509(der) {
  const oid = [0x06, 0x09, 0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 0x01, 0x01, 0x01];
  for (let i = 0; i < der.length - oid.length; i++) {
    let match = true;
    for (let j = 0; j < oid.length; j++) {
      if (der[i + j] !== oid[j]) { match = false; break; }
    }
    if (match) {
      let start = i - 4;
      while (start > 0 && der[start] !== 0x30) start--;
      const lenByte = der[start + 1];
      let totalLen, lenBytes;
      if (lenByte < 0x80) { totalLen = lenByte + 2; lenBytes = 1; }
      else {
        const numLenBytes = lenByte & 0x7f;
        totalLen = 0;
        for (let k = 0; k < numLenBytes; k++) totalLen = (totalLen << 8) | der[start + 2 + k];
        totalLen += 2 + numLenBytes;
        lenBytes = 1 + numLenBytes;
      }
      return der.slice(start, start + totalLen);
    }
  }
  throw new Error('SPKI not found in X509');
}

async function firebaseGetUserByEmail(projectId, accessToken, email) {
  const r = await fetch(
    `https://identitytoolkit.googleapis.com/v1/projects/${projectId}/accounts:lookup`,
    {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${accessToken}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ email: [email] }),
    }
  );
  const d = await r.json();
  return d.users && d.users[0] ? d.users[0] : null;
}

async function firebaseCreateUser(projectId, accessToken, { email, password, displayName, disabled = false }) {
  const r = await fetch(
    `https://identitytoolkit.googleapis.com/v1/projects/${projectId}/accounts`,
    {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${accessToken}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ email, password, displayName, disabled, emailVerified: false }),
    }
  );
  const d = await r.json();
  if (d.error) throw new Error(d.error.message || 'create user failed');
  return d;
}

async function firebaseDbSet(projectId, accessToken, path, data, merge = false) {
  const url = `https://${projectId}-default-rtdb.firebaseio.com/${path}.json`;
  const r = await fetch(url, {
    method: merge ? 'PATCH' : 'PUT',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${accessToken}`,
    },
    body: JSON.stringify(data),
  });
  if (!r.ok) {
    const text = await r.text();
    throw new Error(`db set failed: ${r.status} ${text}`);
  }
  return r.json();
}

async function firebaseDbGet(projectId, accessToken, path) {
  const url = `https://${projectId}-default-rtdb.firebaseio.com/${path}.json`;
  const r = await fetch(url, {
    method: 'GET',
    headers: { 'Authorization': `Bearer ${accessToken}` },
  });
  if (!r.ok) return null;
  return r.json();
}

async function firebaseDbMultiUpdate(projectId, accessToken, basePath, updates) {
  const path = basePath === '/' ? '' : basePath;
  const url = `https://${projectId}-default-rtdb.firebaseio.com/${path}.json`;
  const r = await fetch(url, {
    method: 'PATCH',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${accessToken}`,
    },
    body: JSON.stringify(updates),
  });
  if (!r.ok) {
    const text = await r.text();
    throw new Error(`db multi-update failed: ${r.status} ${text}`);
  }
  return r.json();
}

function generateTempPassword() {
  const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnpqrstuvwxyz23456789';
  let s = '';
  for (let i = 0; i < 16; i++) s += chars[Math.floor(Math.random() * chars.length)];
  return s + '!9';
}

function json(data, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: { 'Content-Type': 'application/json', ...CORS },
  });
}
