# Pllato CRM

Внутренний портал управления бизнесом с миграцией из Битрикс24.

## Архитектура

- **Frontend:** `index.html` — vanilla JS, GitHub Pages
- **Backend:** Cloudflare Worker (`worker.js`)
- **База:** Firebase Realtime Database (`pllato-crm`)
- **Хранилище:** Cloudflare R2 (`pllato-crm-files`)

## Деплой

### 1. Frontend (GitHub Pages)

```bash
git add index.html README.md
git commit -m "Initial commit"
git push origin main
```

Включи GitHub Pages: **Settings → Pages → Source: main / root → Save**

URL: `https://pllato.github.io/pllato-crm/`

### 2. Worker (Cloudflare)

Скопируй содержимое `worker.js` в Cloudflare Worker `pllato-crm-worker` через:

**Cloudflare Dashboard → Workers & Pages → pllato-crm-worker → Edit Code → вставить → Save and Deploy**

### 3. Authorized Domains в Firebase

В Firebase Console → Authentication → Settings → Authorized domains добавить:

- `pllato.github.io`
- (позже) свой кастомный домен

## Использование

1. Открой `https://pllato.github.io/pllato-crm/`
2. Войди через Google (твой `uurraa@gmail.com` уже разрешён как admin)
3. Иди в раздел **Миграция Bitrix**
4. Запусти **Тест подключения** → должен показать данные из Битрикса
5. Запусти **Предварительный просмотр пользователей** → увидишь 88 пользователей
6. Запусти **Реальная миграция** → создаст всех в Firebase Auth

## Ключевые сущности в БД

```
/users/{firebaseUid}
  bitrixId, email, name, lastName, position, active, ...

/userMapping/bitrix/{bitrixId}
  firebaseUid, email
  
/userMapping/firebase/{firebaseUid}
  bitrixId, email
```
