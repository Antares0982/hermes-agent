// Service Worker for Hermes PWA
// Caches static assets so the app opens even when offline.
// The chat itself requires WebSocket connectivity — offline mode
// shows cached UI with a "reconnecting" indicator.

const CACHE_NAME = 'hermes-pwa-v1';
const STATIC_ASSETS = [
  '/',
  '/index.html',
  '/manifest.json',
];

self.addEventListener('install', (event) => {
  event.waitUntil(
    caches.open(CACHE_NAME).then((cache) => {
      return cache.addAll(STATIC_ASSETS);
    })
  );
  self.skipWaiting();
});

self.addEventListener('activate', (event) => {
  event.waitUntil(
    caches.keys().then((names) => {
      return Promise.all(
        names
          .filter((name) => name !== CACHE_NAME)
          .map((name) => caches.delete(name))
      );
    })
  );
  self.clients.claim();
});

self.addEventListener('fetch', (event) => {
  // Only handle GET requests to our own origin.
  if (event.request.method !== 'GET') return;

  event.respondWith(
    caches.match(event.request).then((cached) => {
      // Return cached response; if not in cache, try network.
      return cached || fetch(event.request).then((response) => {
        // Cache successful responses for future offline use.
        if (response.ok && response.type === 'basic') {
          const clone = response.clone();
          caches.open(CACHE_NAME).then((cache) => {
            cache.put(event.request, clone);
          });
        }
        return response;
      }).catch(() => {
        // Offline and not cached — the page itself is cached
        // so this only affects dynamic sub-resources.
        return new Response('', { status: 503 });
      });
    })
  );
});
