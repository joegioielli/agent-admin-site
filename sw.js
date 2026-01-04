self.addEventListener('fetch', e => {
  if (e.request.url.includes('/chat.html')) {
    e.respondWith(caches.match(e.request).then(r => r || fetch(e.request)));
  }
});
