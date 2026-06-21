import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// During `vite dev` the frontend runs on :5173 and proxies the WebSocket to the
// Go backend on :8080. `vite build` emits static assets into dist/, which the Go
// binary embeds and serves itself.
export default defineConfig({
  plugins: [react()],
  base: './',
  build: { outDir: 'dist', emptyOutDir: true },
  server: {
    port: 5173,
    proxy: {
      '/ws': { target: 'ws://localhost:8080', ws: true },
    },
  },
})
