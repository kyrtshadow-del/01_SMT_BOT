import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// Vite dev server config with proxy to FastAPI backend
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      // Frontend calls `/api/...`, proxy to backend `/web/api/...`
      "/api": {
        target: "http://127.0.0.1:8000",
        changeOrigin: true,
        rewrite: (path) => path.replace(/^\/api/, "/web/api")
      }
    }
  }
});

