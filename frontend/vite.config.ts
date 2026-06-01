import react from "@vitejs/plugin-react";
import { resolve } from "path";
import { defineConfig } from "vite";

const apiProxyTarget =
  process.env.VITE_API_URL || `http://localhost:${process.env.API_PORT || "8000"}`;

export default defineConfig({
  plugins: [react()],
  resolve: {
    alias: {
      "@": resolve(__dirname, "src"),
    },
  },
  server: {
    port: 3000,
    proxy: {
      "/api": {
        target: apiProxyTarget,
        changeOrigin: true,
      },
    },
  },
});
