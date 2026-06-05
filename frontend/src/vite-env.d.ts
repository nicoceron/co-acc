/// <reference types="vite/client" />

interface ImportMetaEnv {
  readonly VITE_API_URL: string;
  readonly VITE_ALLOW_FIXTURES?: string;
  readonly VITE_PUBLIC_MODE?: string;
  readonly VITE_PATTERNS_ENABLED?: string;
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}
