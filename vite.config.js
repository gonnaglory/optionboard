import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// https://vitejs.dev/config/
export default defineConfig({
  plugins: [react()],
  build: {
    sourcemap: false, // отключаем карты в проде
    target: 'es2019',
    chunkSizeWarningLimit: 800
  }
})


module.exports = {
  theme: {
    extend: {
      colors: {
        "neon-blue": "#3b82f6",   // Tailwind blue-500
        "neon-pink": "#ec4899",   // Tailwind pink-500
        "neon-green": "#22c55e",  // Tailwind green-500
      },
    },
  },
  plugins: [
    require('tailwind-scrollbar')({ nocompatible: true }),
  ],
};
