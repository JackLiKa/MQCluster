'use client'

/**
 * 主题 Provider——基于 next-themes，在 <html> 上切换 .dark 类。
 * 默认跟随系统，切换时禁用过渡动画避免闪烁。
 */

import { ThemeProvider as NextThemesProvider } from 'next-themes'

export function ThemeProvider({ children }: { children: React.ReactNode }) {
  return (
    <NextThemesProvider attribute="class" defaultTheme="system" enableSystem disableTransitionOnChange>
      {children}
    </NextThemesProvider>
  )
}
