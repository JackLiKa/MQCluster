import type { NextConfig } from 'next'

/**
 * Next.js 核心配置。
 *
 * 關鍵配置：
 * - rewrites：將 /api/** 和 /ws 請求代理到後端 http://localhost:8088，
 *   實現前後端分離開發時的無 CORS 調用。
 * - 後端為 Spring Boot 4.1.0，端口 8088。
 * - 前端 dev server 端口 3000（Next.js 默認）。
 */
const nextConfig: NextConfig = {
  allowedDevOrigins: ['127.0.0.1'],
  async rewrites() {
    return [
      {
        source: '/api/:path*',
        destination: `${process.env.BACKEND_URL ?? 'http://localhost:8088'}/api/:path*`,
      },
      {
        source: '/ws/:path*',
        destination: `${process.env.BACKEND_URL ?? 'http://localhost:8088'}/ws/:path*`,
      },
    ]
  },
}

export default nextConfig
