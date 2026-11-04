import type { NextConfig } from 'next'

/**
 * Next.js 核心配置。
 *
 * 关键配置：
 * - rewrites：将 /api/** 和 /ws 请求代理到后端 http://localhost:8088，
 *   实现前后端分离开发时的无 CORS 调用。
 * - proxyTimeout：Kafka 一键启动（controller + 3 broker）可能超过默认 30s，放宽到 120s。
 * - 后端为 Spring Boot 4.1.0，端口 8088；前端 dev server 端口 3000。
 */
const nextConfig: NextConfig = {
  allowedDevOrigins: ['127.0.0.1'],
  experimental: {
    proxyTimeout: 120_000,
  },
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
