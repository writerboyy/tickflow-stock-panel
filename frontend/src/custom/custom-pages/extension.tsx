import { lazy } from 'react'
import { Code2, Crosshair, ShieldCheck, WalletCards } from 'lucide-react'
import type { FrontendExtension } from '@/extensions/types'

const FreeStrategy = lazy(() => import('@/pages/FreeStrategy').then(module => ({ default: module.FreeStrategyPage })))
const LargeOrders = lazy(() => import('@/pages/LargeOrders').then(module => ({ default: module.LargeOrders })))
const LimitBoard = lazy(() => import('@/pages/LimitBoard').then(module => ({ default: module.LimitBoard })))
const MarketHeat = lazy(() => import('@/pages/MarketHeat').then(module => ({ default: module.MarketHeat })))
const PaperTrading = lazy(() => import('@/pages/PaperTrading').then(module => ({ default: module.PaperTrading })))

const extension: FrontendExtension = {
  id: 'custom.pages',
  apiVersion: 1,
  routes: [
    { id: 'custom-free-strategy', path: '/free-strategy', component: FreeStrategy },
    { id: 'custom-large-orders', path: '/large-orders', component: LargeOrders },
    { id: 'custom-limit-board', path: '/limit-board', component: LimitBoard },
    { id: 'custom-market-heat', path: '/market-heat', component: MarketHeat },
    { id: 'custom-paper-trading', path: '/paper-trading', component: PaperTrading },
  ],
  navigation: [
    { id: 'custom-free-strategy', routeId: 'custom-free-strategy', label: '量化策略', icon: Code2, order: 300 },
    { id: 'custom-large-orders', routeId: 'custom-large-orders', label: '持仓风控', icon: ShieldCheck, order: 310 },
    { id: 'custom-limit-board', routeId: 'custom-limit-board', label: '短线猎手', icon: Crosshair, order: 320 },
    { id: 'custom-paper-trading', routeId: 'custom-paper-trading', label: '模拟', icon: WalletCards, order: 330 },
  ],
}

export default extension
