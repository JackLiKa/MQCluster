'use client'

/**
 * 带禁用原因的按钮：disabledReason 非空时按钮禁用，悬停 / 聚焦显示原因。
 * 禁用的 <button> 不触发指针事件，所以用可聚焦的 span 作为 tooltip 触发器。
 */

import { Button } from '@/components/ui/button'
import { Tooltip, TooltipContent, TooltipTrigger } from '@/components/ui/tooltip'

type ButtonProps = React.ComponentProps<typeof Button>

export function HintButton({
  disabledReason,
  disabled,
  ...props
}: ButtonProps & { disabledReason?: string | null }) {
  if (!disabledReason) return <Button disabled={disabled} {...props} />
  return (
    <Tooltip>
      <TooltipTrigger render={<span tabIndex={0} className="inline-flex cursor-not-allowed" />}>
        <Button disabled {...props} />
      </TooltipTrigger>
      <TooltipContent className="max-w-xs">{disabledReason}</TooltipContent>
    </Tooltip>
  )
}
