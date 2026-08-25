export function formatIntradayTimeKey(datetime: string): string {
  const value = String(datetime ?? '')
  const hasExplicitTimezone = /(?:Z|[+-]\d{2}:?\d{2})$/i.test(value)

  if (hasExplicitTimezone) {
    const parsed = new Date(value)
    if (!Number.isNaN(parsed.getTime())) {
      return CN_TIME_FORMATTER.format(parsed)
    }
  }

  const localMatch = value.match(/(?:^|[T\s])(\d{2}):(\d{2})/)
  if (localMatch) return `${localMatch[1]}:${localMatch[2]}`
  return value.slice(11, 16)
}

const CN_TIME_FORMATTER = new Intl.DateTimeFormat('en-GB', {
  timeZone: 'Asia/Shanghai',
  hour: '2-digit',
  minute: '2-digit',
  hour12: false,
})
